//! The filter DSL: a tiny JSON S-expression parsed here into an
//! `iceberg::expr::Predicate`.
//!
//! A filter is a JSON array whose first element is the operator:
//!
//! ```text
//! ["=",  "col", <literal>]     ["!=", "col", <literal>]
//! ["<",  "col", <literal>]     ["<=", "col", <literal>]
//! [">",  "col", <literal>]     [">=", "col", <literal>]
//! ["is-null",   "col"]         ["not-null", "col"]
//! ["is-nan",    "col"]         ["not-nan",  "col"]
//! ["starts-with", "col", "prefix"]
//! ["in",     "col", [<literal>, …]]
//! ["not-in", "col", [<literal>, …]]
//! ["and", <filter>, <filter>, …]   ["or", <filter>, <filter>, …]
//! ["not", <filter>]
//! ["true"]  ["false"]
//! ```
//!
//! Literals are plain JSON scalars (`3`, `1.5`, `"abc"`, `true`) and are coerced
//! to the column's Iceberg type using the table schema, so `["=", "id", 3]` on a
//! `long` column produces `Datum::long(3)` and not `Datum::int(3)` — an important
//! detail, because a mistyped datum makes the predicate silently non-matching.
//! Dates/times/timestamps accept either an integer (days / micros since epoch) or
//! an ISO-8601 string.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema, Type};
use serde_json::Value;

type R<T> = Result<T, String>;

/// Parse `json` into a `Predicate`, typing literals against `schema`.
pub(crate) fn parse_filter(json: &str, schema: &Schema) -> R<Predicate> {
    let v: Value = serde_json::from_str(json).map_err(|e| format!("filter is not JSON: {e}"))?;
    build(&v, schema)
}

fn build(v: &Value, schema: &Schema) -> R<Predicate> {
    let arr = v
        .as_array()
        .ok_or_else(|| format!("filter node must be a JSON array, got {v}"))?;
    let op = arr
        .first()
        .and_then(|o| o.as_str())
        .ok_or_else(|| format!("filter node needs a string operator, got {v}"))?
        .to_ascii_lowercase();

    let need = |n: usize| -> R<()> {
        if arr.len() == n {
            Ok(())
        } else {
            Err(format!(
                "operator '{op}' takes {} argument(s), got {}",
                n - 1,
                arr.len() - 1
            ))
        }
    };
    let col = |i: usize| -> R<&str> {
        arr.get(i)
            .and_then(|c| c.as_str())
            .ok_or_else(|| format!("operator '{op}': argument {i} must be a column name"))
    };

    match op.as_str() {
        "true" => Ok(Predicate::AlwaysTrue),
        "false" => Ok(Predicate::AlwaysFalse),

        "and" | "or" => {
            if arr.len() < 3 {
                return Err(format!("operator '{op}' needs at least two operands"));
            }
            let mut it = arr[1..].iter();
            let mut acc = build(it.next().unwrap(), schema)?;
            for next in it {
                let p = build(next, schema)?;
                acc = if op == "and" { acc.and(p) } else { acc.or(p) };
            }
            Ok(acc)
        }
        "not" => {
            need(2)?;
            Ok(build(&arr[1], schema)?.negate())
        }

        "is-null" | "isnull" => {
            need(2)?;
            Ok(Reference::new(col(1)?).is_null())
        }
        "not-null" | "is-not-null" | "notnull" => {
            need(2)?;
            Ok(Reference::new(col(1)?).is_not_null())
        }
        "is-nan" | "isnan" => {
            need(2)?;
            Ok(Reference::new(col(1)?).is_nan())
        }
        "not-nan" | "is-not-nan" => {
            need(2)?;
            Ok(Reference::new(col(1)?).is_not_nan())
        }

        "in" | "not-in" | "notin" => {
            need(3)?;
            let name = col(1)?;
            let ty = column_type(schema, name)?;
            let items = arr[2]
                .as_array()
                .ok_or_else(|| format!("operator '{op}': third argument must be a JSON array"))?;
            let datums = items
                .iter()
                .map(|i| datum(i, &ty, name))
                .collect::<R<Vec<_>>>()?;
            let r = Reference::new(name);
            Ok(if op == "in" {
                r.is_in(datums)
            } else {
                r.is_not_in(datums)
            })
        }

        "=" | "==" | "eq" | "!=" | "<>" | "ne" | "<" | "lt" | "<=" | "le" | ">" | "gt" | ">="
        | "ge" | "starts-with" | "startswith" | "not-starts-with" => {
            need(3)?;
            let name = col(1)?;
            let ty = column_type(schema, name)?;
            let d = datum(&arr[2], &ty, name)?;
            let r = Reference::new(name);
            Ok(match op.as_str() {
                "=" | "==" | "eq" => r.equal_to(d),
                "!=" | "<>" | "ne" => r.not_equal_to(d),
                "<" | "lt" => r.less_than(d),
                "<=" | "le" => r.less_than_or_equal_to(d),
                ">" | "gt" => r.greater_than(d),
                ">=" | "ge" => r.greater_than_or_equal_to(d),
                "starts-with" | "startswith" => r.starts_with(d),
                _ => r.not_starts_with(d),
            })
        }

        other => Err(format!("unknown filter operator '{other}'")),
    }
}

fn column_type(schema: &Schema, name: &str) -> R<PrimitiveType> {
    let field = schema
        .field_by_name(name)
        .ok_or_else(|| format!("filter: column '{name}' is not in the table schema"))?;
    match field.field_type.as_ref() {
        Type::Primitive(p) => Ok(p.clone()),
        other => Err(format!(
            "filter: column '{name}' has non-primitive type {other}; only primitive columns are filterable"
        )),
    }
}

fn datum(v: &Value, ty: &PrimitiveType, col: &str) -> R<Datum> {
    let bad = |want: &str| format!("filter: column '{col}' is {ty}, expected a {want} literal, got {v}");
    match ty {
        PrimitiveType::Boolean => v.as_bool().map(Datum::bool).ok_or_else(|| bad("boolean")),
        PrimitiveType::Int => v
            .as_i64()
            .and_then(|i| i32::try_from(i).ok())
            .map(Datum::int)
            .ok_or_else(|| bad("32-bit integer")),
        PrimitiveType::Long => v.as_i64().map(Datum::long).ok_or_else(|| bad("integer")),
        PrimitiveType::Float => v
            .as_f64()
            .map(|f| Datum::float(f as f32))
            .ok_or_else(|| bad("number")),
        PrimitiveType::Double => v.as_f64().map(Datum::double).ok_or_else(|| bad("number")),
        PrimitiveType::String => v
            .as_str()
            .map(Datum::string)
            .ok_or_else(|| bad("string")),
        PrimitiveType::Date => match v {
            Value::Number(_) => v
                .as_i64()
                .and_then(|i| i32::try_from(i).ok())
                .map(Datum::date)
                .ok_or_else(|| bad("day count")),
            Value::String(s) => Datum::date_from_str(s).map_err(|e| format!("filter: {col}: {e}")),
            _ => Err(bad("date")),
        },
        PrimitiveType::Time => match v {
            Value::Number(_) => Datum::time_micros(v.as_i64().unwrap())
                .map_err(|e| format!("filter: {col}: {e}")),
            Value::String(s) => Datum::time_from_str(s).map_err(|e| format!("filter: {col}: {e}")),
            _ => Err(bad("time")),
        },
        PrimitiveType::Timestamp => match v {
            Value::Number(_) => Ok(Datum::timestamp_micros(v.as_i64().unwrap())),
            Value::String(s) => {
                Datum::timestamp_from_str(s).map_err(|e| format!("filter: {col}: {e}"))
            }
            _ => Err(bad("timestamp")),
        },
        PrimitiveType::Timestamptz => match v {
            Value::Number(_) => Ok(Datum::timestamptz_micros(v.as_i64().unwrap())),
            Value::String(s) => {
                Datum::timestamptz_from_str(s).map_err(|e| format!("filter: {col}: {e}"))
            }
            _ => Err(bad("timestamptz")),
        },
        PrimitiveType::TimestampNs => v
            .as_i64()
            .map(Datum::timestamp_nanos)
            .ok_or_else(|| bad("nanosecond timestamp")),
        PrimitiveType::TimestamptzNs => v
            .as_i64()
            .map(Datum::timestamptz_nanos)
            .ok_or_else(|| bad("nanosecond timestamptz")),
        PrimitiveType::Uuid => v
            .as_str()
            .ok_or_else(|| bad("uuid string"))
            .and_then(|s| Datum::uuid_from_str(s).map_err(|e| format!("filter: {col}: {e}"))),
        PrimitiveType::Decimal { .. } => match v {
            Value::String(s) => {
                Datum::decimal_from_str(s).map_err(|e| format!("filter: {col}: {e}"))
            }
            Value::Number(_) => Datum::decimal_from_str(v.to_string())
                .map_err(|e| format!("filter: {col}: {e}")),
            _ => Err(bad("decimal")),
        },
        PrimitiveType::Binary | PrimitiveType::Fixed(_) => Err(format!(
            "filter: column '{col}' is {ty}; binary/fixed columns are not supported by the filter DSL"
        )),
    }
}
