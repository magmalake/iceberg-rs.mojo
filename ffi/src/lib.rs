//! C-ABI shim over the `iceberg-rust` crates for the **iceberg-rs.mojo** binding.
//!
//! `iceberg-rust` is async (tokio) and speaks Arrow. This crate exposes a *narrow,
//! synchronous* C ABI on top of it: every entry point blocks on one shared
//! multi-thread tokio runtime, handles are opaque `*mut c_void`, strings are
//! UTF-8 `char*` (NUL-terminated in, heap-allocated out + `ib_string_free`), and
//! errors are signalled out-of-band through `ib_last_error()`.
//!
//! Record batches cross the boundary two ways:
//!
//! * as an **opaque `Batch` handle** (`ib_scan_next`, `ib_batch_get_*`,
//!   `ib_batch_builder_*`) — the simple path the Mojo binding uses by default;
//! * as an **Arrow C Data Interface** pair (`ib_scan_next_batch`,
//!   `ib_batch_export`, `ib_batch_import`, `ib_table_append_batch`) — the
//!   zero-copy path for consumers that already speak Arrow (e.g. marrow).
//!
//! Layout of the modules mirrors the surface: catalogs, tables + metadata, the
//! filter DSL, scans, batches, and the append/commit writer.

use std::ffi::{CStr, CString, c_char};
use std::sync::OnceLock;

use tokio::runtime::Runtime as TokioRuntime;

// The modules are `pub` (and re-exported) purely so `cargo test` can call the
// same entry points through the rlib that Mojo reaches through `dlopen`.
pub mod batch;
pub mod catalog;
pub mod filter;
pub mod scan;
pub mod table;
pub mod write;

pub use batch::*;
pub use catalog::*;
pub use scan::*;
pub use table::*;
pub use write::*;

// ── shared tokio runtime ─────────────────────────────────────────────────────

/// The one runtime every `extern "C"` entry point blocks on. iceberg-rust's own
/// `Runtime::try_current()` (used by the catalog builders) needs a tokio context,
/// which `block_on` provides.
pub(crate) fn rt() -> &'static TokioRuntime {
    static RT: OnceLock<TokioRuntime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

// ── thread-local last error ──────────────────────────────────────────────────

thread_local! {
    static LAST_ERR: std::cell::RefCell<CString> =
        std::cell::RefCell::new(CString::new("").unwrap());
}

pub(crate) fn set_err(msg: impl Into<String>) {
    let msg: String = msg.into();
    LAST_ERR.with(|e| {
        *e.borrow_mut() =
            CString::new(msg).unwrap_or_else(|_| CString::new("error (embedded NUL)").unwrap());
    });
}

/// Pointer to a NUL-terminated message describing the most recent failure on this
/// thread. Always valid (empty when nothing failed); valid until the next failing
/// call on the same thread. Do **not** free it.
#[no_mangle]
pub extern "C" fn ib_last_error() -> *const c_char {
    LAST_ERR.with(|e| e.borrow().as_ptr())
}

/// Version string of the `iceberg` crate this shim was built against.
/// Static storage; do **not** free.
#[no_mangle]
pub extern "C" fn ib_version() -> *const c_char {
    // The iceberg crate has no runtime version accessor, so the value is baked in
    // at build time from our own dependency pin.
    concat!("0.10.1", "\0").as_ptr() as *const c_char
}

// ── string plumbing ──────────────────────────────────────────────────────────

/// Free a string returned by any `ib_*` function that hands out `char*`.
///
/// # Safety
/// `s` must be a pointer previously returned by this library, and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn ib_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Borrow a `*const c_char` as `&str`, reporting a typed error on failure.
pub(crate) fn cstr<'a>(p: *const c_char, what: &str) -> Option<&'a str> {
    if p.is_null() {
        set_err(format!("{what}: null pointer"));
        return None;
    }
    match unsafe { CStr::from_ptr(p) }.to_str() {
        Ok(s) => Some(s),
        Err(_) => {
            set_err(format!("{what}: not valid UTF-8"));
            None
        }
    }
}

/// Same as [`cstr`] but a NULL pointer maps to `None` without an error.
pub(crate) fn cstr_opt<'a>(p: *const c_char, what: &str) -> Result<Option<&'a str>, ()> {
    if p.is_null() {
        return Ok(None);
    }
    match unsafe { CStr::from_ptr(p) }.to_str() {
        Ok(s) => Ok(Some(s)),
        Err(_) => {
            set_err(format!("{what}: not valid UTF-8"));
            Err(())
        }
    }
}

/// Hand a Rust `String` to the caller as a heap `char*` (freed by `ib_string_free`).
pub(crate) fn out_string(s: String) -> *mut c_char {
    match CString::new(s) {
        Ok(c) => c.into_raw(),
        Err(_) => {
            set_err("result string contained an embedded NUL");
            std::ptr::null_mut()
        }
    }
}

/// Byte length (excluding the NUL) of a C string this library returned.
///
/// Together with [`ib_string_copy`] this lets a caller with no pointer
/// arithmetic — the Mojo binding treats handles as opaque `Int`s — read a result
/// string without ever dereferencing it itself.
///
/// # Safety
/// `s` must be a NUL-terminated string, typically one this library returned.
#[no_mangle]
pub unsafe extern "C" fn ib_string_len(s: *const c_char) -> i64 {
    if s.is_null() {
        return -1;
    }
    unsafe { CStr::from_ptr(s) }.to_bytes().len() as i64
}

/// Copy up to `cap` bytes of the C string `s` into `out` (no NUL is written).
/// Returns the number of bytes copied, or -1 on error.
///
/// # Safety
/// `s` must be NUL-terminated; `out` must have room for `cap` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_string_copy(s: *const c_char, out: *mut u8, cap: usize) -> i64 {
    if s.is_null() || (out.is_null() && cap > 0) {
        return -1;
    }
    let bytes = unsafe { CStr::from_ptr(s) }.to_bytes();
    let n = bytes.len().min(cap);
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out, n) };
    n as i64
}
