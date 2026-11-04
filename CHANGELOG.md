# Changelog

## Unreleased

## 0.1.3 - 2026-11-03

- The shim is found without `CONDA_PREFIX`: after `$CONDA_PREFIX/lib`,
  iceberg-rs looks next to the executable, in its `../lib`, and on the
  binary's rpath (the environment `mojo build` ran in), so a binary built
  with `pixi run mojo build` runs from a plain shell. The search lives in
  `src/iceberg_rs/_dlopen.mojo`, shared with the other magmalake tins that
  load a library. A shim that can't be found is an Error naming every place
  searched and saying to set `CONDA_PREFIX`.
- The fallback to `ffi/target/release/libicebergrsmojo.*` relative to the
  working directory is gone: loading a library from the working directory
  isn't safe, and the pixi environment always has the shim.
