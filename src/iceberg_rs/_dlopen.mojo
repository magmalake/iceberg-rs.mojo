"""Find and `dlopen` a shared library a tin ships, inside or outside pixi.

The same module lives in every magmalake tin that loads a library at run
time (zstd, brotli, lz4, objectstore, postgres, sqlite, json, flare,
iceberg-rs); keep the copies identical.

`open_library` tries, in order, and opens the first that loads
(`find_library` returns the first that exists, for code that opens it
itself):

1. `$CONDA_PREFIX/lib/<file>`, when `CONDA_PREFIX` is an absolute path:
   `pixi run`, `pixi shell`, an activated conda env, or an image that sets
   it (orca's sets `CONDA_PREFIX=/opt/orca`).
2. `<exe dir>/<file>` and `<exe dir>/../lib/<file>`, where `<exe dir>` is
   the running executable's directory after resolving symlinks: a binary
   copied next to its libraries, or installed as `<prefix>/bin/x` with them
   in `<prefix>/lib`.
3. The bare `<file>`, which the dynamic linker looks up on the binary's
   rpath (`mojo build` records the lib directory of the environment it
   built in, so this is the env the binary was built against), then
   `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` and the system directories.

It never looks in the working directory. When nothing loads it raises an
Error that names the library, every place it looked and the last `dlopen`
error, and says to set `CONDA_PREFIX`.

The search costs a few `stat` calls and a `realpath`. The tins open their
library once per process and keep the handle (a `dlopen`/`dlclose` pair
costs hundreds of microseconds), so it runs once; flare, which still opens
some of its shims per call, runs it per call.
"""

from std.ffi import DEFAULT_RTLD, OwnedDLHandle, external_call
from std.os import getenv
from std.os.path import dirname, exists, realpath
from std.sys import CompilationTarget


def executable_dir() -> String:
    """The running executable's directory, symlinks resolved; "" if unknown.
    """
    try:
        comptime if CompilationTarget.is_macos():
            var buf = List[UInt8](length=4096, fill=0)
            var size = List[UInt32](length=1, fill=4095)
            var rc = external_call["_NSGetExecutablePath", Int32](
                buf.unsafe_ptr(), size.unsafe_ptr()
            )
            if rc != 0:
                return String("")
            var n = 0
            while n < len(buf) and buf[n] != 0:
                n += 1
            var path = String(unsafe_from_utf8=Span(buf)[:n])
            return dirname(realpath(path))
        else:
            return dirname(realpath("/proc/self/exe"))
    except:
        return String("")


def _join(dir: String, file: String) -> String:
    # `String("") += ...` rather than `+`: see flare's tls/config.mojo on
    # `getenv(...) + literal` results that alias each other's buffers.
    var out = String("")
    out += dir
    out += "/"
    out += file
    return out^


def library_candidates(file: String) -> List[String]:
    """The paths `open_library` tries for `file`, in order (see the module
    docstring). Absolute candidates are listed whether or not they exist;
    the last is the bare `file`, for the dynamic linker's search."""
    var out = List[String]()
    var prefix = getenv("CONDA_PREFIX", "")
    if prefix.startswith("/"):
        out.append(_join(_join(prefix, "lib"), file))
    var exe = executable_dir()
    if exe.startswith("/"):
        out.append(_join(exe, file))
        out.append(_join(_join(dirname(exe), "lib"), file))
    out.append(file.copy())
    return out^


def find_library(file: String) -> String:
    """The first absolute candidate for `file` that exists, else the bare
    `file` for the dynamic linker: a path for code that opens the library
    itself. `open_library` also tries the next candidate when one exists but
    won't load, and raises the clear Error."""
    for path in library_candidates(file):
        if path.startswith("/") and exists(path):
            return path
    return file.copy()


def open_library(
    tin: StaticString, file: String, flags: Int = DEFAULT_RTLD
) raises -> OwnedDLHandle:
    """`dlopen` the first of `library_candidates(file)` that loads.

    Args:
        tin: The tin's name, for the error message (e.g. "zstd.mojo").
        file: The library's file name (e.g. "libzstdmojo.so").
        flags: `dlopen` flags.

    Raises:
        Error: If none loads. The message names the library, where it
            looked and the last `dlopen` error, and says to set
            `CONDA_PREFIX`.
    """
    var tried = String("")
    var last = String("")
    for path in library_candidates(file):
        # An absolute path that isn't there would only add dyld's
        # "no such file" noise to the error; skip straight past it.
        if path.startswith("/") and not exists(path):
            tried += "\n  " + path + " (no such file)"
            continue
        try:
            return OwnedDLHandle(path, flags)
        except e:
            last = String(e)
            if path.startswith("/"):
                tried += "\n  " + path + " (dlopen failed)"
            else:
                tried += (
                    "\n  "
                    + path
                    + " on the dynamic linker's search path (the build"
                    " environment's lib via rpath, LD_LIBRARY_PATH /"
                    " DYLD_LIBRARY_PATH, system directories)"
                )
    var msg = String(tin) + ": cannot load " + file + "; looked in:" + tried
    if not getenv("CONDA_PREFIX", "").startswith("/"):
        msg += "\nCONDA_PREFIX is not set."
    msg += (
        "\nSet CONDA_PREFIX to the pixi or conda environment that has lib/"
        + file
        + " (or run the program with `pixi run`), or put "
        + file
        + " next to the executable."
    )
    if last:
        msg += "\nLast dlopen error: " + last
    raise Error(msg)
