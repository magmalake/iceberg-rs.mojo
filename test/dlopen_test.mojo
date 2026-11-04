"""The library search outside pixi (see `iceberg_rs._dlopen`).

`pixi run test` also runs this file with `CONDA_PREFIX` unset, which loads
the shim through the binary's rpath, as from a plain shell.
This file checks the search order, that the real library opens, and that a
missing one raises a clear Error rather than aborting.
"""

from std.os import getenv
from std.os.path import dirname
from std.sys import CompilationTarget
from std.testing import assert_equal, assert_true, TestSuite
from iceberg_rs._dlopen import executable_dir, library_candidates, open_library


comptime FILE = "libicebergrsmojo.dylib" if CompilationTarget.is_macos() else "libicebergrsmojo.so"


def test_candidates_order() raises:
    var c = library_candidates(FILE)
    var exe = executable_dir()
    assert_true(exe.startswith("/"), "executable_dir: " + exe)
    var i = 0
    var prefix = getenv("CONDA_PREFIX", "")
    if prefix.startswith("/"):
        assert_equal(c[0], prefix + "/lib/" + FILE)
        i = 1
    assert_equal(c[i], exe + "/" + FILE)
    assert_equal(c[i + 1], dirname(exe) + "/lib/" + FILE)
    assert_equal(c[len(c) - 1], FILE)


def test_missing_library_raises() raises:
    try:
        _ = open_library("iceberg-rs.mojo", "libmagmalake-missing.so")
    except e:
        var msg = String(e)
        assert_true(
            msg.startswith("iceberg-rs.mojo: cannot load libmagmalake-missing.so"), msg
        )
        assert_true("CONDA_PREFIX" in msg, msg)
        return
    raise Error("open_library found a library that doesn't exist")


def test_library_opens() raises:
    _ = open_library("iceberg-rs.mojo", FILE)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
