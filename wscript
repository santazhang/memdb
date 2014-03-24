APPNAME="memdb"
VERSION="0.1"

def options(opt):
    opt.load("compiler_cxx")

def configure(conf):
    conf.load("compiler_cxx")
    _enable_cxx11(conf)
    _enable_debug(conf)
    _extra_warnings(conf)
    conf.env.append_value("CXXFLAGS", "-Wno-nested-anon-types")
    conf.env.LIB_PTHREAD = 'pthread'
    conf.env.INCLUDES_BASE = os.path.join(os.getcwd(), "../base-utils")
    conf.env.LIBPATH_BASE = os.path.join(os.getcwd(), "../base-utils/build")
    conf.env.LIB_BASE = 'base'

def build(bld):
    _depend("test/test-txn-gen.cc", "test/test-txn-gen.txt", "test/test-txn-gen.py")

    bld.stlib(source=bld.path.ant_glob("memdb/*.cc"), target="memdb", includes="memdb", use="BASE PTHREAD")

    def _prog(source, target, includes=".", use="memdb BASE PTHREAD"):
        bld.program(source=source, target=target, includes=includes, use=use)

    _prog(bld.path.ant_glob("test/test*.cc"), "unittest", use="memdb BASE PTHREAD")

#
# waf helper code
#

import os
import sys
from waflib import Logs

# use clang++ as default compiler (for c++11 support on mac)
if sys.platform == 'darwin' and not os.environ.has_key("CXX"):
    os.environ["CXX"] = "clang++"

def _enable_cxx11(conf):
    Logs.pprint("PINK", "C++11 features enabled")
    if sys.platform == "darwin":
        conf.env.append_value("CXXFLAGS", "-stdlib=libc++")
        conf.env.append_value("LINKFLAGS", "-stdlib=libc++")
    conf.env.append_value("CXXFLAGS", "-std=c++0x")

def _enable_debug(conf):
    if os.getenv("DEBUG") in ["true", "1"]:
        Logs.pprint("PINK", "Debug support enabled")
        conf.env.append_value("CXXFLAGS", "-Wall -pthread -ggdb".split())
    else:
        conf.env.append_value("CXXFLAGS", "-Wall -pthread -O3 -ggdb -fno-omit-frame-pointer -DNDEBUG".split())

def _extra_warnings(conf):
    warning_flags = "-Wextra -pedantic -Wformat=2 -Wno-unused-parameter -Wshadow -Wwrite-strings -Wredundant-decls -Wmissing-include-dirs -Wno-format-nonliteral"
    if sys.platform == "darwin":
        warning_flags += " -Wno-gnu -Wstrict-prototypes -Wold-style-definition -Wnested-externs"
    conf.env.append_value("CXXFLAGS", warning_flags.split())

def _run_cmd(cmd):
    Logs.pprint('PINK', cmd)
    os.system(cmd)

def _properly_split(args):
    if args == None:
        return []
    else:
        return args.split()

def _depend(target, source, action):
    target = _properly_split(target)
    source = _properly_split(source)
    for s in source:
        if not os.path.exists(s):
            Logs.pprint('RED', "'%s' not found!" % s)
            exit(1)
    for t in target:
        if not os.path.exists(t):
            _run_cmd(action)
            return
    if not target or min([os.stat(t).st_mtime for t in target]) < max([os.stat(s).st_mtime for s in source]):
        _run_cmd(action)
