#!/usr/bin/env python

import os
import re

input_fpath = os.path.splitext(__file__)[0] + ".txt"
output_fpath = os.path.splitext(__file__)[0] + ".cc"

test_counter = 1

schema = {} # name => [column], [column_types], {key_columns}

def proper_split(txt):
    sp = re.split("[, \\(\\)]", txt)
    while "" in sp:
        sp.remove("")
    return sp

class genfile(object):
    def __init__(self, g):
        self.g = g
        self.indent_level = 0
    def writeln(self, txt):
        out = "    " * self.indent_level + txt
        self.g.write(out + "\n")
#        print out
    def indent(self):
        self.indent_level += 1;
    def outdent(self):
        self.indent_level -= 1;
        assert self.indent_level >= 0

txnmgr_type = "TxnMgr2PL"
row_type = "FineLockedRow"
table_type = "UnsortedTable"

with open(output_fpath, "w") as g:
    g = genfile(g)
    g.writeln("""// automatically generated from test-txn-gen.txt, do not modify!
#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

using namespace std;
using namespace base;
using namespace mdb;

""")

    with open(input_fpath) as f:
        line_counter = 0
        for line in f:
            line_counter += 1
            line = line.strip()
            if line.startswith("#") or line == "":
                continue
            g.writeln("// L%d: %s" % (line_counter, line))

            if line.startswith("table"):
                sp = proper_split(line[5:])
                tbl_name = sp[0]
                columns = []
                col_types = []
                key_cols = set()
                for s in sp[1:]:
                    col_name, col_type = s.split(":")
                    if col_name.startswith("*"):
                        col_name = col_name[1:]
                        key_cols.add(col_name)
                    columns += col_name,
                    col_types += col_type,
                schema[tbl_name] = columns, col_types, key_cols

            elif line.startswith("begin_test"):
                idx = line.find("(")
                idx2 = line.find(")")
                test_name = line[idx + 1:idx2]
                if test_name == "":
                    test_name = str(test_counter)
                    test_counter += 1
                idx = line.find("[")
                idx2 = line.find("]")
                sp = proper_split(line[idx+1:idx2])
                for s in sp:
                    if s == "2pl":
                        txnmgr_type = "TxnMgr2PL"
                    elif s == "unsorted":
                        table_type = "UnsortedTable"
                    elif s == "sorted":
                        table_type = "SortedTable"
                    elif s == "fine":
                        row_type = "FineLockedRow"
                    elif s == "coarse":
                        row_type = "CoarseLockedRow"
                    else:
                        raise Exception("cannot handle begin_test attribute: %s" % s)
                g.writeln("TEST(txn_gen, %s) {" % test_name)
                g.indent()
                g.writeln("%s txnmgr;" % txnmgr_type);
                for sch in schema:
                    g.writeln("Schema schema_%s;" % sch)
                    col_names, col_types, key_cols = schema[sch]
                    for idx in range(len(col_names)):
                        cn = col_names[idx]
                        ty = col_types[idx]
                        if cn in key_cols:
                            g.writeln("schema_%s.add_key_column(\"%s\", Value::%s);" % (sch, cn, ty.upper()))
                        else:
                            g.writeln("schema_%s.add_column(\"%s\", Value::%s);" % (sch, cn, ty.upper()))
                    g.writeln("Table* tbl_%s = new %s(&schema_%s);" % (sch, table_type, sch))
                    g.writeln("txnmgr.reg_table(\"%s\", tbl_%s);" % (sch, sch))

            elif line.startswith("begin"):
                txn_name = line[6:-1]
                g.writeln("Txn* txn_%s = txnmgr.start(%d);" % (txn_name, txn_name.__hash__()))

            elif line.startswith("insert"):
                expect_outcome = None
                if line.endswith("ok"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "TRUE"
                elif line.endswith("fail"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "FALSE"
                sp = proper_split(line)
                txn, tbl = sp[1], sp[2]
                g.writeln("{")
                g.indent()
                sch = schema[tbl]
                col_types = sch[1]
                init_list = ""
                for i in range(len(col_types)):
                    if col_types[i] == "i32":
                        init_list += "Value(i32(%s))," % sp[3 + i]
                    elif col_types[i] == "i64":
                        init_list += "Value(i64(%s))," % sp[3 + i]
                    elif col_types[i] == "str":
                        init_list += "Value(%s)," % sp[3 + i]
                    else:
                        raise Exception("no such type: %s" % col_types[i])
                g.writeln("Row* insert_row = %s::create(&schema_%s, vector<Value>({%s}));" % (row_type, tbl, init_list[:-1]))
                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(txn_%s->insert_row(tbl_%s, insert_row));" % (txn, tbl))
                elif expect_outcome == "FALSE":
                    g.writeln("EXPECT_FALSE(txn_%s->insert_row(tbl_%s, insert_row));" % (txn, tbl))
                else:
                    g.writeln("txn_%s->insert_row(tbl_%s, insert_row);" % (txn, tbl))
                g.outdent()
                g.writeln("}")

            elif line.startswith("remove"):
                expect_outcome = None
                if line.endswith("ok"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "TRUE"
                elif line.endswith("fail"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "FALSE"
                sp = proper_split(line)
                txn, tbl = sp[1], sp[2]
                sch = schema[tbl]
                for i in range(len(sch[0])):
                    if sch[0][i] in sch[2]:
                        key_type = sch[1][i]
                g.writeln("do {")
                g.indent()
                if key_type == "i32":
                    value_init = "i32(%s)" % sp[3]
                elif key_type == "i64":
                    value_init = "i64(%s)" % sp[3]
                elif key_type == "str":
                    value_init = sp[3]
                g.writeln("ResultSet result_set = txn_%s->query(tbl_%s, Value(%s));" % (txn, tbl, value_init))
                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(result_set.has_next());")
                g.writeln("if (!result_set.has_next()) {")
                g.indent()
                g.writeln("break;")
                g.outdent()
                g.writeln("}")
                g.writeln("Row* query_row = result_set.next();")
                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(txn_%s->remove_row(tbl_%s, query_row));" % (txn, tbl))
                elif expect_outcome == "FALSE":
                    g.writeln("EXPECT_FALSE(txn_%s->remove_row(tbl_%s, query_row));" % (txn, tbl))
                else:
                    g.writeln("txn_%s->remove_row(tbl_%s, query_row);" % (txn, tbl))
                g.outdent()
                g.writeln("} while(0);")

            elif line.startswith("read"):
                expect_outcome = None
                if line.endswith("ok"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "TRUE"
                elif line.endswith("fail"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "FALSE"
                sp = proper_split(line)
                txn, tbl = sp[1], sp[2]
                sch = schema[tbl]
                for i in range(len(sch[0])):
                    if sch[0][i] in sch[2]:
                        key_type = sch[1][i]
                g.writeln("do {")
                g.indent()
                if key_type == "i32":
                    value_init = "i32(%s)" % sp[3]
                elif key_type == "i64":
                    value_init = "i64(%s)" % sp[3]
                elif key_type == "str":
                    value_init = sp[3]
                g.writeln("ResultSet result_set = txn_%s->query(tbl_%s, Value(%s));" % (txn, tbl, value_init))
                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(result_set.has_next());")
                g.writeln("if (!result_set.has_next()) {")
                g.indent()
                g.writeln("break;")
                g.outdent()
                g.writeln("}")
                g.writeln("Row* query_row = result_set.next();")
                g.writeln("Value v_read;");
                query_col_id = int(sp[4])
                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(txn_%s->read_column(query_row, %d, &v_read));" % (txn, query_col_id))
                elif expect_outcome == "FALSE":
                    g.writeln("EXPECT_FALSE(txn_%s->read_column(query_row, %d, &v_read));" % (txn, query_col_id))
                else:
                    g.writeln("txn_%s->read_column(query_row, %d, &v_read);" % (txn, query_col_id))
                if "=" in line:
                    query_value_type = sch[1][query_col_id]
                    query_value = sp[-1]
                    if query_value_type == "i32":
                        g.writeln("EXPECT_EQ(v_read, Value(i32(%s)));" % query_value);
                    elif query_value_type == "i64":
                        g.writeln("EXPECT_EQ(v_read, Value(i64(%s)));" % query_value);
                    elif query_value_type == "str":
                        g.writeln("EXPECT_EQ(v_read, Value(%s));" % query_value);
                g.outdent()
                g.writeln("} while(0);")

            elif line.startswith("write"):
                expect_outcome = None
                if line.endswith("ok"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "TRUE"
                elif line.endswith("fail"):
                    idx = line.index("->")
                    line = line[:idx].strip()
                    expect_outcome = "FALSE"
                sp = proper_split(line)
                txn, tbl = sp[1], sp[2]
                sch = schema[tbl]
                for i in range(len(sch[0])):
                    if sch[0][i] in sch[2]:
                        key_type = sch[1][i]
                g.writeln("do {")
                g.indent()
                if key_type == "i32":
                    value_init = "i32(%s)" % sp[3]
                elif key_type == "i64":
                    value_init = "i64(%s)" % sp[3]
                elif key_type == "str":
                    value_init = sp[3]
                g.writeln("ResultSet result_set = txn_%s->query(tbl_%s, Value(%s));" % (txn, tbl, value_init))
                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(result_set.has_next());")
                g.writeln("if (!result_set.has_next()) {")
                g.indent()
                g.writeln("break;")
                g.outdent()
                g.writeln("}")
                g.writeln("Row* query_row = result_set.next();")
                query_col_id = int(sp[4])

                write_val = sp[5]
                if sch[1][query_col_id] == "i32":
                    g.writeln("Value v_write(i32(%s));" % write_val)
                elif sch[1][query_col_id] == "i64":
                    g.writeln("Value v_write(i64(%s));" % write_val)
                elif sch[1][query_col_id] == "str":
                    g.writeln("Value v_write(%s);" % write_val)
                else:
                    raise Exception("unknown type: %s" % sp[1][query_col_id])

                if expect_outcome == "TRUE":
                    g.writeln("EXPECT_TRUE(txn_%s->write_column(query_row, %d, v_write));" % (txn, query_col_id))
                elif expect_outcome == "FALSE":
                    g.writeln("EXPECT_FALSE(txn_%s->write_column(query_row, %d, v_write));" % (txn, query_col_id))
                else:
                    g.writeln("txn_%s->write_column(query_row, %d, v_write);" % (txn, query_col_id))
                g.outdent()
                g.writeln("} while(0);")

            elif line.startswith("abort"):
                txn_name = line[6:-1]
                g.writeln("txn_%s->abort();" % txn_name)
                g.writeln("delete txn_%s;" % txn_name)

            elif line.startswith("commit"):
                if line.endswith("ok") or line.endswith("fail"):
                    idx = line.find(")")
                    txn_name = line[7:idx].strip()
                    if line.endswith("ok"):
                        outcome = "TRUE"
                    else:
                        outcome = "FALSE"
                    g.writeln("EXPECT_%s(txn_%s->commit());" % (outcome, txn_name))
                else:
                    txn_name = line[7:-1]
                    g.writeln("txn_%s->commit();" % txn_name)
                g.writeln("delete txn_%s;" % txn_name)

            elif line.startswith("end_test"):
                for sch in schema:
                    g.writeln("delete tbl_%s;" % sch)
                g.outdent()
                g.writeln("}\n\n")
