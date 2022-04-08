#! /usr/bin/python

import sys
import re

def process_output(infile):
	inf = open(infile, 'r')
	outfile = infile + ".p"
	outf = open(outfile, "w")
	lines = inf.xreadlines()
	inplan = False
	for line in lines:
            if line.find("DDL_STATEMENT_BEGIN") >= 0 or line.find("DDL_STATEMENT_END") >= 0:
                continue
            if inplan:
                if re.match(r'\s*\(\s*\d+\s*rows?\s*\)', line):
                    inplan = False
            # expalain_sq_limit is in subselect.sql
            # pg_get_viewdef is in aggregates.sql, create_view.sql, etc.
            # explain_parallel_append is in partition_prune.sql
            # explain_parallel_sort_stats is in select_parallel.sql
            elif re.match(r'\s*QUERY\s*PLAN', line) or \
                 re.match('^\s*explain_sq_limit\s*$', line) or \
                 re.match('^\s*pg_get_viewdef\s*$', line) or \
                 re.match('^\s*explain_parallel_append\s*$', line) or \
                 re.match('^\s*explain_parallel_sort_stats\s*$', line):
                inplan = True
            else:
                outf.write(line)
	inf.close()	
	outf.close()

if __name__ == '__main__':
	infile=sys.argv[1]
	process_output(infile)
