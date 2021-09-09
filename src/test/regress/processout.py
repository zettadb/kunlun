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
		if inplan:
			if re.match(r'\s*\(\s*\d+\s*rows?\s*\)', line):
				inplan = False
		elif re.match(r'\s*QUERY\s*PLAN', line):
			inplan = True
		else:
			outf.write(line)
	inf.close()	
	outf.close()

if __name__ == '__main__':
	infile=sys.argv[1]
	process_output(infile)
