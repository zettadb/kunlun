#! /bin/bash

basedir="$1"

for input in constraints copy create_function_1 create_function_2 misc aggregates remote_dml2 select select_distinct select_distinct_on union; do
	source_name="$input.source"
	sed -e "s!@abs_srcdir@!$basedir!g" \
	    -e "s!@abs_builddir@!$basedir!g" \
	    -e "s!@libdir@!$basedir/lib!g" \
	    -e "s!@DLSUFFIX@!.so!g" input/$source_name > sql/$input.sql
done
