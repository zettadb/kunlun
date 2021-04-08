# src/modules/cluster_log_applier/Makefile

MODULES = cluster_log_applier

EXTENSION = cluster_log_applier
DATA = cluster_log_applier--1.0.sql
PGFILEDESC = "cluster_log_applier - apply cluster DDL logs and status sync logs"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/modules/cluster_log_applier
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
