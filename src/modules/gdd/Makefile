# src/modules/gdd/Makefile

MODULES = global_deadlock_detector

EXTENSION = global_deadlock_detector
DATA = global_deadlock_detector--1.0.sql
PGFILEDESC = "global_deadlock_detector - deadlock detector for global transactions"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/modules/global_deadlock_detector
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
