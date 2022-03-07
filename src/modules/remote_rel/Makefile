# src/modules/remote_rel/Makefile

MODULE_big = remote_rel
#MODULES = remote_rel
OBJS = hooks.o ddl_applier.o ddl_logger.o log_utils.o remote_ddl.o sequence_service.o utils.o $(WIN32RES)
PGFILEDESC = "remote_rel -- Kunlun ddl"

EXTENSION = remote_rel
DATA = remote_rel--1.0.sql

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = src/modules/remote_rel
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif