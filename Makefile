# Makefile for the Neo4j Manual.
#
# Note: requires mvn to unpack some stuff first.

# Project Configuration
project_name               = cypher-refcard
language                   = en

# Minimal setup
target                     = target
config_dir                 = $(CURDIR)/conf
build_dir                  = $(CURDIR)/$(target)
tools_dir                  = $(build_dir)/tools
make_dir                   = $(tools_dir)/make

include $(make_dir)/context-refcard.make


