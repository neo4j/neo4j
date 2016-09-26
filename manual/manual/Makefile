# Makefile for the Neo4j Manual.
#

# Project Configuration
project_name               = neo4j-manual
language                   = en

# Minimal setup
target                     = target
build_dir                  = $(CURDIR)/$(target)
config_dir                 = $(CURDIR)/conf
tools_dir                  = $(build_dir)/tools
make_dir                   = $(tools_dir)/make

include $(make_dir)/context-manual.make

manpage_list = "neo4j server" "neo4j-admin server" "neo4j-shell shell-docs" "neo4j-backup backup-docs" "neo4j-import import-tool-docs"

webhelp_dist: initialize install-extensions manpages copy-images docbook docbook-html year-check
# cleanup
