# Makefile for the Neo4j documentation
#

BUILDDIR = target
SRCDIR   = .
SRCFILE  = $(SRCDIR)/neo4j-manual.txt
CONFDIR  = $(SRCDIR)/conf

ifdef VERBOSE
	V = -v
    VA = VERBOSE=1
endif

ifdef KEEP
	K = -k
	KA = KEEP=1
endif

GENERAL_FLAGS = $(V) $(K)
GENERAL_ALL_FLAGS = $(VA) $(KA)

.PHONY: help clean pdf latexpdf html singlehtml text

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean       to clean the build directory"
	@echo "  pdf         to generate a PDF file using FOP"
	@echo "  latexpdf    to generate a PDF file using LaTeX"
	@echo "  html        to make standalone HTML files"
	@echo "  singlehtml  to make a single large HTML file"
	@echo "  text        to make text files"
	@echo "  all         to make all formats"
#	@echo "  test        to test the documentation syntax"
	@echo "For verbose output, use 'VERBOSE=1'".
	@echo "To keep temporary files, use 'KEEP=1'".

clean:
	-rm -rf $(BUILDDIR)/*

pdf:
	mkdir -p $(BUILDDIR)/fop
	a2x $(GENERAL_FLAGS) -f pdf --fop -D $(BUILDDIR)/fop --conf-file=$(CONFDIR)/fop.conf --xsl-file=$(CONFDIR)/fo.xsl --xsltproc-opts "--stringparam toc.section.depth 1 --stringparam admon.graphics 1" $(SRCFILE)

latexpdf:
	mkdir -p $(BUILDDIR)/dblatex
	a2x $(GENERAL_FLAGS) -f pdf -D $(BUILDDIR)/dblatex --conf-file=$(CONFDIR)/dblatex.conf $(SRCFILE)

html:
	a2x $(GENERAL_FLAGS) -f chunked -D $(BUILDDIR) --conf-file=$(CONFDIR)/chunked.conf --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)

singlehtml:
	mkdir -p $(BUILDDIR)/html/images
	#cp -R images/* $(BUILDDIR)/html/images
	svn export --force images $(BUILDDIR)/html/images
	asciidoc $(V) -a docinfo -a icons -d book -o $(BUILDDIR)/html/index.html --conf-file=$(CONFDIR)/asciidoc.conf $(SRCFILE)

text:
	mkdir -p $(BUILDDIR)/text
	a2x $(GENERAL_FLAGS) -f text -D $(BUILDDIR)/text --xsl-file=conf/text.xsl --conf-file=$(CONFDIR)/text.conf $(SRCFILE)

all:
	make $(GENERAL_ALL_FLAGS) pdf latexpdf html singlehtml text

