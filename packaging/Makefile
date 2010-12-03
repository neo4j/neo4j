# Makefile for the Neo4j documentation
#

BUILDDIR    = target
SRCDIR      = .
SRCFILE     = $(SRCDIR)/neo4j-manual.txt
CONFDIR     = $(SRCDIR)/conf
DOCBOOKFILE = $(BUILDDIR)/neo4j-manual.xml
FOPDIR      = $(BUILDDIR)/pdf
FOPFILE     = $(FOPDIR)/neo4j-manual.fo
FOPPDF      = $(FOPDIR)/neo4j-manual.pdf

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

.PHONY: all dist docbook help clean pdf latexpdf html singlehtml singlehtml-asciidoc text meta

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean       to clean the build directory"
	@echo "  dist        to generate the common distribution formats"
	@echo "  pdf         to generate a PDF file using FOP"
	@echo "  latexpdf    to generate a PDF file using LaTeX"
	@echo "  html        to make standalone HTML files"
	@echo "  singlehtml  to make a single large HTML file"
	@echo "  singlehtml-asciidoc  to make a single HTML file using asciidoc only"
	@echo "  text        to make text files"
	@echo "  all         to make all formats"
	@echo "For verbose output, use 'VERBOSE=1'".
	@echo "To keep temporary files, use 'KEEP=1'".

dist: pdf html singlehtml text

all: pdf latexpdf html singlehtml singlehtml-asciidoc text

clean:
	-rm -rf $(BUILDDIR)/*

docbook:
	mkdir -p $(BUILDDIR)
	asciidoc $(V) --backend docbook --attribute docinfo --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKFILE) $(SRCFILE)
	xmllint --nonet --noout --valid $(DOCBOOKFILE)

pdf-a2x:
	mkdir -p $(BUILDDIR)/fop
	a2x $(GENERAL_FLAGS) -f pdf --fop -D $(BUILDDIR)/fop --conf-file=$(CONFDIR)/fop.conf --xsl-file=$(CONFDIR)/fo.xsl --xsltproc-opts "--stringparam toc.section.depth 1 --stringparam admon.graphics 1" $(SRCFILE)

pdf: docbook
	mkdir -p $(FOPDIR)
	cd $(FOPDIR)
	xsltproc --stringparam toc.section.depth 1 --stringparam admon.graphics 1 --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1  --output $(FOPFILE) $(CONFDIR)/fo.xsl $(DOCBOOKFILE)
	cd $(SRCDIR)
	fop -fo $(FOPFILE) -pdf $(FOPPDF)
ifndef KEEP
	rm $(FOPFILE)
endif

latexpdf:
	mkdir -p $(BUILDDIR)/dblatex
	a2x $(GENERAL_FLAGS) -f pdf -D $(BUILDDIR)/dblatex --conf-file=$(CONFDIR)/dblatex.conf $(SRCFILE)

html:
	a2x $(GENERAL_FLAGS) -f chunked -D $(BUILDDIR) --conf-file=$(CONFDIR)/chunked.conf --xsl-file=$(CONFDIR)/chunked.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)

# use the asciidoc tool only, not docbok
singlehtml-asciidoc:
	mkdir -p $(BUILDDIR)/singlehtml/images
	#cp -R images/* $(BUILDDIR)/singlehtml/images
	svn export --force images $(BUILDDIR)/singlehtml/images
	asciidoc $(V) -a docinfo -a icons -d book -o $(BUILDDIR)/singlehtml/index.html --conf-file=$(CONFDIR)/asciidoc.conf $(SRCFILE)

singlehtml:
	mkdir -p $(BUILDDIR)/html
	a2x $(GENERAL_FLAGS) -f xhtml -D $(BUILDDIR)/html --conf-file=$(CONFDIR)/xhtml.conf --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)

text:
	mkdir -p $(BUILDDIR)/text
	a2x $(GENERAL_FLAGS) -f text -D $(BUILDDIR)/text --xsl-file=conf/text.xsl --conf-file=$(CONFDIR)/text.conf $(SRCFILE)

meta:
	mkdir -p $(BUILDDIR)/meta-fop
	a2x $(GENERAL_FLAGS) -f pdf --fop -D $(BUILDDIR)/meta-fop --conf-file=$(CONFDIR)/fop.conf --xsl-file=$(CONFDIR)/fo.xsl --xsltproc-opts "--stringparam toc.section.depth 1 --stringparam admon.graphics 1" $(SRCDIR)/meta/index.txt

