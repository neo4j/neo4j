# Makefile for the Neo4j documentation
#

SRCDIR           = $(CURDIR)
BUILDDIR         = $(SRCDIR)/target
SRCFILE          = $(SRCDIR)/neo4j-manual.txt
CONFDIR          = $(SRCDIR)/conf
DOCBOOKFILE      = $(BUILDDIR)/neo4j-manual.xml
DOCBOOKSHORTINFOFILE = $(BUILDDIR)/neo4j-manual-shortinfo.xml
FOPDIR           = $(BUILDDIR)/pdf
FOPFILE          = $(FOPDIR)/neo4j-manual.fo
FOPPDF           = $(FOPDIR)/neo4j-manual.pdf
TEXTWIDTH        = 80
TEXTDIR          = $(BUILDDIR)/text
TEXTFILE         = $(TEXTDIR)/neo4j-manual.txt
TEXTHTMLFILE     = $(TEXTFILE).html
SINGLEHTMLDIR    = $(BUILDDIR)/html
SINGLEHTMLFILE   = $(SINGLEHTMLDIR)/neo4j-manual.html
ANNOTATEDDIR     = $(BUILDDIR)/annotated
ANNOTATEDFILE    = $(HTMLDIR)/neo4j-manual.html
CHUNKEDHTMLDIR   = $(BUILDDIR)/chunked
CHUNKEDOFFLINEHTMLDIR = $(BUILDDIR)/chunked-offline
CHUNKEDTARGET     = $(BUILDDIR)/neo4j-manual.chunked
MANPAGES         = $(BUILDDIR)/manpages

ifdef VERBOSE
	V = -v
    VA = VERBOSE=1
endif

ifdef KEEP
	K = -k
	KA = KEEP=1
endif

ifdef VERSION
    VERS = --attribute revnumber=$(VERSION)
else
    VERS = --attribute revnumber=-neo4j-version
endif

ifdef IMPORTDIR
    IMPDIR = --attribute importdir=$(IMPORTDIR)
else
    IMPDIR = --attribute importdir=$(SRCDIR)
    IMPORTDIR = $(SRCDIR)
endif

GENERAL_FLAGS = $(V) $(K) $(VERS) $(IMPDIR)

.PHONY: all dist docbook help clean pdf latexpdf html offline-html singlehtml text cleanup annotated manpages

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean       to clean the build directory"
	@echo "  dist        to generate the common distribution formats"
	@echo "  pdf         to generate a PDF file using FOP"
	@echo "  latexpdf    to generate a PDF file using LaTeX"
	@echo "  html        to make standalone HTML files"
	@echo "  singlehtml  to make a single large HTML file"
	@echo "  text        to make text files"
	@echo "  annotated   to make a single annotated HTML file"
	@echo "  manpages    to make the manpages"
	@echo "  all         to make all formats"
	@echo "For verbose output, use 'VERBOSE=1'".
	@echo "To keep temporary files, use 'KEEP=1'".
	@echo "To set the version, use 'VERSION=[the version]'".

dist: pdf html offline-html annotated text manpages cleanup

all: pdf latexpdf html offline-html singlehtml text annotated manpages cleanup

clean:
	-rm -rf $(BUILDDIR)/*

cleanup:
ifndef KEEP
	rm -f $(DOCBOOKFILE)
	rm -f $(DOCBOOKSHORTINFOFILE)
	rm -f $(BUILDDIR)/*.xml
endif

docbook:  manpages
	mkdir -p $(BUILDDIR)
	asciidoc $(V) $(VERS) $(IMPDIR) --backend docbook --attribute docinfo --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKFILE) $(SRCFILE)
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKFILE)

docbook-shortinfo:  manpages
	mkdir -p $(BUILDDIR)
	asciidoc $(V) $(VERS) $(IMPDIR) --backend docbook --attribute docinfo1 --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKSHORTINFOFILE) $(SRCFILE)
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKSHORTINFOFILE)

pdf:  docbook
	mkdir -p $(FOPDIR)
	cd $(FOPDIR)
	xsltproc --xinclude --output $(FOPFILE) $(CONFDIR)/fo.xsl $(DOCBOOKFILE)
	cd $(SRCDIR)
	fop -fo $(FOPFILE) -pdf $(FOPPDF)
ifndef KEEP
	rm -f $(FOPFILE)
endif

latexpdf:  manpages
	mkdir -p $(BUILDDIR)/dblatex
	a2x $(GENERAL_FLAGS) -L -f pdf -D $(BUILDDIR)/dblatex --conf-file=$(CONFDIR)/dblatex.conf $(SRCFILE)

# currently builds docbook format first
html:  manpages
	a2x $(GENERAL_FLAGS) -L -f chunked -D $(BUILDDIR) --conf-file=$(CONFDIR)/chunked.conf --xsl-file=$(CONFDIR)/chunked.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	mv $(CHUNKEDTARGET) $(CHUNKEDHTMLDIR)
	cp -fr $(SRCDIR)/js $(CHUNKEDHTMLDIR)/js

# currently builds docbook format first
offline-html:  manpages
	a2x $(GENERAL_FLAGS) -L -f chunked -D $(BUILDDIR) --conf-file=$(CONFDIR)/chunked.conf --xsl-file=$(CONFDIR)/chunked-offline.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	mv $(CHUNKEDTARGET) $(CHUNKEDOFFLINEHTMLDIR)
	cp -fr $(SRCDIR)/js $(CHUNKEDOFFLINEHTMLDIR)/js

# currently builds docbook format first
singlehtml:  manpages
	mkdir -p $(SINGLEHTMLDIR)
	a2x $(GENERAL_FLAGS) -L -f xhtml -D $(SINGLEHTMLDIR) --conf-file=$(CONFDIR)/xhtml.conf --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	cp -fr $(SRCDIR)/js $(SINGLEHTMLDIR)/js

# currently builds docbook format first
annotated:  manpages
	mkdir -p $(ANNOTATEDDIR)
	a2x $(GENERAL_FLAGS) -L -a showcomments -f xhtml -D $(ANNOTATEDDIR) --conf-file=$(CONFDIR)/xhtml.conf --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	cp -fr $(SRCDIR)/js $(ANNOTATEDDIR)/js

# missing: check what files are needed and copy them
singlehtml-notworkingrightnow: docbook-shortinfo
	mkdir -p $(SINGLEHTMLDIR)
	cd $(SINGLEHTMLDIR)
	xsltproc --xinclude --stringparam admon.graphics 1 --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1  --output $(SINGLEHTMLFILE) $(CONFDIR) $(DOCBOOKSHORTINFOFILE)
	cd $(SRCDIR)

text: docbook-shortinfo
	mkdir -p $(TEXTDIR)
	cd $(TEXTDIR)
	xsltproc --xinclude --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1 --stringparam admon.graphics 0  --output $(TEXTHTMLFILE) $(CONFDIR)/text.xsl $(DOCBOOKSHORTINFOFILE)
	cd $(SRCDIR)
	w3m -cols $(TEXTWIDTH) -dump -T text/html -no-graph $(TEXTHTMLFILE) > $(TEXTFILE)
ifndef KEEP
	rm -f $(TEXTHTMLFILE)
	rm -f $(TEXTDIR)/*.html
endif

manpages:
	mkdir -p $(MANPAGES)
	a2x -k -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-shell-docs-jar/ops/neo4j-shell.1.txt
	mv $(MANPAGES)/*.xml $(BUILDDIR)
	gzip $(MANPAGES)/*
