# Makefile for the Neo4j documentation
#

SRCDIR       = $(CURDIR)
BUILDDIR     = $(SRCDIR)/target
SRCFILE      = $(SRCDIR)/neo4j-manual.txt
CONFDIR      = $(SRCDIR)/conf
DOCBOOKFILE  = $(BUILDDIR)/neo4j-manual.xml
DOCBOOKSHORTINFOFILE = $(BUILDDIR)/neo4j-manual-shortinfo.xml
FOPDIR       = $(BUILDDIR)/pdf
FOPFILE      = $(FOPDIR)/neo4j-manual.fo
FOPPDF       = $(FOPDIR)/neo4j-manual.pdf
TEXTWIDTH    = 80
TEXTDIR      = $(BUILDDIR)/text
TEXTFILE     = $(TEXTDIR)/neo4j-manual.text
TEXTHTMLFILE = $(TEXTFILE).html
HTMLDIR      = $(BUILDDIR)/html
HTMLFILE     = $(HTMLDIR)/neo4j-manual.html
ANNOTATEDDIR = $(BUILDDIR)/annotated
ANNOTATEDFILE= $(HTMLDIR)/neo4j-manual.html

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


GENERAL_FLAGS = $(V) $(K) $(VERS)

.PHONY: all dist docbook help clean pdf latexpdf html singlehtml singlehtml-asciidoc text meta cleanup annotated

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
	@echo "  all         to make all formats"
	@echo "For verbose output, use 'VERBOSE=1'".
	@echo "To keep temporary files, use 'KEEP=1'".
	@echo "To set the version, use 'VERSION=[the version]'".

dist: pdf html annotated text cleanup

all: pdf latexpdf html singlehtml singlehtml-asciidoc text annotated cleanup

clean:
	-rm -rf $(BUILDDIR)/*

cleanup:
ifndef KEEP
	rm -f $(DOCBOOKFILE)
endif
ifndef KEEP
	rm -f $(DOCBOOKSHORTINFOFILE)
endif

docbook:
	mkdir -p $(BUILDDIR)
	asciidoc $(V) $(VERS) --backend docbook --attribute docinfo --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKFILE) $(SRCFILE)
	xmllint --nonet --noout --valid $(DOCBOOKFILE)

docbook-shortinfo:
	mkdir -p $(BUILDDIR)
	asciidoc $(V) $(VERS) --backend docbook --attribute docinfo1 --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKSHORTINFOFILE) $(SRCFILE)
	xmllint --nonet --noout --valid $(DOCBOOKSHORTINFOFILE)

pdf: docbook
	mkdir -p $(FOPDIR)
	cd $(FOPDIR)
	xsltproc --output $(FOPFILE) $(CONFDIR)/fo.xsl $(DOCBOOKFILE)
	cd $(SRCDIR)
	fop -fo $(FOPFILE) -pdf $(FOPPDF)
ifndef KEEP
	rm $(FOPFILE)
endif

latexpdf:
	mkdir -p $(BUILDDIR)/dblatex
	a2x $(GENERAL_FLAGS) -f pdf -D $(BUILDDIR)/dblatex --conf-file=$(CONFDIR)/dblatex.conf $(SRCFILE)

# currently builds docbook format first
html:
	a2x $(GENERAL_FLAGS) -f chunked -D $(BUILDDIR) --conf-file=$(CONFDIR)/chunked.conf --xsl-file=$(CONFDIR)/chunked.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)

# currently builds docbook format first
singlehtml:
	mkdir -p $(HTMLDIR)
	a2x $(GENERAL_FLAGS) -f xhtml -D $(HTMLDIR) --conf-file=$(CONFDIR)/xhtml.conf --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)

# currently builds docbook format first
annotated:
	mkdir -p $(ANNOTATEDDIR)
	a2x $(GENERAL_FLAGS) -a showcomments -f xhtml -D $(ANNOTATEDDIR) --conf-file=$(CONFDIR)/xhtml.conf --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)

# missing: check what files are needed and copy them
singlehtml-notworkingrightnow: docbook-shortinfo
	mkdir -p $(HTMLDIR)
	cd $(HTMLDIR)
	xsltproc --stringparam admon.graphics 1 --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1  --output $(HTMLFILE) $(CONFDIR) $(DOCBOOKSHORTINFOFILE)
	cd $(SRCDIR)

text: docbook-shortinfo
	mkdir -p $(TEXTDIR)
	cd $(TEXTDIR)
	xsltproc  --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1 --stringparam admon.graphics 0  --output $(TEXTHTMLFILE) $(CONFDIR)/text.xsl $(DOCBOOKSHORTINFOFILE)
	cd $(SRCDIR)
	w3m -cols $(TEXTWIDTH) -dump -T text/html -no-graph $(TEXTHTMLFILE) > $(TEXTFILE)
ifndef KEEP
	rm $(TEXTHTMLFILE)
endif

