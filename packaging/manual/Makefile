# Makefile for the Neo4j documentation
#

BUILDDIR         = $(CURDIR)/target
SRCDIR           = $(BUILDDIR)/classes
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

dist: pdf html annotated text manpages cleanup

all: pdf latexpdf html offline-html singlehtml text annotated manpages cleanup

clean:
	-rm -rf $(BUILDDIR)/*

cleanup:
	#
	#
	# Cleaning up.
	#
	#
ifndef KEEP
	rm -f $(DOCBOOKFILE)
	rm -f $(DOCBOOKSHORTINFOFILE)
	rm -f $(BUILDDIR)/*.xml
	rm -f $(FOPDIR)/images
endif

copyimages:
	#
	#
	# Copying images from source projects.
	#
	#
	cp -fr $(IMPORTDIR)/*/*/images/* $(SRCDIR)/images/

docbook:  manpages copyimages
	#
	#
	# Building docbook output.
	#
	#
	mkdir -p $(BUILDDIR)
	asciidoc $(V) $(VERS) $(IMPDIR) --backend docbook --attribute docinfo --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKFILE) $(SRCFILE)
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKFILE)

docbook-shortinfo:  manpages copyimages
	#
	#
	# Building docbook output with short info.
	#
	#
	mkdir -p $(BUILDDIR)
	asciidoc $(V) $(VERS) $(IMPDIR) --backend docbook --attribute docinfo1 --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKSHORTINFOFILE) $(SRCFILE)
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKSHORTINFOFILE)

pdf:  docbook copyimages
	#
	#
	# Building PDF.
	#
	#
	mkdir -p $(FOPDIR)
	cd $(FOPDIR)
	xsltproc --xinclude --output $(FOPFILE) $(CONFDIR)/fo.xsl $(DOCBOOKFILE)
	ln -s $(SRCDIR)/images $(FOPDIR)/images
	fop -fo $(FOPFILE) -pdf $(FOPPDF)
ifndef KEEP
	rm -f $(FOPFILE)
endif

latexpdf:  manpages copyimages
	#
	#
	# Building PDF using LaTex.
	#
	#
	mkdir -p $(BUILDDIR)/dblatex
	a2x $(GENERAL_FLAGS) -L -f pdf -D $(BUILDDIR)/dblatex --conf-file=$(CONFDIR)/dblatex.conf $(SRCFILE)

# currently builds docbook format first
html: manpages copyimages
	#
	#
	# Building html output.
	#
	#
	a2x $(GENERAL_FLAGS) -L -f chunked -D $(BUILDDIR) --attribute docinfo1 --attribute=icons --doctype book --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --xsl-file=$(CONFDIR)/chunked.xsl --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" $(SRCFILE)
	mv $(CHUNKEDTARGET) $(CHUNKEDHTMLDIR)
	cp -fr $(SRCDIR)/js $(CHUNKEDHTMLDIR)/js

# currently builds docbook format first
offline-html:  manpages copyimages
	#
	#
	# Building html output for offline use.
	#
	#
	a2x $(GENERAL_FLAGS) -L -f chunked -D $(BUILDDIR) --attribute docinfo1 --attribute=icons --doctype book --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --xsl-file=$(CONFDIR)/chunked-offline.xsl --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" $(SRCFILE)
	mv $(CHUNKEDTARGET) $(CHUNKEDOFFLINEHTMLDIR)
	cp -fr $(SRCDIR)/js $(CHUNKEDOFFLINEHTMLDIR)/js

# currently builds docbook format first
singlehtml:  manpages copyimages
	#
	#
	# Building single html file output.
	#
	#
	mkdir -p $(SINGLEHTMLDIR)
	a2x $(GENERAL_FLAGS) -L -f xhtml -D $(SINGLEHTMLDIR) --conf-file=$(CONFDIR)/xhtml.conf --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	cp -fr $(SRCDIR)/js $(SINGLEHTMLDIR)/js

# currently builds docbook format first
annotated:  manpages copyimages
	#
	#
	# Building annotated html output.
	#
	#
	mkdir -p $(ANNOTATEDDIR)
	a2x $(GENERAL_FLAGS) -L -a showcomments -f xhtml -D $(ANNOTATEDDIR) --conf-file=$(CONFDIR)/xhtml.conf --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	cp -fr $(SRCDIR)/js $(ANNOTATEDDIR)/js

# missing: check what files are needed and copy them
singlehtml-notworkingrightnow: docbook-shortinfo
	mkdir -p $(SINGLEHTMLDIR)
	cd $(SINGLEHTMLDIR)
	xsltproc --xinclude --stringparam admon.graphics 1 --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1  --output $(SINGLEHTMLFILE) $(CONFDIR) $(DOCBOOKSHORTINFOFILE)
	cd $(SRCDIR)

text: docbook-shortinfo
	#
	#
	# Building text output.
	#
	#
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
	#
	#
	# Building manpages.
	#
	#
	mkdir -p $(MANPAGES)
	# shell
	a2x -k -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-shell-docs-jar/man/neo4j-shell.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-shell-docs-jar/man/neo4j-shell.1.txt
	mv $(MANPAGES)/neo4j-shell.1.text $(MANPAGES)/neo4j-shell.txt
	# neo4j
	a2x -k -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j.1.txt
	mv $(MANPAGES)/neo4j.1.text $(MANPAGES)/neo4j.txt
	# neo4j-coordinator
	a2x -k -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator.1.txt
	mv $(MANPAGES)/neo4j-coordinator.1.text $(MANPAGES)/neo4j-coordinator.txt
	# neo4j-coordinator-shell
	a2x -k -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator-shell.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator-shell.1.txt
	mv $(MANPAGES)/neo4j-coordinator-shell.1.text $(MANPAGES)/neo4j-coordinator-shell.txt
	# clean up
	mv $(MANPAGES)/*.xml $(BUILDDIR)
	rm -rf $(MANPAGES)/*.html
	# gzip -q $(MANPAGES)/*

