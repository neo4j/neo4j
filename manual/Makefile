# Makefile for the Neo4j documentation
#

BUILDDIR         = $(CURDIR)/target
SRCDIR           = $(BUILDDIR)/classes
SRCFILE          = $(SRCDIR)/neo4j-manual.txt
METAFROMDIR      = $(CURDIR)/src/docs
METAFROMCSSDIR   = $(CURDIR)/src/main/resources/css
METAFROMIMGDIR   = $(CURDIR)/src/main/resources/images
METASRCDIR       = $(BUILDDIR)/metadocssrc
METASRCFILE      = $(METASRCDIR)/index.txt
IMGDIR           = $(SRCDIR)/images
CSSDIR           = $(SRCDIR)/css
JSDIR            = $(SRCDIR)/js
CONFDIR          = $(SRCDIR)/conf
DOCBOOKFILE      = $(BUILDDIR)/neo4j-manual.xml
DOCBOOKSHORTINFOFILE = $(BUILDDIR)/neo4j-manual-shortinfo.xml
DOCBOOKFILEHTML  = $(BUILDDIR)/neo4j-manual-html.xml
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
CHUNKEDSHORTINFOTARGET = $(BUILDDIR)/neo4j-manual-html.chunked
METAHTMLDIR      = $(BUILDDIR)/metadocs
MANPAGES         = $(BUILDDIR)/manpages
UPGRADE          = $(BUILDDIR)/upgrade
FILTERSRC        = $(CURDIR)/src/bin/resources
FILTERDEST       = ~/.asciidoc/filters
SCRIPTDIR        = $(CURDIR)/src/build

ifdef VERBOSE
	V = -v
	VA = VERBOSE=1
endif

ifdef KEEP
	K = -k
	KA = KEEP=1
endif

ifdef VERSION
	VERSNUM =$(VERSION)
else
	VERSNUM =-neo4j-version
endif

ifdef IMPORTDIR
	IMPDIR = --attribute importdir=$(IMPORTDIR)
else
	IMPDIR = --attribute importdir=$(BUILDDIR)/docs
	IMPORTDIR = $(BUILDDIR)/docs
endif

ifneq (,$(findstring SNAPSHOT,$(VERSNUM)))
	GITVERSNUM =master
else
	GITVERSNUM =$(VERSION)
endif

ifndef VERSION
	GITVERSNUM =master
endif

VERS =  --attribute revnumber=$(VERSNUM)

GITVERS = --attribute gitversion=$(GITVERSNUM)

ASCIIDOC_FLAGS = $(V) $(VERS) $(GITVERS) $(IMPDIR)

A2X_FLAGS = $(K) $(ASCIIDOC_FLAGS)

.PHONY: all dist docbook help clean pdf latexpdf html offline-html singlehtml text cleanup annotated manpages upgrade installfilter html-check text-check meta check

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean       to clean the build directory"
	@echo "  dist        to generate the common distribution formats"
	@echo "  pdf         to generate a PDF file using FOP"
	@echo "  html        to make standalone HTML files"
	@echo "  singlehtml  to make a single large HTML file"
	@echo "  text        to make text files"
	@echo "  annotated   to make a single annotated HTML file"
	@echo "  manpages    to make the manpages"
	@echo "For verbose output, use 'VERBOSE=1'".
	@echo "To keep temporary files, use 'KEEP=1'".
	@echo "To set the version, use 'VERSION=[the version]'".
	@echo "To set the importdir, use 'IMPORTDIR=[the importdir]'".

dist: installfilter offline-html html html-check text text-check annotated pdf manpages upgrade cleanup

check: html-check text-check cleanup

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
	rm -f $(DOCBOOKFILEPDF)
	rm -f $(DOCBOOKSHORTINFOFILE)
	rm -f $(BUILDDIR)/*.xml
	rm -f $(ANNOTATEDDIR)/*.xml
	rm -f $(FOPDIR)/images
	rm -f $(FOPFILE)
	rm -f $(UPGRADE)/*.xml
	rm -f $(UPGRADE)/*.html
endif

installfilter:
	#
	#
	# Installing asciidoc filters.
	#
	#
	mkdir -p $(FILTERDEST)
	cp -fr $(FILTERSRC)/* $(FILTERDEST)

copyimages:
	#
	#
	# Copying images from source projects.
	#
	#
	cp -fr $(IMPORTDIR)/*/*/images/* $(SRCDIR)/images/

html-check: offline-html
	#
	#
	# Checking that identifiers exist where they should.
	#
	$(SCRIPTDIR)/htmlcheck.sh $(CHUNKEDOFFLINEHTMLDIR)

text-check: text
	#
	#
	# Checking that snippets are in place.
	#
	$(SCRIPTDIR)/textcheck.sh $(TEXTFILE)

docbook:  manpages copyimages
	#
	#
	# Building docbook output.
	#
	#
	mkdir -p $(BUILDDIR)
	asciidoc $(ASCIIDOC_FLAGS) --backend docbook --attribute docinfo --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKFILE) $(SRCFILE)
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKFILE)

docbook-shortinfo:  manpages copyimages
	#
	#
	# Building docbook output with short info.
	# Checking for missing include files.
	# Checking DocBook validity.
	#
	#
	mkdir -p $(BUILDDIR)
	asciidoc $(ASCIIDOC_FLAGS) --backend docbook --attribute docinfo1 --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --out-file $(DOCBOOKSHORTINFOFILE) $(SRCFILE) 2>&1 | $(SCRIPTDIR)/outputcheck-includefiles.sh
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKSHORTINFOFILE)

docbook-html:  manpages copyimages
	#
	#
	# Building docbook output with short info for html outputs.
	# Checking DocBook validity.
	#
	#
	mkdir -p $(BUILDDIR)
	asciidoc $(ASCIIDOC_FLAGS) --backend docbook --attribute docinfo1 --doctype book --conf-file=$(CONFDIR)/asciidoc.conf --conf-file=$(CONFDIR)/docbook45.conf --conf-file=$(CONFDIR)/linkedimages.conf --out-file $(DOCBOOKFILEHTML) $(SRCFILE)
	# replacing svg files with png files by ugly hack
	sed -e 's/.svg"/.svg.png"/g' <$(DOCBOOKFILEHTML) >$(DOCBOOKFILEHTML).tmp
	rm $(DOCBOOKFILEHTML)
	mv $(DOCBOOKFILEHTML).tmp $(DOCBOOKFILEHTML)
	xmllint --nonet --noout --xinclude --postvalid $(DOCBOOKFILEHTML)

pdf: docbook-shortinfo copyimages
	#
	#
	# Building PDF.
	#
	#
	mkdir -p $(FOPDIR)
	cd $(FOPDIR)
	xsltproc --xinclude --output $(FOPFILE) $(CONFDIR)/fo.xsl $(DOCBOOKSHORTINFOFILE)
	ln -s $(SRCDIR)/images $(FOPDIR)/images
	export FOP_OPTS="-Xmx2048m"
	fop -fo $(FOPFILE) -pdf $(FOPPDF) -c $(CONFDIR)/fop.xml

latexpdf: docbook-shortinfo copyimages
	#
	#
	# Building PDF using LaTex.
	#
	#
	mkdir -p $(BUILDDIR)/dblatex
	cp -f $(DOCBOOKSHORTINFOFILE) $(BUILDDIR)/dblatex/neo4j-manual-shortinfo.xml
	a2x $(A2X_FLAGS) -L -f pdf -D $(BUILDDIR)/dblatex --conf-file=$(CONFDIR)/dblatex.conf $(DOCBOOKSHORTINFOFILE)

html: manpages copyimages docbook-html
	#
	#
	# Building html output.
	# Checking for missing images/resources.
	#
	#
	a2x $(V) -L -f chunked -D $(BUILDDIR) --xsl-file=$(CONFDIR)/chunked.xsl -r $(IMGDIR) -r $(CSSDIR) --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" --xsltproc-opts "--stringparam toc.section.depth 1" $(DOCBOOKFILEHTML) 2>&1 | $(SCRIPTDIR)/outputcheck-images.sh
	rm -rf $(CHUNKEDHTMLDIR)
	mv $(CHUNKEDSHORTINFOTARGET) $(CHUNKEDHTMLDIR)
	cp -fr $(JSDIR) $(CHUNKEDHTMLDIR)/js
	cp -fr $(CSSDIR)/* $(CHUNKEDHTMLDIR)/css
	cp -fr $(SRCDIR)/images/*.svg $(CHUNKEDHTMLDIR)/images

offline-html:  manpages copyimages docbook-html
	#
	#
	# Building html output for offline use.
	#
	#
	a2x $(V) -L -f chunked -D $(BUILDDIR) --xsl-file=$(CONFDIR)/chunked-offline.xsl -r $(IMGDIR) -r $(CSSDIR) --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" --xsltproc-opts "--stringparam toc.section.depth 1" $(DOCBOOKFILEHTML)
	rm -rf $(CHUNKEDOFFLINEHTMLDIR)
	mv $(CHUNKEDSHORTINFOTARGET) $(CHUNKEDOFFLINEHTMLDIR)
	cp -fr $(JSDIR) $(CHUNKEDOFFLINEHTMLDIR)/js
	cp -fr $(CSSDIR)/* $(CHUNKEDOFFLINEHTMLDIR)/css/
	cp -fr $(SRCDIR)/images/*.svg $(CHUNKEDOFFLINEHTMLDIR)/images

# currently builds docbook format first
singlehtml:  manpages copyimages
	#
	#
	# Building single html file output.
	#
	#
	mkdir -p $(SINGLEHTMLDIR)
	a2x $(A2X_FLAGS) -L -f xhtml -D $(SINGLEHTMLDIR) --conf-file=$(CONFDIR)/xhtml.conf --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/linkedimages.conf" --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	cp -fr $(JSDIR) $(SINGLEHTMLDIR)/js

# builds docbook format first
annotated:  manpages copyimages
	#
	#
	# Building annotated html output.
	#
	#
	mkdir -p $(ANNOTATEDDIR)
	a2x $(A2X_FLAGS) -L -a showcomments -f xhtml -D $(ANNOTATEDDIR) --conf-file=$(CONFDIR)/xhtml.conf --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/linkedimages.conf" --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" $(SRCFILE)
	cp -fr $(SRCDIR)/js $(ANNOTATEDDIR)/js
	cp -fr $(SRCDIR)/css/* $(ANNOTATEDDIR)/css
	cp -fr $(SRCDIR)/images/*.svg $(ANNOTATEDDIR)/images

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
	a2x -k $(V) -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-shell-docs-jar/man/neo4j-shell.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-shell-docs-jar/man/neo4j-shell.1.txt
	mv $(MANPAGES)/neo4j-shell.1.text $(MANPAGES)/neo4j-shell.txt
	# neo4j
	a2x -k $(V) -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j.1.txt
	mv $(MANPAGES)/neo4j.1.text $(MANPAGES)/neo4j.txt
	# neo4j-coordinator
	a2x -k $(V) -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator.1.txt
	mv $(MANPAGES)/neo4j-coordinator.1.text $(MANPAGES)/neo4j-coordinator.txt
	# neo4j-coordinator-shell
	a2x -k $(V) -f manpage -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator-shell.1.txt
	a2x -k -f text -d  manpage -D $(MANPAGES) $(IMPORTDIR)/neo4j-server-docs-jar/man/neo4j-coordinator-shell.1.txt
	mv $(MANPAGES)/neo4j-coordinator-shell.1.text $(MANPAGES)/neo4j-coordinator-shell.txt
	# clean up
	mkdir -p $(ANNOTATEDDIR)
	cp $(MANPAGES)/*.xml $(ANNOTATEDDIR)
	mv $(MANPAGES)/*.xml $(BUILDDIR)
	rm -rf $(MANPAGES)/*.html
	# gzip -q $(MANPAGES)/*

upgrade:
	#
	#
	# Building upgrade text.
	#
	#
	mkdir -p $(UPGRADE)
	a2x -k -f text -D $(UPGRADE) $(IMPORTDIR)/neo4j-docs-jar/ops/upgrades.txt
	mv $(UPGRADE)/upgrades.text $(UPGRADE)/UPGRADE.txt

meta:
	#
	#
	# Building metadocs.
	#
	#
	rm -rf $(METASRCDIR)
	cp -fr $(METAFROMDIR) $(METASRCDIR)
	ln -s $(METAFROMCSSDIR) $(METASRCDIR)/css
	ln -s $(METAFROMIMGDIR) $(METASRCDIR)/images
	rm -rf $(METAHTMLDIR)
	mkdir -p $(METAHTMLDIR)
	cp -fr $(METAFROMCSSDIR) $(METAHTMLDIR)/css
	a2x -L -f xhtml -D $(METAHTMLDIR) --conf-file=$(CONFDIR)/xhtml.conf --asciidoc-opts "--conf-file=$(CONFDIR)/asciidoc.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/docbook45.conf" --asciidoc-opts "--conf-file=$(CONFDIR)/linkedimages.conf" --xsl-file=$(CONFDIR)/xhtml.xsl --xsltproc-opts "--stringparam admon.graphics 1" --resource $(IMGDIR) $(METASRCFILE)
	cp -fr $(JSDIR) $(METAHTMLDIR)/js

