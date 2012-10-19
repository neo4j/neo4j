# Makefile for the Neo4j documentation
#

PROJECTNAME      = neo4j-manual
BUILDDIR         = $(CURDIR)/target
SRCDIR           = $(BUILDDIR)/classes
SRCFILE          = $(SRCDIR)/$(PROJECTNAME).asciidoc
IMGDIR           = $(SRCDIR)/images
CSSDIR           = $(SRCDIR)/css
JSDIR            = $(SRCDIR)/js
CONFDIR          = $(SRCDIR)/conf
DOCBOOKFILE      = $(BUILDDIR)/$(PROJECTNAME)-shortinfo.xml
DOCBOOKFILEHTML  = $(BUILDDIR)/$(PROJECTNAME)-html.xml
FOPDIR           = $(BUILDDIR)/pdf
FOPFILE          = $(FOPDIR)/$(PROJECTNAME).fo
FOPPDF           = $(FOPDIR)/$(PROJECTNAME).pdf
TEXTWIDTH        = 80
TEXTDIR          = $(BUILDDIR)/text
TEXTFILE         = $(TEXTDIR)/$(PROJECTNAME).txt
TEXTHTMLFILE     = $(TEXTFILE).html
SINGLEHTMLDIR    = $(BUILDDIR)/html
SINGLEHTMLFILE   = $(SINGLEHTMLDIR)/$(PROJECTNAME).html
ANNOTATEDDIR     = $(BUILDDIR)/annotated
ANNOTATEDFILE    = $(HTMLDIR)/$(PROJECTNAME).html
CHUNKEDHTMLDIR   = $(BUILDDIR)/chunked
CHUNKEDOFFLINEHTMLDIR = $(BUILDDIR)/chunked-offline
CHUNKEDTARGET     = $(BUILDDIR)/$(PROJECTNAME).chunked
CHUNKEDSHORTINFOTARGET = $(BUILDDIR)/$(PROJECTNAME)-html.chunked
MANPAGES         = $(BUILDDIR)/manpages
UPGRADE          = $(BUILDDIR)/upgrade
FILTERSRC        = $(CURDIR)/src/bin/resources
FILTERDEST       = ~/.asciidoc/filters
SCRIPTDIR        = $(CURDIR)/src/build
ASCIDOCDIR       = $(CURDIR)/src/bin/asciidoc
ASCIIDOC         = $(ASCIDOCDIR)/asciidoc.py
A2X              = $(ASCIDOCDIR)/a2x.py

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
	IMPDIR = --attribute importdir="$(IMPORTDIR)"
else
	IMPDIR = --attribute importdir="$(BUILDDIR)/docs"
	IMPORTDIR = "$(BUILDDIR)/docs"
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

.PHONY: all dist docbook help clean pdf html offline-html singlehtml text cleanup annotated manpages upgrade installfilter html-check text-check check yearcheck

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

dist: installfilter offline-html html html-check text text-check pdf manpages upgrade cleanup yearcheck

check: html-check text-check cleanup

clean:
	-rm -rf "$(BUILDDIR)/"*

cleanup:
	#
	#
	# Cleaning up.
	#
	#
ifndef KEEP
	rm -f "$(DOCBOOKFILE)"
	rm -f "$(BUILDDIR)/"*.xml
	rm -f "$(ANNOTATEDDIR)/"*.xml
	rm -f "$(FOPDIR)/images"
	rm -f "$(FOPFILE)"
	rm -f "$(UPGRADE)/"*.xml
	rm -f "$(UPGRADE)/"*.html
endif

installfilter:
	#
	#
	# Installing asciidoc filters.
	#
	#
	mkdir -p $(FILTERDEST)
	cp -fr "$(FILTERSRC)/"* $(FILTERDEST)

copyimages:
	#
	#
	# Copying images from source projects.
	#
	#
	"$(SCRIPTDIR)/copy-images.sh" "$(IMPORTDIR)" "$(IMGDIR)"

html-check: offline-html
	#
	#
	# Checking that identifiers exist where they should.
	#
	"$(SCRIPTDIR)/htmlcheck.sh" "$(CHUNKEDOFFLINEHTMLDIR)"

text-check: text
	#
	#
	# Checking that snippets are in place.
	#
	"$(SCRIPTDIR)/textcheck.sh" "$(TEXTFILE)"

docbook-shortinfo:  manpages copyimages
	#
	#
	# Building docbook output with short info.
	# Checking DocBook validity.
	#
	#
	mkdir -p "$(BUILDDIR)"
	"$(ASCIIDOC)" $(ASCIIDOC_FLAGS) --backend docbook --attribute docinfo1 --attribute nonhtmloutput=1 --doctype book --conf-file="$(CONFDIR)/asciidoc.conf" --conf-file="$(CONFDIR)/docbook45.conf" --out-file "$(DOCBOOKFILE)" "$(SRCFILE)"
	xmllint --nonet --noout --xinclude --postvalid "$(DOCBOOKFILE)"

docbook-html:  manpages copyimages
	#
	#
	# Building docbook output with short info for html outputs.
	# Checking for missing include files.
	# Checking DocBook validity.
	#
	#
	mkdir -p "$(BUILDDIR)"
	"$(ASCIIDOC)" $(ASCIIDOC_FLAGS) --backend docbook --attribute docinfo1 --attribute console=1 --doctype book --conf-file="$(CONFDIR)/asciidoc.conf" --conf-file="$(CONFDIR)/docbook45.conf" --conf-file="$(CONFDIR)/linkedimages.conf" --out-file "$(DOCBOOKFILEHTML)" "$(SRCFILE)" 2>&1 | "$(SCRIPTDIR)/outputcheck-includefiles.sh"
	xmllint --nonet --noout --xinclude --postvalid "$(DOCBOOKFILEHTML)"

pdf: docbook-shortinfo copyimages
	#
	#
	# Building PDF.
	#
	#
	mkdir -p "$(FOPDIR)"
	cd "$(FOPDIR)"
	xsltproc --xinclude --output "$(FOPFILE)" "$(CONFDIR)/fo.xsl" "$(DOCBOOKFILE)"
	ln -s "$(SRCDIR)/images" "$(FOPDIR)/images"
	#export FOP_OPTS="-Xmx2048m"
	#fop -fo $(FOPFILE) -pdf $(FOPPDF) -c $(CONFDIR)/fop.xml
	# For fop 1.0, timezone has to be a non-negative one.
	MAVEN_OPTS="-Xmx2048m -Duser.timezone=GMT" mvn -f="fop-pom.xml" -e exec:java -Dexec.mainClass="org.apache.fop.cli.Main" -Djava.awt.headless=true -Dexec.args="-fo '$(FOPFILE)' -pdf '$(FOPPDF)' -c '$(CONFDIR)/fop.xml'" 2>&1 | "$(SCRIPTDIR)/outputcheck-images-fop.sh"

html: manpages copyimages docbook-html
	#
	#
	# Building html output.
	# Checking for missing images/resources.
	#
	#
	"$(A2X)" $(V) -L -f chunked -D "$(BUILDDIR)" --xsl-file="$(CONFDIR)/chunked.xsl" -r "$(IMGDIR)" -r "$(CSSDIR)" --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" --xsltproc-opts "--stringparam toc.section.depth 1" "$(DOCBOOKFILEHTML)" 2>&1 | "$(SCRIPTDIR)/outputcheck-images.sh"
	rm -rf "$(CHUNKEDHTMLDIR)"
	mv "$(CHUNKEDSHORTINFOTARGET)" "$(CHUNKEDHTMLDIR)"
	cp -fr "$(JSDIR)" "$(CHUNKEDHTMLDIR)/js"
	cp -fr "$(CSSDIR)/"* "$(CHUNKEDHTMLDIR)/css"
	cp -fr "$(SRCDIR)/images/"*.png "$(CHUNKEDHTMLDIR)/images"

offline-html:  manpages copyimages docbook-html
	#
	#
	# Building html output for offline use.
	#
	#
	"$(A2X)" $(V) -L -f chunked -D "$(BUILDDIR)" --xsl-file="$(CONFDIR)/chunked-offline.xsl" -r "$(IMGDIR)" -r "$(CSSDIR)" --xsltproc-opts "--stringparam admon.graphics 1" --xsltproc-opts "--xinclude" --xsltproc-opts "--stringparam chunk.section.depth 1" --xsltproc-opts "--stringparam toc.section.depth 1" "$(DOCBOOKFILEHTML)"
	rm -rf "$(CHUNKEDOFFLINEHTMLDIR)"
	mv "$(CHUNKEDSHORTINFOTARGET)" "$(CHUNKEDOFFLINEHTMLDIR)"
	cp -fr "$(JSDIR)" "$(CHUNKEDOFFLINEHTMLDIR)/js"
	cp -fr "$(CSSDIR)/"* "$(CHUNKEDOFFLINEHTMLDIR)/css/"
	cp -fr "$(SRCDIR)/images/"*.png "$(CHUNKEDOFFLINEHTMLDIR)/images"

# currently builds docbook format first
singlehtml:  dist
	#
	#
	# Building single html file output.
	#
	#
	mkdir -p "$(SINGLEHTMLDIR)"
	"$(A2X)" $(A2X_FLAGS) -L -f xhtml -D "$(SINGLEHTMLDIR)" --conf-file="$(CONFDIR)/xhtml.conf" --asciidoc-opts "--conf-file=\"$(CONFDIR)/asciidoc.conf\"" --asciidoc-opts "--conf-file=\"$(CONFDIR)/docbook45.conf\"" --asciidoc-opts "--conf-file=\"$(CONFDIR)/linkedimages.conf\"" --xsl-file="$(CONFDIR)/xhtml.xsl" --xsltproc-opts "--stringparam admon.graphics 1" "$(SRCFILE)"
	cp -fr "$(JSDIR)" "$(SINGLEHTMLDIR)/js"
	cp -fr "$(CSSDIR)" "$(SINGLEHTMLDIR)/css"
	cp -fr "$(IMGDIR)" "$(SINGLEHTMLDIR)/images"
	mv "$(SINGLEHTMLDIR)/$(PROJECTNAME).html" "$(SINGLEHTMLDIR)/index.html"


# builds docbook format first
annotated:  dist
	#
	#
	# Building annotated html output.
	#
	#
	mkdir -p "$(ANNOTATEDDIR)"
	"$(A2X)" $(A2X_FLAGS) -L -a showcomments -f xhtml -D "$(ANNOTATEDDIR)" --conf-file="$(CONFDIR)/xhtml.conf" --asciidoc-opts "--conf-file=\"$(CONFDIR)/asciidoc.conf\"" --asciidoc-opts "--conf-file=\"$(CONFDIR)/docbook45.conf\"" --asciidoc-opts "--conf-file=\"$(CONFDIR)/linkedimages.conf\"" --xsl-file="$(CONFDIR)/xhtml.xsl" --xsltproc-opts "--stringparam admon.graphics 1" "$(SRCFILE)"
	cp -fr "$(SRCDIR)/js" "$(ANNOTATEDDIR)/js"
	cp -fr "$(SRCDIR)/css/"* "$(ANNOTATEDDIR)/css"
	cp -fr "$(SRCDIR)/images/"*.png "$(ANNOTATEDDIR)/images"
	mv "$(ANNOTATEDDIR)/$(PROJECTNAME).html" "$(ANNOTATEDDIR)/index.html"

text: docbook-shortinfo
	#
	#
	# Building text output.
	#
	#
	mkdir -p "$(TEXTDIR)"
	cd "$(TEXTDIR)"
	xsltproc --xinclude --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1 --stringparam admon.graphics 0  --output "$(TEXTHTMLFILE)" "$(CONFDIR)/text.xsl" "$(DOCBOOKFILE)"
	sed 's/ //g' <"$(TEXTHTMLFILE)" >"$(TEXTHTMLFILE).filtered"
	rm -f "$(TEXTHTMLFILE)"
	mv "$(TEXTHTMLFILE).filtered" "$(TEXTHTMLFILE)"
	cd "$(SRCDIR)"
	cp -f "$(SCRIPTDIR)/bom" "$(TEXTFILE)"
	w3m -cols "$(TEXTWIDTH)" -dump -T text/html -no-graph "$(TEXTHTMLFILE)" >> "$(TEXTFILE)"
ifndef KEEP
	rm -f "$(TEXTHTMLFILE)"
	rm -f "$(TEXTDIR)/"*.html
	rm -f "$(CURDIR)/"*.html
endif

manpages:
	#
	#
	# Building manpages.
	#
	#
	mkdir -p "$(MANPAGES)"
	"$(SCRIPTDIR)/manpage.sh" "$(V)" "$(MANPAGES)" "$(IMPORTDIR)" "$(A2X)" "$(SCRIPTDIR)" "neo4j server" "neo4j-shell shell" "neo4j-backup backup"
	# clean up
	mkdir -p "$(ANNOTATEDDIR)"
	cp "$(MANPAGES)/"*.xml "$(ANNOTATEDDIR)"
	mv "$(MANPAGES)/"*.xml "$(BUILDDIR)"
	rm -rf "$(MANPAGES)/"*.html

upgrade:
	#
	#
	# Building upgrade text.
	#
	#
	mkdir -p "$(UPGRADE)"
	"$(ASCIIDOC)" --backend docbook -a a2x-format=text --out-file "$(UPGRADE)/upgrades.xml" "$(BUILDDIR)/docs/neo4j-docs-jar/ops/upgrades.asciidoc"
	# swap out arrow glyph for plain -->
	sed 's/&#8594;/--\&gt;/g' <"$(UPGRADE)/upgrades.xml" >"$(UPGRADE)/upgrades.xml.safe"
	rm -f "$(UPGRADE)/upgrades.xml"
	mv "$(UPGRADE)/upgrades.xml.safe" "$(UPGRADE)/upgrades.xml"
	xmllint --nonet --noout --valid "$(UPGRADE)/upgrades.xml"
	cd "$(UPGRADE)"
	xsltproc  --stringparam callout.graphics 0 --stringparam navig.graphics 0 --stringparam admon.textlabel 1 --stringparam admon.graphics 0  --output "$(UPGRADE)/upgrades.text.html" "$(ASCIDOCDIR)/docbook-xsl/text.xsl" "$(UPGRADE)/upgrades.xml"
	cd "$(CURDIR)"
	w3m -cols 70 -dump -T text/html -no-graph "$(UPGRADE)/upgrades.text.html" > "$(UPGRADE)/upgrades.text"
	cp -f "$(SCRIPTDIR)/bom" "$(UPGRADE)/UPGRADE.txt"
	cat "$(UPGRADE)/upgrades.text" >> "$(UPGRADE)/UPGRADE.txt"
	rm "$(UPGRADE)/upgrades.text"

slidestest:
	#
	#
	# Building slides.
	#
	#
	"$(ASCIIDOC)" $(ASCIIDOC_FLAGS) --backend docbook --doctype article --conf-file="$(CONFDIR)/asciidoc.conf" --conf-file="$(CONFDIR)/docbook45.conf" --conf-file="$(CONFDIR)/docbook45-slides.conf" --out-file ./target/slidestest/article-slides.xml ./target/docs/neo4j-examples-docs-jar/dev/examples/hello-world.txt
	xsltproc --xinclude --output ./target/slidestest/slides /usr/share/xml/docbook/stylesheet/docbook-xsl/slides/xhtml/default.xsl ./target/slidestest/article-slides.xml
	xmllint --nonet --noout --xinclude --postvalid ./target/slidestest/article-slides.xml

yearcheck:
	#
	#
	# Check that the manual has the correct year set.
	#
	#
	"$(SCRIPTDIR)/yearcheck.sh" "$(SRCDIR)"

