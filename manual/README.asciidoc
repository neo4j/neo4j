About Neo4j Manual
==================

The documents use the asciidoc format, see:

* http://www.methods.co.nz/asciidoc/
* http://powerman.name/doc/asciidoc

== Building the documentation ==

Asciidoc version 8.6.3 is a requirement
together with whatever dependencies it needs
and specifically docbook, w3m and fop.

Maven is used to unpack the pieces of the manual and
to execute asciidoc on them.

To build the documentation, use: +
`mvn clean install`

== Headings and document structure ==

Each document starts over with headings from level zero (the document title).
To push the headings down to the right level in the output, the leveloffset 
attribute is used when including the document in another document.

== Writing ==

Put one sentence on each line. This makes it easy to move around content,
and also easy to spot (too) long sentences.

== Gotchas ==

* A chapter can't be empty. (the build will fail)
* The document title should be "underlined" by the same
  number "=" as there are characters in the title.
* Always have a blank line at the end of documents
  (or the title of the next document might end up in the last
  paragraph of the document)

== Comments ==

There's a separate build including comments.
// this is such a comment
The comments are not visible in the normal build.
Comment blocks won't be included in any build at all.

== Attributes ==

Common attributes you can use in documents:
{neo4j-version}
{neo4j-git-tag}

NOTE: Do not add other attributes!

== Toolchain ==

Useful links when configuring the docbook toolchain:

* http://www.docbook.org/tdg/en/html/docbook.html
* http://www.sagehill.net/docbookxsl/index.html
* http://docbook.sourceforge.net/release/xsl/1.76.1/doc/html/index.html
* http://docbook.sourceforge.net/release/xsl/1.76.1/doc/fo/index.html

=== With brew on OSX ===

  brew install docbook asciidoc w3m fop

