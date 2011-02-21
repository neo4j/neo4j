About Neo4j Manual
==================

The documents use the asciidoc format, see:
http://www.methods.co.nz/asciidoc/
http://powerman.name/doc/asciidoc

== Building the documentation ==

Asciidoc version 8.6.3 is a requirement.
(and whatever dependencies it needs)

building the full docs distribution:
$make dist

building single page html output:
$make singlehtml

for other build options, see
$make help

== Documents and directory structure ==

Documents go into one directory per chapter.
Multiple documents constituting a chapter 
should be included through an index.txt file.

The root directory is reserved for top-level
concerns (the main document, glossary etc).

Documents not referenced (included) from another
document is not part of the build. 

== Headings and document structure ==

Each document starts over with headings from level zero (the document title).
To push the headings down to the right level in the output, the leveloffset 
attribute is used when including the document in another document.

A chapter can't be empty. (the build will fail)

== Writing ==

Put one sentence on each line. This makes it easy to move around content,
and also easy to spot (too) long sentences.

== Comments ==

We'll have a separate build including comments.
// this is such a comment
The comments are not visible in the normal build.
Comment blocks won't be included in any build at all.

== Attributes ==

Common attributes you can use in documents:
{neo4j-version}

== Toolchain ==

Useful links when configuring the docbook toolchain:

* http://www.docbook.org/tdg/en/html/docbook.html
* http://www.sagehill.net/docbookxsl/index.html

