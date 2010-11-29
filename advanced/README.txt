Neo4j Manual

Regarding the included build.sh script:
* The dblatex toolchain for creating a PDF works with
  asciidoc version 8.5.2.
* The FOP toolchain for creating a PDF need version
  8.6.3 of asciidoc due to the options used.
* Singe page HTML output works with 8.5.2, while
  chunked HTML and text does not.
