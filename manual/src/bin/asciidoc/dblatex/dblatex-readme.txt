AsciiDoc dblatex README
=======================

Customization
-------------
The `./dblatex` directory contains:

`./dblatex/asciidoc-dblatex.xsl`:: Optional dblatex XSL parameter
customization.

`./dblatex/asciidoc-dblatex.sty`:: Optional customized LaTeX styles.

Use these files with dblatex(1) `-p` and `-s` options, for example:

  dblatex -p ../dblatex/asciidoc-dblatex.xsl \
          -s ../dblatex/asciidoc-dblatex.sty article.xml


Limitations
-----------
Observed in dblatex 0.2.8.

- dblatex doesn't seem to process the DocBook 'literallayout' element
  correctly: it is rendered in a monospaced font and no inline
  elements are processed. By default the normal font should be used
  and almost all DocBook inline elements should be processed
  (http://www.docbook.org/tdg/en/html/literallayout.html).  I almost
  fixed this by overriding the default dblatex literallayout template
  (in `./dblatex/asciidoc-dblatex.xsl`) and using the LaTeX 'alltt'
  package, but there are remaining problems:

  * Blank lines are replaced by a single space.
  * The 'literallayout' element incorrectly wraps text when rendered
    inside a table.

- Callouts do not work inside DocBook 'literallayout' elements which
  means callouts are not displayed inside AsciiDoc literal blocks.  A
  workaround is to change the AsciiDoc literal block to a listing
  block.
