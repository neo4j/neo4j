AsciiDoc Code Filter
====================

This simple minded filter highlights source code keywords and
comments.

NOTE: The filter is to demonstrate how to write a filter -- it's much
to simplistic to be passed off as a code syntax highlighter. If you
want a full featured highlighter use the 'source highlighter filter.


Files
-----
code-filter.py::
        The filter Python script.
code-filter.conf::
        The AsciiDoc filter configuration file.
code-filter-test.txt::
        Short AsciiDoc document to test the filter.


Installation
------------
The code filter is installed in the distribution `filters` directory
as part of the standard AsciiDoc install.

Test it on the `code-filter-test.txt` file:

  $ asciidoc -v code-filter-test.txt
  $ firefox code-filter-test.txt &


Help
----
Execute the filter with the help option:

  $ ./code-filter.py --help
