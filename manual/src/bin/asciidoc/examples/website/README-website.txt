AsciiDoc Website
================

The AsciiDoc website source is included in the AsciiDoc distribution
(in `./examples/website/`) as an example of using AsciiDoc to build a
website.

A simple shell script (`./examples/website/build-website.sh`) will
build the site's web pages -- just set the `LAYOUT` variable to the
desired layout.

Website Layouts
---------------
The website layout is determined by the layout configuration file
(`layout1.conf` or `layout2.conf`) and corresponding layout CSS file
(`layout1.css` or `layout2.css`). The example website comes with the
following layouts:

[width="80%",cols="1,4",options="header"]
|====================================================================
|Layout   |  Description
|layout1  |  Table based layout
|layout2  |  CSS based layout (this is the default layout)
|====================================================================

The default tables based layout (layout1) works with most modern
browsers.

NOTE: The simulated frames layout (layout2) does not work with IE6.
