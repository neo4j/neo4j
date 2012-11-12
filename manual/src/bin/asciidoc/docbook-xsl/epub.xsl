<!--
  Generates EPUB XHTML documents from DocBook XML source using DocBook XSL
  stylesheets.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:import href="http://docbook.sourceforge.net/release/xsl/current/epub/docbook.xsl"/>
<xsl:import href="common.xsl"/>

<!--
DocBook XSL 1.75.2: Nav headers are invalid XHTML (table width element).
Suppressed by default in Docbook XSL 1.76.1 epub.xsl.
-->
<xsl:param name="suppress.navigation" select="1"/>

<!--
DocBook XLS 1.75.2 doesn't handle TOCs
-->
<xsl:param name="generate.toc">
  <xsl:choose>
    <xsl:when test="/article">
/article  nop
    </xsl:when>
    <xsl:when test="/book">
/book  nop
    </xsl:when>
  </xsl:choose>
</xsl:param>

</xsl:stylesheet>
