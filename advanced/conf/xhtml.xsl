<!--
  Generates single XHTML document from DocBook XML source using DocBook XSL
  stylesheets.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:import href="http://docbook.sourceforge.net/release/xsl/current/xhtml/docbook.xsl"/>
<xsl:import href="common.xsl"/>

<xsl:import href="head.xsl"/>

<xsl:import href="footer.xsl"/>

<xsl:param name="generate.legalnotice.link" select="1"/>
<xsl:param name="legalnotice.filename">legalnotice.html</xsl:param>
<xsl:param name="generate.revhistory.link" select="1"/>

<xsl:template name="section.titlepage.before.recto">
  <xsl:variable name="top-anchor">
    <xsl:call-template name="object.id">
      <xsl:with-param name="object" select="/*[1]"/>
    </xsl:call-template>
  </xsl:variable>

  <p class="returntotop">
    <a href="#{$top-anchor}">
      <xsl:text>Return to top</xsl:text>
    </a>
  </p>
</xsl:template>

</xsl:stylesheet>

