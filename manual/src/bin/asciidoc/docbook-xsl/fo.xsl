<!--
  Generates single FO document from DocBook XML source using DocBook XSL
  stylesheets.

  See xsl-stylesheets/fo/param.xsl for all parameters.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format">
<xsl:import href="http://docbook.sourceforge.net/release/xsl/current/fo/docbook.xsl"/>
<xsl:import href="common.xsl"/>

<xsl:param name="fop1.extensions" select="1" />
<xsl:param name="variablelist.as.blocks" select="1" />

<xsl:param name="paper.type" select="'A4'"/>
<!--
<xsl:param name="paper.type" select="'USletter'"/>
-->
<xsl:param name="hyphenate">false</xsl:param>
<!-- justify, left or right -->
<xsl:param name="alignment">left</xsl:param>

<xsl:param name="body.font.family" select="'serif'"/>
<xsl:param name="body.font.master">12</xsl:param>
<xsl:param name="body.font.size">
 <xsl:value-of select="$body.font.master"/><xsl:text>pt</xsl:text>
</xsl:param>

<xsl:param name="body.margin.bottom" select="'0.5in'"/>
<xsl:param name="body.margin.top" select="'0.5in'"/>
<xsl:param name="bridgehead.in.toc" select="0"/>

<!-- overide setting in common.xsl -->
<xsl:param name="table.frame.border.thickness" select="'2px'"/>

<!-- Default fetches image from Internet (long timeouts) -->
<xsl:param name="draft.watermark.image" select="''"/>

<!-- Line break -->
<xsl:template match="processing-instruction('asciidoc-br')">
  <fo:block/>
</xsl:template>

<!-- Horizontal ruler -->
<xsl:template match="processing-instruction('asciidoc-hr')">
  <fo:block space-after="1em">
    <fo:leader leader-pattern="rule" rule-thickness="0.5pt"  rule-style="solid" leader-length.minimum="100%"/>
  </fo:block>
</xsl:template>

<!-- Hard page break -->
<xsl:template match="processing-instruction('asciidoc-pagebreak')">
   <fo:block break-after='page'/>
</xsl:template>

<!-- Sets title to body text indent -->
<xsl:param name="body.start.indent">
  <xsl:choose>
    <xsl:when test="$fop.extensions != 0">0pt</xsl:when>
    <xsl:when test="$passivetex.extensions != 0">0pt</xsl:when>
    <xsl:otherwise>1pc</xsl:otherwise>
  </xsl:choose>
</xsl:param>
<xsl:param name="title.margin.left">
  <xsl:choose>
    <xsl:when test="$fop.extensions != 0">-1pc</xsl:when>
    <xsl:when test="$passivetex.extensions != 0">0pt</xsl:when>
    <xsl:otherwise>0pt</xsl:otherwise>
  </xsl:choose>
</xsl:param>
<xsl:param name="page.margin.bottom" select="'0.25in'"/>
<xsl:param name="page.margin.inner">
  <xsl:choose>
    <xsl:when test="$double.sided != 0">0.75in</xsl:when>
    <xsl:otherwise>0.75in</xsl:otherwise>
  </xsl:choose>
</xsl:param>
<xsl:param name="page.margin.outer">
  <xsl:choose>
    <xsl:when test="$double.sided != 0">0.5in</xsl:when>
    <xsl:otherwise>0.5in</xsl:otherwise>
  </xsl:choose>
</xsl:param>

<xsl:param name="page.margin.top" select="'0.5in'"/>
<xsl:param name="page.orientation" select="'portrait'"/>
<xsl:param name="page.width">
  <xsl:choose>
    <xsl:when test="$page.orientation = 'portrait'">
      <xsl:value-of select="$page.width.portrait"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$page.height.portrait"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:param>

<xsl:attribute-set name="monospace.properties">
  <xsl:attribute name="font-size">10pt</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="admonition.title.properties">
  <xsl:attribute name="font-size">14pt</xsl:attribute>
  <xsl:attribute name="font-weight">bold</xsl:attribute>
  <xsl:attribute name="hyphenate">false</xsl:attribute>
  <xsl:attribute name="keep-with-next.within-column">always</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="sidebar.properties" use-attribute-sets="formal.object.properties">
  <xsl:attribute name="border-style">solid</xsl:attribute>
  <xsl:attribute name="border-width">1pt</xsl:attribute>
  <xsl:attribute name="border-color">silver</xsl:attribute>
  <xsl:attribute name="background-color">#ffffee</xsl:attribute>
  <xsl:attribute name="padding-left">12pt</xsl:attribute>
  <xsl:attribute name="padding-right">12pt</xsl:attribute>
  <xsl:attribute name="padding-top">6pt</xsl:attribute>
  <xsl:attribute name="padding-bottom">6pt</xsl:attribute>
  <xsl:attribute name="margin-left">0pt</xsl:attribute>
  <xsl:attribute name="margin-right">12pt</xsl:attribute>
  <xsl:attribute name="margin-top">6pt</xsl:attribute>
  <xsl:attribute name="margin-bottom">6pt</xsl:attribute>
</xsl:attribute-set>

<xsl:param name="callout.graphics" select="'1'"/>

<!-- Only shade programlisting and screen verbatim elements -->
<xsl:param name="shade.verbatim" select="1"/>
<xsl:attribute-set name="shade.verbatim.style">
  <xsl:attribute name="background-color">
    <xsl:choose>
      <xsl:when test="self::programlisting|self::screen">#E0E0E0</xsl:when>
      <xsl:otherwise>inherit</xsl:otherwise>
    </xsl:choose>
  </xsl:attribute>
</xsl:attribute-set>

<!--
  Force XSL Stylesheets 1.72 default table breaks to be the same as the current
  version (1.74) default which (for tables) is keep-together="auto".
-->
<xsl:attribute-set name="table.properties">
  <xsl:attribute name="keep-together.within-column">auto</xsl:attribute>
</xsl:attribute-set>

</xsl:stylesheet>
