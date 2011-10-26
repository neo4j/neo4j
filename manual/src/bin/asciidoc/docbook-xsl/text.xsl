<?xml version="1.0"?>
<!--
  Used by AsciiDoc a2x(1) for w3m(1) based text generation.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version="1.0">
  <xsl:import
    href="http://docbook.sourceforge.net/release/xsl/current/xhtml/docbook.xsl"/>

  <!-- parameters for optimal text output -->
  <xsl:param name="callout.graphics" select="0"/>
  <xsl:param name="callout.unicode" select="0"/>
  <xsl:param name="section.autolabel" select="1"/>
  <xsl:param name="section.label.includes.component.label" select="1"/>
  <xsl:param name="generate.toc">
  appendix  title
  article/appendix  nop
  article   toc,title
  book      toc,title,figure,table,example,equation
  chapter   title
  part      toc,title
  preface   toc,title
  qandadiv  toc
  qandaset  toc
  reference toc,title
  section   toc
  set       toc,title
  </xsl:param>

  <xsl:template match="book/bookinfo/title | article/articleinfo/title" mode="titlepage.mode">
      <hr />
        <xsl:apply-imports/>
      <hr />
  </xsl:template>

  <xsl:template match="book/*/title | article/*/title" mode="titlepage.mode">
      <br /><hr />
        <xsl:apply-imports/>
      <hr />
  </xsl:template>

  <xsl:template match="book/chapter/*/title | article/section/*/title" mode="titlepage.mode">
      <br />
        <xsl:apply-imports/>
      <hr width="100" align="left" />
  </xsl:template>


</xsl:stylesheet>
