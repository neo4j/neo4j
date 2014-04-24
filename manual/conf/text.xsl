<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
  xmlns:d="http://docbook.org/ns/docbook" exclude-result-prefixes="d">

  <xsl:import href="urn:docbkx:stylesheet" />

  <!-- parameters for optimal text output -->
  <xsl:param name="callout.graphics" select="0"/>
  <xsl:param name="callout.unicode" select="0"/>
  <xsl:param name="section.autolabel" select="1"/>
  <xsl:param name="section.label.includes.component.label" select="1"/>
  <xsl:param name="generate.toc">
  article   toc,title
  </xsl:param>

  <xsl:template match="d:article/d:articleinfo/d:title" mode="titlepage.mode">
      <hr />
        <xsl:apply-imports/>
      <hr />
  </xsl:template>

  <xsl:template match="d:article/*/d:title" mode="titlepage.mode">
      <hr />
        <xsl:apply-imports/>
      <hr />
  </xsl:template>

  <xsl:template match="d:article/d:section/*/d:title" mode="titlepage.mode">
        <xsl:apply-imports/>
      <hr width="100" align="left" />
  </xsl:template>
  
  <xsl:template match="d:varlistentry">
    <dt>
      <xsl:call-template name="id.attribute"/>
      <xsl:call-template name="anchor"/>
      <xsl:apply-templates select="d:term"/>
    </dt>
    <dd>
      <xsl:apply-templates select="d:listitem"/>
    </dd>
    <br />
  </xsl:template>

</xsl:stylesheet>
