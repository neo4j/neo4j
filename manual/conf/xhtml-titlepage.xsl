<?xml version="1.0"?>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:d="http://docbook.org/ns/docbook"
xmlns:exsl="http://exslt.org/common" version="1.0" exclude-result-prefixes="exsl d">


<xsl:template name="book.titlepage.recto">
  <xsl:choose>
    <xsl:when test="d:bookinfo/d:title">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:title"/>
    </xsl:when>
    <xsl:when test="d:info/d:title">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:title"/>
    </xsl:when>
    <xsl:when test="d:title">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:title"/>
    </xsl:when>
  </xsl:choose>

  <xsl:choose>
    <xsl:when test="d:bookinfo/d:subtitle">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:subtitle"/>
    </xsl:when>
    <xsl:when test="d:info/d:subtitle">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:subtitle"/>
    </xsl:when>
    <xsl:when test="d:subtitle">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:subtitle"/>
    </xsl:when>
  </xsl:choose>

  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:corpauthor"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:corpauthor"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:authorgroup"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:authorgroup"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:author"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:author"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:othercredit"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:othercredit"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:releaseinfo"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:releaseinfo"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:copyright"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:copyright"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:legalnotice"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:legalnotice"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:pubdate"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:pubdate"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:revision"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:revision"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:revhistory"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:revhistory"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:abstract"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:abstract"/>
</xsl:template>

</xsl:stylesheet>