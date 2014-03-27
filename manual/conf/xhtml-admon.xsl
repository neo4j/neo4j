<?xml version="1.0" encoding="ASCII"?>
<xsl:stylesheet xmlns="http://www.w3.org/1999/xhtml" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:d="http://docbook.org/ns/docbook"
 version="1.0" exclude-result-prefixes="xsl d">

<xsl:template name="graphical.admonition">
  <xsl:variable name="admon.type">
    <xsl:choose>
      <xsl:when test="local-name(.)='note'">Note</xsl:when>
      <xsl:when test="local-name(.)='warning'">Warning</xsl:when>
      <xsl:when test="local-name(.)='caution'">Caution</xsl:when>
      <xsl:when test="local-name(.)='tip'">Tip</xsl:when>
      <xsl:when test="local-name(.)='important'">Important</xsl:when>
      <xsl:otherwise>Note</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="alt">
    <xsl:call-template name="gentext">
      <xsl:with-param name="key" select="$admon.type"/>
    </xsl:call-template>
  </xsl:variable>

  <div>
    <xsl:attribute name="class">
      <xsl:value-of select="concat('admonitionblock ', $admon.type)"/>
    </xsl:attribute>

    <table>
      <tr>
        <td class="icon">
          <img alt="[{$alt}]">
            <xsl:attribute name="src">
              <xsl:call-template name="admon.graphic"/>
            </xsl:attribute>
          </img>
        </td>
        <td class="content">
          <xsl:call-template name="anchor"/>
          <xsl:if test="$admon.textlabel != 0 or d:title or d:info/d:title">
            <b>
              <xsl:apply-templates select="." mode="object.title.markup"/>
            </b>
          </xsl:if>
          <xsl:apply-templates/>
        </td>
      </tr>
    </table>
  </div>
</xsl:template>

</xsl:stylesheet>
