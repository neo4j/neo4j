<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
  xmlns:d="http://docbook.org/ns/docbook" xmlns="http://www.w3.org/1999/xhtml" exclude-result-prefixes="d">

  <xsl:template match="d:programlisting">
    <xsl:param name="suppress-numbers" select="'0'" />

    <xsl:call-template name="anchor" />

    <xsl:variable name="div.element">pre</xsl:variable>

    <xsl:element name="{$div.element}">
      <xsl:apply-templates select="." mode="common.html.attributes" />
      <xsl:call-template name="id.attribute" />
      <xsl:if test="@language != ''">
        <xsl:attribute name="data-lang">
            <xsl:value-of select="@language" />
        </xsl:attribute>
      </xsl:if>
      <xsl:apply-templates />
    </xsl:element>
  </xsl:template>

</xsl:stylesheet>
