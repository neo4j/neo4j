<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.footer.content">
  <HR/>
  <a>
    <xsl:attribute name="href">
      <xsl:apply-templates select="/book/bookinfo/legalnotice[1]" mode="chunk-filename"/>
    </xsl:attribute>

    <xsl:apply-templates select="/book/bookinfo/copyright[1]" mode="titlepage.mode"/>
  </a>
</xsl:template>

</xsl:stylesheet>

