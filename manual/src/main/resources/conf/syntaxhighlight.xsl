<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template match="*/programlisting" mode="class.value">
  <xsl:if test="@language">
    <xsl:value-of select="concat('programlisting brush: ', @language)"/>
  </xsl:if>
</xsl:template>

</xsl:stylesheet>

