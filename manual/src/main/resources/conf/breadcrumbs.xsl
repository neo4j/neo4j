<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  
  <xsl:template name="breadcrumbs">
    
    <xsl:param name="this.node" select="."/>
    
    <xsl:if test="name(.) != 'book'">
      <div class="breadcrumbs">
          <span class="breadcrumb-link">
            <a href="index.html">The Neo4j Manual</a>
          </span>
          <xsl:text> &gt; </xsl:text>
        <xsl:for-each select="$this.node/ancestor::*[parent::*]">
          <span class="breadcrumb-link">
            <a>
              <xsl:attribute name="href">
                <xsl:call-template name="href.target">
                  <xsl:with-param name="object" select="."/>
                  <xsl:with-param name="context" select="$this.node"/>
                </xsl:call-template>
              </xsl:attribute>
              <xsl:apply-templates select="." mode="title.markup"/>
            </a>
          </span>
          <xsl:text> &gt; </xsl:text>
        </xsl:for-each>
        <!-- And display the current node, but not as a link -->
        <span class="breadcrumb-node">
          <xsl:apply-templates select="$this.node" mode="title.markup"/>
        </span>
      </div>
    </xsl:if> 
  </xsl:template>

</xsl:stylesheet>

