<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.footer.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[
<div id="neo-disqus-wrapper">
</div>
<script type="text/javascript" src="../disqus.js"></script>
<script type="text/javascript">

  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
    (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  //Allow Linker 
  ga('create', 'UA-1192232-34','auto', {'allowLinker': true});
  ga('send', 'pageview');

  // Load the plugin.
  ga('require', 'linker');

  // Define which domains to autoLink.
  ga('linker:autoLink', ['neo4j.org','neo4j.com','neotechnology.com','graphdatabases.com','graphconnect.com']);
</script>
]]>
</xsl:text>
  <HR/>
  <a>
    <xsl:attribute name="href">
      <xsl:apply-templates select="/book/bookinfo/legalnotice[1]" mode="chunk-filename"/>
    </xsl:attribute>

    <xsl:apply-templates select="/book/bookinfo/copyright[1]" mode="titlepage.mode"/>
  </a>
</xsl:template>

</xsl:stylesheet>

