<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.footer.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[
 <footer>
   <p>
   © Copyright
   <a href="http://creativecommons.org/licenses/by-sa/3.0/"><img id="copyright-img" src="images/by-sa.svg" alt="CC BY-SA 3.0" title="Creative Commons Attribution-ShareAlike 3.0"></a>
   <a href="http://www.neotechnology.com/"><img id="neotech-logo" src="images/neo-technology.svg" alt="Neo Technology"></a>
   </p>
 </footer>
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
</xsl:template>
</xsl:stylesheet>
