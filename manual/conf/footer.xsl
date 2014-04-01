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
  var _gaq = _gaq || [];
  _gaq.push(['_setAccount', 'UA-1192232-16']);
  _gaq.push(['_trackPageview']);

  (function() {
    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
    var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
  })();
 </script>  
]]>
</xsl:text>
</xsl:template>
</xsl:stylesheet>
