<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.footer.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[
<div id="disqus_thread"></div>
<script type="text/javascript">
// DISQUS
(function() {

  var pageId = "manual";
  var title = "The Neo4j Manual";
  if ( document.body.getElementsByTagName("h1").length > 0 )
  {
    pageId += "-toc";
  }
  else
  {
    var headings = document.body.getElementsByTagName("h2");
    if ( headings.length > 0 )
    {
      var headingElement = headings[0];
      if ( headingElement.firstChild )
      {
        pageId += "-";
        pageId += headingElement.firstChild.getAttribute("id");
      }
    }
    if ( document.title )
    {
       title = document.title;
    }
  }
  // use pageId for disqus here, if it's defined
  var disqus_shortname = "neo4j";
  var disqus_identifier = pageId;
  var disqus_developer = 1;
  var disqus_title = "";

/*
  var dsq = document.createElement('script'); dsq.type = 'text/javascript'; dsq.async = true;
  dsq.src = 'http://' + disqus_shortname + '.disqus.com/embed.js';
  (document.getElementsByTagName('head')[0] || document.getElementsByTagName('body')[0]).appendChild(dsq);
*/
})();

// GA
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

