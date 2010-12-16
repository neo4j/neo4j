<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.footer.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[
<div id="disqus_thread"></div>
<script type="text/javascript">
// DISQUS
  var disqus_identifier = "manual";
  var disqus_title = "The Neo4j Manual";
  if ( document.body.getElementsByTagName("h1").length > 0 )
  {
    disqus_identifier += "-toc";
  }
  else
  {
    var headings = document.body.getElementsByTagName("h2");
    if ( headings.length > 0 )
    {
      var headingElement = headings[0];
      if ( headingElement.firstChild )
      {
        disqus_identifier += "-";
        disqus_identifier += headingElement.firstChild.getAttribute("id");
      }
    }
    if ( document.title )
    {
       disqus_title = document.title;
       var match = disqus_title.match( /^(Chapter|)[0-9\.\s]*(.*)$/ );
       if ( match && match[2] )
       {
         disqus_title = match[2];
       }
    }
  }
  var disqus_url = window.location;
  if ( disqus_url.protocol !== "http:" && disqus_url.protocol !== "https:" )
  {
    var docsLocation = "http://docs.neo4j.org/chunked/snapshot/";
    var path = disqus_url.pathname;
    var position = path.lastIndexOf('/');
    if ( position === -1 )
    {
      position = path.lastIndexOf('\\');
    }
    if ( position > 0 )
    {
      var page = path.substring( position + 1 );
      disqus_url = docsLocation + page;
    }
  }
  var disqus_shortname = "neo4j";
  var disqus_developer = 0;

(function() {
  var dsq = document.createElement('script'); dsq.type = 'text/javascript'; dsq.async = true;
  dsq.src = 'http://' + disqus_shortname + '.disqus.com/embed.js';
  (document.getElementsByTagName('head')[0] || document.getElementsByTagName('body')[0]).appendChild(dsq);
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
  <HR/>
  <a>
    <xsl:attribute name="href">
      <xsl:apply-templates select="//legalnotice[1]" mode="chunk-filename"/>
    </xsl:attribute>

    <xsl:apply-templates select="//copyright[1]" mode="titlepage.mode"/>
  </a>
</xsl:template>

</xsl:stylesheet>

