<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.footer.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[
<div id="neo-disqus-wrapper">
  <div id="neo-disqus-intro">
    <h4>Asking questions</h4>
    <p>
      Here's where to ask to get the best answers to your Neo4j questions:
    </p>
    <div class="itemizedlist">
      <ul type="disc" class="itemizedlist">
        <li class="listitem"><em>Having trouble running an example from the manual?</em>
          First make sure that you're using the same version of Neo4j as the manual was built for!
          There's a dropdown on all pages that lets you switch to a different version.</li>
        <li class="listitem"><em>Something doesn't work as expected with Neo4j?</em>
            The stackoverflow.com <a href="http://stackoverflow.com/questions/tagged/neo4j">neo4j tag</a> is an excellent place for this!</li>
        <li class="listitem"><em>Found a bug?</em>
            Then please report and track it using the GitHub <a href="https://github.com/neo4j/neo4j/issues">Neo4j Issues</a> page.
            Note however, that you can report <i>documentation bugs</i> using the Disqus thread below as well.
            </li>
        <li class="listitem"><em>Have a data modeling question or want to participate in discussions around Neo4j and graphs?</em>
            The <a href="https://groups.google.com/forum/?fromgroups#!forum/neo4j">Neo4j Google Group</a> is a great place for this.</li>
        <li class="listitem"><em>Is 140 characters enough for your question?</em>
            Then obviously Twitter is an option.
            There's lots of <a href="https://twitter.com/search?q=neo4j">#neo4j</a> activity there.</li>
        <li class="listitem"><em>Have a question on the content of this page or missing something here?</em>
            Then you're all set, use the discussion thread below.
            Please post any comments or suggestions regarding the documentation right here!</li>
      <ul>
    </div>
  </div>
  <div id="disqus_thread"></div>
</div>
<script type="text/javascript" src="js/disqus.js"></script>
<script type="text/javascript">
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
      <xsl:apply-templates select="/book/bookinfo/legalnotice[1]" mode="chunk-filename"/>
    </xsl:attribute>

    <xsl:apply-templates select="/book/bookinfo/copyright[1]" mode="titlepage.mode"/>
  </a>
</xsl:template>

</xsl:stylesheet>

