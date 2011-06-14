<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

<xsl:template name="user.head.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[

<!-- favicon -->

<link rel="shortcut icon" href="http://neo4j.org/favicon.ico" type="image/vnd.microsoft.icon" />
<link rel="icon" href="http://neo4j.org/favicon.ico" type="image/x-icon" />

<!-- style -->

<link href="css/shCore.css" rel="stylesheet" type="text/css" />
<link href="css/shCoreEclipse.css" rel="stylesheet" type="text/css" />
<link href="css/shThemeEclipse.css" rel="stylesheet" type="text/css" />

<style type="text/css">
span.remark {
  background: yellow;
}
td p {
  margin: 0.125em 0;
}
#disqus_thread {
  max-width: 40em;
}
body #toolbar_item command_help {
  /* display: none !important; */
}
.syntaxhighlighter a,
.syntaxhighlighter div,
.syntaxhighlighter code,
.syntaxhighlighter table,
.syntaxhighlighter table td,
.syntaxhighlighter table tr,
.syntaxhighlighter table tbody,
.syntaxhighlighter table thead,
.syntaxhighlighter table caption,
.syntaxhighlighter textarea {
  font-size: 14px !important;
}
</style>

<!-- Syntax Highlighter -->

<script type="text/javascript" src="js/shCore.js"></script>
<script type="text/javascript" src="js/shBrushJava.js"></script>
<script type="text/javascript" src="js/shBrushJScript.js"></script>
<script type="text/javascript" src="js/shBrushBash.js"></script>
<script type="text/javascript" src="js/shBrushPlain.js"></script>
<script type="text/javascript" src="js/shBrushXml.js"></script>

<!-- activate when needed
<script type="text/javascript" src="js/shBrushPython.js"></script>
<script type="text/javascript" src="js/shBrushRuby.js"></script>
<script type="text/javascript" src="js/shBrushCSharp.js"></script>
<script type="text/javascript" src="js/shBrushGroovy.js"></script>
-->
 
<script type="text/javascript">
  SyntaxHighlighter.defaults['tab-size'] = 4;
  SyntaxHighlighter.defaults['gutter'] = false;
  SyntaxHighlighter.defaults['toolbar'] = false;
  SyntaxHighlighter.all()
</script>

<!-- JQuery -->

<script type="text/javascript" src="js/jquery-1.6.1.min.js"></script>

<!-- Smart Image Scaling -->
<!-- Makes sure images are never scaled to more than 100% -->
<!-- while preserving image scaling as much as possible. -->

<script type="text/javascript">

jQuery( window ).load(  function()
{
  setImageSizes( jQuery );
});

function setImageSizes( $ )
{
  $( "span.inlinemediaobject > img[width]" ).each( function()
  {
    var img = this;
    var width = $( this ).parent().width();
    if ( img.naturalWidth && width > img.naturalWidth )
    {
      removeWidth( img );
    }
    else if ( img.realWidth && width > img.realWidth )
    {
      removeWidth( img );
    }
    else
    {
      $("<img />")
        .attr( "src", img.getAttribute( "src" ) )
        .load( function( )
        {
          img.realWidth = this.width;
          if ( width > this.width )
          {
            removeWidth( img );
          }
        });
    }
  });
}

function resetImageSizes( $ )
{
  $( "span.inlinemediaobject > img" ).each( function()
  {
    if ( this.originalWidth )
    {
      this.setAttribute( "width", this.originalWidth );
    }
  });
  setImageSizes( $ );
}

jQuery( window ).resize( function()
{
  resetImageSizes( jQuery );
});

function removeWidth( image )
{
  if ( ! image.originalWidth )
  {
    image.originalWidth = image.getAttribute( "width" );
  }
  image.removeAttribute( "width" );
}

</script>

]]>
</xsl:text>
</xsl:template>

</xsl:stylesheet>

