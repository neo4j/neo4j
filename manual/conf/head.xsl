<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0" exclude-result-prefixes="d">

<xsl:template name="user.webhelp.head.content">
<xsl:text disable-output-escaping="yes">
<![CDATA[

<!-- favicon -->

<link rel="shortcut icon" href="common/images/favicon.ico" />

<!-- fonts -->

<link href='http://fonts.googleapis.com/css?family=Ubuntu+Mono|PT+Sans:400,700,400italic' rel='stylesheet' type='text/css' />

<!-- Syntax Highlighter -->

<link rel="stylesheet" href="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/codemirror.min.css" />
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/codemirror.min.js"></script>
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/addon/runmode/runmode.min.js"></script>
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/mode/javascript/javascript.min.js"></script>
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/mode/shell/shell.min.js"></script>
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/mode/sql/sql.min.js"></script>
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/mode/xml/xml.min.js"></script>
<script src="http://cdnjs.cloudflare.com/ajax/libs/codemirror/4.0.3/mode/clike/clike.min.js"></script>

<link rel="stylesheet" href="http://gist.neo4j.org/css/codemirror-neo.css" />
<script src="http://gist.neo4j.org/js/codemirror-cypher.js"></script>
<script src="js/colorize.js"></script>
 
<script type="text/javascript">
  $(function (){
    CodeMirror.colorize();
  });
</script>

<!-- Cypher Console -->

<script type="text/javascript" src="js/console.js"></script>
<script type="text/javascript" src="js/cypherconsole.js"></script>

<!-- Version -->
<script type="text/javascript" src="js/version.js"></script>
<script type="text/javascript" src="http://docs.neo4j.org/chunked/versions.js"></script>
<script type="text/javascript" src="js/versionswitcher.js"></script>

<!-- Discuss -->
<script type="text/javascript" src="js/mutate.min.js"></script>
<script type="text/javascript" src="js/disqus.js"></script>

<script type="text/javascript">
    /*@cc_on @*/
    /*@
     $( '#content' ).addClass( 'internet-explorer' );
     @*/
</script>

]]>
</xsl:text>

</xsl:template>

</xsl:stylesheet>

