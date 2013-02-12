var disqus_identifier = "manual";
var disqus_title = "The Neo4j Manual";
function getIdFromHeading(headingElement)
{
  var id = disqus_identifier;
  var child = headingElement.firstChild;
  if ( !child || child.nodeName.toLowerCase() !== "a" )
  {
    return null;
  }
  var attr = child.getAttribute( "id" );
  if ( !attr )
  {
    return null;
  }
  id += "-";
  id += headingElement.firstChild.getAttribute( "id" );
  return id;
}
var headings = document.body.getElementsByTagName( "h1" );
if ( headings.length > 0 )
{
  var h1Id = getIdFromHeading( headings[0] );
  if ( h1Id )
  {
    if ( h1Id.length > 2 && h1Id.substr( 0, 9 ) === "manual-id" )
    {
      disqus_identifier += "-toc";
    }
    else
    {
      disqus_identifier = h1Id;
    }
  }
}
else
{
  headings = document.body.getElementsByTagName( "h2" );
  if ( headings.length > 0 )
  {
    var id = getIdFromHeading( headings[0] );
    if ( id )
    {
      disqus_identifier = id;
    }
    else
    {
      var divs = document.body.getElementsByTagName( "div" );
      for ( var i = 0, l = divs.length; i < l; i += 1 )
      {
        var div = divs[i];
        if ( div.className === "refsynopsisdiv" )
        {
          var divId = getIdFromHeading( div );
          if ( divId )
          {
            disqus_identifier = divId;
          }
          break;
        }
      }
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
if ( disqus_url.protocol === "http:" || disqus_url.protocol === "https:" )
{
  var docsLocation = "http://docs.neo4j.org/chunked/snapshot/";
  var path = disqus_url.pathname;
  var position = path.lastIndexOf( '/' );
  if ( position === -1 )
  {
    position = path.lastIndexOf( '\\' );
  }
  if ( position > 0 )
  {
    var page = path.substring( position + 1 );
    disqus_url = docsLocation + page;
  }
}
var disqus_shortname = "neo4j";
var disqus_developer = 1;

( function()
{
  var dsq = document.createElement( 'script' );
  dsq.type = 'text/javascript';
  dsq.async = true;
  dsq.src = 'http://' + disqus_shortname + '.disqus.com/embed.js';
  ( document.getElementsByTagName( 'head' )[0] || document.getElementsByTagName( 'body' )[0] ).appendChild( dsq );
} )();
