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
var disqus_shortname = "neo4j";
var disqus_developer = 0;
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
else
{
  disqus_developer = 1;
}

var intro = $( '<div id="neo-disqus-intro"/>' );
intro.append( '<h4>Asking questions</h4>',
    "<p>Here's where to ask to get the best answers to your Neo4j questions:</p>" );

var listWrapper = $( '<div class="itemizedlist">' );
var list = $( '<ul type="disc" class="itemizedlist" />' );
listWrapper.append( list );
intro.append( listWrapper );

function appendListItem(list, heading, content)
{
  list.append( "<li class='listitem'><em>" + heading + "</em> " + content + "</li>" );
}

appendListItem( list, "Having trouble running an example from the manual?",
    "First make sure that you're using the same version of Neo4j as the manual was built for! "
        + "There's a dropdown on all pages that lets you switch to a different version." );
appendListItem(
    list,
    "Something doesn't work as expected with Neo4j?",
    "The stackoverflow.com <a href='http://stackoverflow.com/questions/tagged/neo4j'>neo4j tag</a> is an excellent place for this!" );
appendListItem(
    list,
    "Found a bug?",
    "Then please report and track it using the GitHub <a href='https://github.com/neo4j/neo4j/issues'>Neo4j Issues</a> page. "
        + "Note however, that you can report <i>documentation bugs</i> using the Disqus thread below as well." );
appendListItem(
    list,
    "Have a data modeling question or want to participate in discussions around Neo4j and graphs?",
    "The <a href='https://groups.google.com/forum/?fromgroups#!forum/neo4j'>Neo4j Google Group</a> is a great place for this." );
appendListItem( list, "Is 140 characters enough for your question?", +"Then obviously Twitter is an option. "
    + "There's lots of <a href='https://twitter.com/search?q=neo4j'>#neo4j</a> activity there." );
appendListItem( list, "Have a question on the content of this page or missing something here?",
    "Then you're all set, use the discussion thread below. "
        + "Please post any comments or suggestions regarding the documentation right here!" );

$( intro ).appendTo( "#neo-disqus-wrapper" );
$( "<div id='disqus_thread'></div>" ).appendTo( "#neo-disqus-wrapper" );

( function()
{
  var dsq = document.createElement( 'script' );
  dsq.type = 'text/javascript';
  dsq.async = true;
  dsq.src = 'http://' + disqus_shortname + '.disqus.com/embed.js';
  ( document.getElementsByTagName( 'head' )[0] || document.getElementsByTagName( 'body' )[0] ).appendChild( dsq );
} )();
