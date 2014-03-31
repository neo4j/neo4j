/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

function initDisqus()
{
  var OPEN_ICON = 'fa-minus-circle';
  var CLOSED_ICON = 'fa-plus-circle';
  var $pageContent = $( '#content' );
  var $wrapper = $( '<div id="discuss" />' );
  var $header = $( '<div id="discuss-header"><i id="discuss-icon" class="fa fa-comments"></i> <b>Comments</b></div>' );
  var $toggle = $( '<i id="discuss-toggle" class="fa ' + CLOSED_ICON + '"></i>' );
  var $body = $( '<div id="discuss-body" />' );
  var $thread = $( '<div id="disqus_thread"></div>' );
  $header.append( $toggle );
  $wrapper.append( $header );
  $body.append( $thread );
  $wrapper.append( $body );

  var initialized = false;
  var heightUpdated = false;

  function scrollToComments( openDuringLoad )
  {
    $pageContent.scrollTo( openDuringLoad ? $thread : $wrapper );
  }
  
  function showComments( openDuringLoad )
  {
    $body.css( 'display', 'block' );
    if ( !initialized )
    {
      initialized = true;
      $thread.mutate( 'height', function()
      {
        if ( !heightUpdated )
        {
          heightUpdated = true;
          scrollToComments( openDuringLoad );
        }
      } );
      runDisqus( $thread );
    }
    $toggle.removeClass( CLOSED_ICON ).addClass( OPEN_ICON );
  }
  
  function hideComments()
  {
    $body.css( 'display', 'none' );
    $toggle.removeClass( OPEN_ICON ).addClass( CLOSED_ICON );
  }

  $header.click( function()
  {
    if ( $toggle.hasClass( OPEN_ICON ) )
    {
      hideComments();
    }
    else
    {
      showComments();
    }
  } );

  $( '#content > footer' ).first().prepend( $wrapper );
  
  var hash = window.location.hash;
  if ( hash && hash.length > 10 && hash.indexOf( '#comment' ) === 0 )
  {
    showComments( true );
  }
}

$( document ).ready( initDisqus );

function runDisqus( $thread )
{
  window.disqus_identifier = "manual";
  window.disqus_title = "The Neo4j Manual";

  if ( window.neo4jPageId === 'index' )
  {
    window.disqus_identifier += '-toc';
  }
  else
  {
    window.disqus_identifier += '-' + window.neo4jPageId;
  }

  if ( document.title )
  {
    window.disqus_title = document.title;
    var match = window.disqus_title.match( /^(Chapter|)[0-9\.\s]*(.*)$/ );
    if ( match && match[2] )
    {
      window.disqus_title = match[2];
    }
  }

  window.disqus_url = window.location;
  window.disqus_shortname = "neo4j";
  if ( window.disqus_url.host.indexOf( 'neo4j.' ) !== -1 && window.disqus_url.pathname.indexOf( '/lab/' ) === -1 )
  {
    var docsLocation = "http://docs.neo4j.org/chunked/snapshot/";
    var path = window.disqus_url.pathname;
    var position = path.lastIndexOf( '/' );
    if ( position === -1 )
    {
      position = path.lastIndexOf( '\\' );
    }
    if ( position > 0 )
    {
      var page = path.substring( position + 1 );
      window.disqus_url = docsLocation + page;
    }
  }
  else
  {
    window.disqus_shortname = 'neo4j-manual-staging';
  }

  var $intro = $( '<div id="discuss-intro"/>' );
  var listWrapper = $( '<div class="itemizedlist">' );
  var list = $( '<ul type="disc" class="itemizedlist" />' );
  listWrapper.append( list );
  $intro.append( listWrapper );

  function appendListItem( list, heading, content )
  {
    list.append( "<li class='listitem'><em>" + heading + "</em> " + content + "</li>" );
  }

  appendListItem( list, "Having trouble running an example from the manual?",
      "First make sure that you're using the same version of Neo4j as the manual was built for! "
          + "Choose version at the top of the page." );
  appendListItem( list, "Something doesn't work?",
      "Use the stackoverflow.com <a href='http://stackoverflow.com/questions/tagged/neo4j'>neo4j tag</a>!" );
  appendListItem( list, "Found a bug?",
      "GitHub <a href='https://github.com/neo4j/neo4j/issues'>Neo4j Issues</a> is for you. "
          + "For <i>documentation bugs</i>, use the Disqus thread below." );
  appendListItem(
      list,
      "Have a data modeling question or want to participate in discussions around Neo4j and graphs?",
      "The <a href='https://groups.google.com/forum/?fromgroups#!forum/neo4j'>Neo4j Google Group</a> is a great place for this." );
  appendListItem( list, "Is 140 characters enough?", "Go <a href='https://twitter.com/search?q=neo4j'>#neo4j</a>." );
  appendListItem( list, "Have a question on the content of this page or missing something here?",
      "Use the discussion thread below. "
          + "Please post any comments or suggestions regarding the documentation right here!" );

  $thread.before( $intro );

  ( function()
  {
    var dsq = document.createElement( 'script' );
    dsq.type = 'text/javascript';
    dsq.async = true;
    dsq.src = 'http://' + window.disqus_shortname + '.disqus.com/embed.js';
    ( document.getElementsByTagName( 'head' )[0] || document.getElementsByTagName( 'body' )[0] ).appendChild( dsq );
  } )();
}
