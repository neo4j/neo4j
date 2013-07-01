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

/* Cypher Console
 * Adds live cypher console feature to a page.
 */

jQuery( document ).ready(  function()
{
  if ( jQuery.browser.msie ) return;
  createCypherConsoles( jQuery );
});

function createCypherConsoles( $ )
{
  var currentButton;
  var URL_BASE = "http://console-test.neo4j.org/";
  var REQUEST_BASE = URL_BASE + "?";
  
  $('pre.cypher').wrap('<div class="query-wrapper" />').each( function()
  {
    var pre = $(this);
    pre.parent().data('query', pre.text());
  });

  $('p.cypherconsole').each( function()
  {
    var context = $( this );
    var title = $.trim( context.find( '> b, > strong' ).eq(0).text() ) || 'Live Cypher Console';
    title = title.replace( /\.$/, '' );
    var database = context.find( 'span.database' ).eq(0).text();
    if ( !database ) return;
    var command = context.find( 'span.command > strong' ).eq(0).text();
    if ( !command ) return;
    var button = $( '<button class="cypherconsole" type="button" title="Show a console" id="console-iframe-button"><img src="css/utilities-terminal.svg" /><span> ' + title + '</span></button>' );
    var url = getUrl( database, command );
    var link = $( '<button class="cypherconsole" type="button" title="Open the console in a new window." id="console-external-button"><img src="css/external.svg" /><span>&#8201;</span></button>' );
    link.click( function()
    {
      window.open( url, '_blank' );
    });    
    button.click( function()
    {
      handleCypherClick( button, link, url, title );
    });    
    button.insertAfter( this );
    link.insertAfter( button );
  });

  $('p.cypherdoc-console').first().each( function()
  {
    var context = $( this );
    var url = getUrl( "none", "none", "\n\nClick the play buttons to run the queries!" );
    var iframe = $( "<iframe/>" ).attr( "id", "cypherdoc-console" ).addClass( "cypherdoc-console" ).attr( "src", url );
    context.append( iframe );
    context.height( iframe.height() );
    var button = $('<button class="run-query" title="Execute query"><img src="images/play.png" /></button>');
    $('div.query-wrapper').append(button.clone().click(function()
    {
      var query = $(this).parent().data('query');
      $('iframe.cypherdoc-console')[0].contentWindow.postMessage(query, '*');
    }));
    $window = $(window);
    var pathname = window.location.pathname;
    if ( pathname !== "/" && ( pathname.length < 11 || pathname.substr( -11 ) !== "/index.html" ) )
    {
      $window.scroll( function()
      {
        if ( $window.scrollTop() > 150 )
        {
          iframe.css( 'position', 'fixed' );
        }
        else
        {
          iframe.css( 'position', 'static' );
        }
      } );
    }
  });
  
  function getUrl( database, command, message )
  {
    var url = REQUEST_BASE;
    if ( database !== undefined )
    {
      url += "init=" + encodeURIComponent( database );
    }
    if ( command !== undefined )
    {
      url += "&query=" + encodeURIComponent( command );
    }
    if ( message !== undefined )
    {
      url += "&message=" + encodeURIComponent( message );
    }
    if ( window.neo4jVersion != undefined )
    {
      url += "&version=" + encodeURIComponent( neo4jVersion );
    }
    return url + "&no_root=true";
  }
  
  function handleCypherClick( button, link, url, title )
  {
    var iframe=$( "#console" );
    if ( iframe.length )
    {
      iframe.remove();
    }
    if ( button === currentButton )
    {
      // hitting the same button again -- don't add a new console
      currentButton = null;
      return;
    }
    iframe = $( "<iframe/>" ).attr( "id", "console" ).addClass( "console" ).attr( "src", url );
    link.after( iframe );
    currentButton = button;
  } 
}