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
  createCypherConsoles( jQuery );
});

function createCypherConsoles( $ )
{
  var currentButton;
  var URL_BASE = "http://console.neo4j.org/";
  var REQUEST_BASE = URL_BASE + "?";
  var RESOURCE_FILES = ["javascripts/jquery-1.6.4.min.js", "javascripts/d3.min.js", "javascripts/visualization.js", "javascripts/console.js", "img/twitter.jpg", "img/info.png", "img/Link-icon.png", "img/graph.png"];

  $('p.cypherconsole').each( function()
  {
    var title = $.trim( $( 'b', this ).eq(0).text() ) || 'Live Cypher Console';
    title = title.replace( /\.$/, '' );
    var database = $( 'span.database', this ).eq(0).text();
    if ( !database ) return;
    var command = $( 'strong', this ).eq(0).text();
    if ( !command ) return;
    var button = $( '<button class="cypherconsole" type="button"><img src="css/utilities-terminal.png" /> ' + title + '</button>' );
    button.click( function()
    {
      handleCypherClick( button, database, command, title );
    });    
    button.insertAfter( this );
  });
  
  $( RESOURCE_FILES ).each( function()
  {
    var target = URL_BASE + this;
    // need header on server setup first
    // $.get( target );
  });
  
  function handleCypherClick( button, database, command, title )
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
    var url= REQUEST_BASE;
    url += "init=" + encodeURIComponent( database );
    url += "&query=" + encodeURIComponent( command );
    if ( neo4jVersion )
    {
      url += "&version=" + encodeURIComponent( neo4jVersion );
    }
    iframe = $( "<iframe/>" ).attr( "id", "console" ).addClass( "console" ).attr( "src", url );
    button.after( iframe );
    currentButton = button;
  } 
}

