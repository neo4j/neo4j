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

/* Sidebar loader.
 * Dynamically inserts a sidebar into the page.
 */

jQuery( document ).ready(  function()
{
  loadOfflineSidebar( jQuery );
});

function loadOfflineSidebar( $ )
{
  var sidebar = $( "<div id='sidebar' />" );
  var PAGE = /.*\/(.+?)\./;
  var HTML_URL_BASE = "http://docs.neo4j.org/chunked/";
  var PDF_URL_BASE = "http://info.neotechnology.com/download-pdf.html?document=";
  var mainContent = getMainContent();
  if ( !mainContent )
  {
    return;
  }
  mainContent.addClass( "left-column" );
  addOnlineLink();
  addPdfLink();
  $( "html" ).append( sidebar );

  function addOnlineLink()
  {
    var pathinfo = window.location.pathname.match( PAGE );
    if ( pathinfo && pathinfo[1] )
    {
      var url = HTML_URL_BASE + neo4jVersion + "/" + pathinfo[1] + ".html";
      var html = "<div id='online-link'><p>View <a href='" + url +
        "'>this page online</a> for dicussion threads and other additional features.</p></div>"
        sidebar.append( $( html ) );
    }
  }

  function addPdfLink()
  {
    var url = PDF_URL_BASE + neo4jVersion;
    var html = "<div id='pdf-link'><p>Get a PDF version of the Neo4j Manual <a href='" + url +
      "'>here</a>.</p></div>"
      sidebar.append( $( html ) );
  }

  function getMainContent()
  {
    var mainContent;
    var navHeader = $( "body > div.navheader" );
    if ( navHeader && navHeader[0] )
    {
      mainContent = $( navHeader[0] ).next();
    }
    return mainContent;
  }

}


