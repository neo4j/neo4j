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
jQuery( window ).load( function()
{
  jQuery.getScript("../versions.js", function()
  {
    // availableDocVersions is now set
    versionSwitcher( jQuery );
  } );
} );

/**
 * Utility to browse different versions of the documentation.
 * Requires the ../versions.js file in place in place, which
 * lists the available (relevant) versions of the manual.
 * The version to load at page load can be given via the
 * search string, for example ?1.3
 */
function versionSwitcher( $ )
{
  var VERSION_AND_PAGE = /chunked\/(.*)\/(.*\.html)?/;
  var contentElements = [ "", "section", "chapter", "book", "part" ];
  var CONTENT_ELEMENT = contentElements.join( ",body>div." ).replace( /^,/, " " );
  var CONTENT_ELEMENT_LOAD = contentElements.join( ",div." ).replace( /^,/, " " );
  var url = window.location;
  var path = url.pathname;
  var pathinfo = getVersionAndPage( path );
  if ( !pathinfo ) return;
  var currentVersion = pathinfo.version;
  var LOAD_FROM_VERSION = currentVersion;
  var currentPage = pathinfo.page;

  var contentElement = $( CONTENT_ELEMENT );

  var versionSelector = $( '<select id="version-selector" title="See this page for a different Neo4j version." name="version-selector"></select>' );

  loadVersionsIntoSelector( availableDocVersions, versionSelector );

  // select the current option
  setSelector2CurrentVersion( versionSelector, currentVersion );

  // add the dropdown to the page
  $("div.navheader").append( versionSelector );

  // handle changes in the dropdown
  versionSelector.change( function()
  {
    if ( this.selectedIndex === -1 ) return;
    var newVersion = this.options[this.selectedIndex].value;
    loadNewVersion( newVersion );
  } );

  // PRIVATE FUNCTIONS

  /**
   * Load an array of version into a select element and
   * check if the current page actually exists in these versions.
   * Non-existing entries will be disabled.
   */
  function loadVersionsIntoSelector( availableDocVersions, versionSelector )
  {
    $.each( availableDocVersions, function( index, version )
    {
      // add options in disabled state, then enable if the head request was successful
      var newOption = $( '<option disabled="disabled" value="' + version + '">' + version + '</option>' );
      versionSelector.append( newOption );
      checkExistence( version, currentPage, function () 
      {
        newOption.removeAttr( 'disabled' );
      } );
    } );
  }

  /**
   * Set the given version as the selected element in the version selector.
   * If the version is missing, it will be added.
   */
  function setSelector2CurrentVersion( versionSelector, version )
  {
    var currentOptionElement = versionSelector.children( '[value="' + version + '"]' );
    if ( currentOptionElement.length === 0 )
    {
      // add a new option, as the current version wasn't in the list
      var newOption = new Option( version, version );
      newOption.selected = true;
      versionSelector.prepend( newOption );
    }
    else
    {
      currentOptionElement.attr( "selected", true );
    }
  }

  /**
   * Load page contents from a different version into the page.
   * Also loads navigation header/footer from that version.
   */
  function loadNewVersion ( newVersion )
  {
    redirect2NewVersion( newVersion );
  }

  /**
   * Redirect to another version.
   */
  function redirect2NewVersion ( newVersion )
  {
    location.assign( "/chunked/" + newVersion + "/" + currentPage );
  }

  /**
   * Load page contents from a different version into the page.
   * Also loads navigation header/footer from that version.
   */
  function loadNewVersionIntoPage ( newVersion )
  {
    contentElement.load( "/chunked/" + newVersion + "/" + currentPage + CONTENT_ELEMENT_LOAD, function( response, status )

    {
      if ( status == "success" )
      {
        SyntaxHighlighter.highlight();
        // append version to URL to enable browsing of a different version
        fixNavigation ( $( "a.xref, div.toc a", contentElement ), newVersion );
        // load the navheader and navfooter as well
        loadPart( "navheader", newVersion, currentPage );
        loadPart( "navfooter", newVersion, currentPage );
      }
    } );
  }

  /**
   * Change links to enable smooth browsing of a different version.
   * Links that are present in the "base version" as well will get the
   * version to browse added to their URL, while those which
   * are not present will be directly linked to the old version instead.
   */
  function fixNavigation( elements, version )
  {
    if ( LOAD_FROM_VERSION === version ) return;
    elements.each( function()
    {
      var link = this;
      var versionAndPage = getVersionAndPage( link.href );
      if ( versionAndPage )
      {
        checkUrlExistence( link, function ()
        {
          link.href = versionAndPage.page + "?" + version;
          }, function()
        {
          link.href = "../" + version + "/" + versionAndPage.page;
        } );
      }
    } );
  }
  
  /**
   * Load a specific part of the page (not the main content).
   */
  function loadPart( partName, newVersion, currentPage )
  {
    $( "div." + partName ).load( "/chunked/" + newVersion + "/" + currentPage + " div." + partName, function ( response, status ) 
    {
      if ( status == "success")
      {
        fixNavigation( $( "div." + partName + " a" ), newVersion );
      }
    } );
  }

  /**
   * Check if a specific version of a page exists.
   * The success and failure functions will be automatically called on finish.
   */
  function checkExistence( version, page, success, failure )
  {
     var url = "../" + version + "/" + page;
     checkUrlExistence ( url, success, failure );
  }
  
  /**
   * Check if a specific URL exists.
   * The success and failure functions will be automatically called on finish.
   */
  function checkUrlExistence( url, success, failure )
  {
    var settings = { "type": "HEAD", "async": true, "url": url };
    if ( success ) settings.success = success;
    if ( failure ) settings.error = failure;
    $.ajax( settings );
  }

  /**
   * Parse a path to extract version number and page filename.
   */
  function getVersionAndPage( path )
  {
    var pathinfo = path.match( VERSION_AND_PAGE );
    if ( !pathinfo || !pathinfo[1] ) return null;
    var currentVersion = pathinfo[1];
    var currentPage = pathinfo[2] ? pathinfo[2] : "index.html";
    return { version: currentVersion, page: currentPage };
  }
}

