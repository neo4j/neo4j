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
  versionSwitcher( jQuery );
} );

/**
 * Utility to browse different versions of the documentation. Requires the versions.js file loaded, which lists the
 * available (relevant) versions of the manual.
 */
function versionSwitcher( $ )
{
  var MAX_STABLE_COUNT = 2;

  var currentVersion = window.neo4jVersion;
  var currentPage = window.neo4jPageId + '.html';

  loadVersions();

  /**
  * Load an array of version into a div element and check if the current page actually exists in these versions.
  * Non-existing entries will be unlinked.
  * Current version will be marked as such.
  */
  function loadVersions()
  {
    var $navHeader = $( '#navheader' );
    var $versionSelector = $( '<div id="versions"/>' );
    var $links = $( '<ul class="list-inline"/>' ).appendTo( $versionSelector );
    var extraVersions = [];
    var stableCount = 0;
    $.each( availableDocVersions, function( index, version )
    {
      if ( version === currentVersion )
      {
        return;
      }
      var firstChar = version.charAt( 0 );
      if ( firstChar < '0' || firstChar > '9' )
      {
        extraVersions.push( version );
        return;
      }
      if ( version.indexOf( 'M' ) === -1 )
      {
        stableCount++;
        if ( stableCount > MAX_STABLE_COUNT )
        {
          extraVersions.push( version );
          return;
        }
      }
      addVersion( version, $links );
    } );
    $navHeader.append( $versionSelector );

    var $dropdown = $( '<div id="additional-versions"><div class="dropdown"><a class="dropdown-toggle"id="dropdownMenu1" data-toggle="dropdown"><i class="fa fa-caret-down"></i></a></div></div>' );
    var $additionalVersions = $( '<ul class="dropdown-menu dropdown-menu-right" role="menu" aria-labelledby="dropdownMenu1"/>' );
    $.each( extraVersions, function( index, version )
    {
      addVersion( version, $additionalVersions );
    } );
    $dropdown.children().first().append( $additionalVersions );
    $navHeader.append( $dropdown );
  }

  function addVersion( version, $container )
  {
    var $optionWrapper = $( '<li />' );
    var $newOption = $( '<a role="menuitem">' + version + '</a>' ).appendTo( $optionWrapper );
    var url = 'http://docs.neo4j.org/chunked/' + version + '/' + currentPage;
    $container.append( $optionWrapper );
    checkUrlExistence( url, function()
    {
      $newOption.attr( 'href', url );
      $newOption.attr( 'title', 'See this page in version ' + version + '.' );
    }, function()
    {
      $newOption.attr( 'title', 'This page does not exist in version ' + version + '.' );
      $optionWrapper.addClass( 'disabled' );
    } );
  }

  /**
   * Check if a specific URL exists. The success and failure functions will be automatically called on finish.
   */
  function checkUrlExistence( url, success, failure )
  {
    var settings = {
      'type' : 'HEAD',
      'async' : true,
      'url' : url
    };
    if ( success )
      settings.success = success;
    if ( failure )
      settings.error = failure;
    $.ajax( settings );
  }
}