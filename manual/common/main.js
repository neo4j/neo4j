/**
 * Miscellaneous js functions for WebHelp Kasun Gajasinghe, http://kasunbg.blogspot.com David Cramer,
 * http://www.thingbag.net
 * 
 */

// Turn ON and OFF the animations for Show/Hide Sidebar. Extend this to other
// anime as well if any.
var noAnimations = false;

$( document ).ready( function()
{
  var $content = $( '#content' );
  $( '<div id="sidebar-wrapper"/>' ).insertAfter( $content ).load( 'webhelp-tree.html', initialize );
  if ( $content.length > 0 )
  {
    var $body = $( 'body' );
    var SIDEBAR_COOKIE_NAME = 'manual_toc_visible';
    function setSidebarCookie( value )
    {
      $.cookie( SIDEBAR_COOKIE_NAME, value, { expires: 3, path: '/' } );
    }
    var sidebarCookieValue = $.cookie( SIDEBAR_COOKIE_NAME );
    if ( sidebarCookieValue === 'no' || ( typeof sidebarCookieValue === 'undefined' && $( window ).width() < 769 ) )
    {
      $body.addClass( 'toc-is-hidden' );
    }
    
    $( "#showHideButton" ).click( function()
    {
      if ( $body.hasClass( 'toc-is-hidden' ) )
      {
        $body.removeClass( 'toc-is-hidden' );
        setSidebarCookie( 'yes' );
      }
      else
      {
        $body.addClass( 'toc-is-hidden' );
        setSidebarCookie( 'no' );
      }
      return false;
    } );
  }
  addBootstrapStyling();
  
  $( 'h3, h4, h5, h6', $content ).click(function()
  {
    var $heading = $( this );
    var $section = $heading.parents( 'section' ).first();
    if ( !$section.length )
    {
      return;
    }
    var id = $section.attr( 'id' );
    if ( id )
    {
      var ident = '#' + id;
      if ( window.history && window.history.pushState )
      {
        window.history.pushState( null, $heading.text(), ident );
        $content.scrollTo( $section, { 'duration': 300 } );
      }
      else
      {
        window.location.assign( ident );
      }
    }
  });
} );

function addBootstrapStyling()
{
  var $content = $('#content section');
  $('img', $content).addClass('img-responsive');
  $('div.admonitionblock img', $content).removeClass('img-responsive');
  $('#deployment-requirements dl, #rest-api-traverse dl').addClass('dl-horizontal');
  $('div.table table,div.informaltable table', $content).addClass('table table-condensed table-hover');
  var $admonblocks = $('div.admonitionblock');
  $admonblocks.filter('.Note').find('td.content').addClass('alert alert-info');
  $admonblocks.filter('.Tip').find('td.content').addClass('alert alert-info');
  $admonblocks.filter('.Important').find('td.content').addClass('alert alert-warning');
  $admonblocks.filter('.Caution').find('td.content').addClass('alert alert-warning');
  $admonblocks.filter('.Warning').find('td.content').addClass('alert alert-danger');
  $('div.sidebar', $content).addClass('alert alert-info');
  $('#content div.titlepage div.abstract').addClass('alert alert-info');
}

function addSearchHighlightButton()
{
  $('#navLinks')
  .append('<span id="showHideHighlight"><a tabindex="6" href="javascript:;" title="Toggle search highlight">Highlight <i class="fa fa-eraser"></i></a></span>')
  .click(toggleHighlight);
}

function initialize()
{
  // Set the webhelp-currentid class on the current page in the treenav.
  var foundPage = undefined;
  var page = window.location.href;
  if ( window.location.hash )
  {
    // only look at the URL without the hash
    page = page.substr( 0, page.length - window.location.hash.length );
  }
  if ( page )
  {
    $( '#tree' ).find( 'a' ).each( function()
    {
      if ( this.href === page )
      {
        $( this ).addClass( 'webhelp-currentid' );
        foundPage = this;
        return false;
      }
    } );
  }
  /*
   * works better without this. // When you click on a link to an // anchor, scroll down // 105 px to cope with the fact
   * that the // banner // hides the top 95px or so of the page. // This code deals with the problem when // you click
   * on a link within a page. $('a[href*=#]') .click( function() { if (location.pathname.replace(/^\//, '') ==
   * this.pathname .replace(/^\//, '') && location.hostname == this.hostname) { var $target = $(this.hash); $target =
   * $target.length && $target || $('[name=' + this.hash.slice(1) + ']'); if (!(this.hash == "#searchDiv" || this.hash ==
   * "#treeDiv" || this.hash == "") && $target.length) { var targetOffset = $target.offset().top - 120;
   * $('html,body').animate({ 'scrollTop' : targetOffset }, 200); return false; } } });
   */
  // Generate tabs in nav-pane with JQuery
  $( function()
  {
    $( "#tabs" ).tabs( {
      cookie : {
        expires : 2
      // store cookie for 2 days.
      }
    } );
  } );

  // Generate the tree
  $( "#ulTreeDiv" ).attr( "style", "" );
  $( "#tree" ).treeview( {
    collapsed : true,
    animated : false,
    control : "#sidetreecontrol",
    persist : "cookie",
    toggle : toggleMenuItem
  } );

  function toggleMenuItem( num, ul )
  {
    var $li = $( ul.parentNode );
    var $icon = $li.children( "i" );
    setIcon( $li, $icon );
  }

  function setIcon( $li, $icon )
  {
    if ( $li.hasClass( "expandable" ) )
    {
      $icon.removeClass( "fa-folder-open-o" ).addClass( "fa-folder-o" );
    }
    else
    {
      $icon.removeClass( "fa-folder-o" ).addClass( "fa-folder-open-o" );
    }
  }

  // after toc fully styled, display it.
  // Until loading, a 'loading' image will
  // be displayed
  $( "#tocLoading" ).attr( "style", "display:none;" );
  // $("#ulTreeDiv").attr("style","display:block;");

  // .searchButton is the css class
  // applied to 'Go' button
  $( function()
  {
    $( "button", ".searchButton" ).button();

    $( "button", ".searchButton" ).click( function()
    {
      return false;
    } );
  } );

  var searchText = $.cookie( 'textToSearch' );
  if ( searchText != undefined && searchText.length > 0 )
  {
    document.getElementById( 'textToSearch' ).value = searchText;
    // 'ui-tabs-1' is the cookie name which
    // is used for the persistence of the
    // tabs.(Content/Search tab)
    if ( $.cookie( 'ui-tabs-1' ) === '1' )
    {
      // search tab is active
      $.cookie( 'ui-tabs-1', '0' ); // default to not keep it active on next page
      Verifie( 'searchForm' );
      if ( searchText.length > 1 )
      {
        searchHighlight( searchText );
        addSearchHighlightButton();
        $('#showHideHighlight i').css('color', '#ffe48b');
      }
      else
      {
        highlightOn = false;
      }
    }
  }

  syncToc( foundPage );
  // Synchronize the toc tree
  // with the content pane,
  // when loading the page.
  // $("#doSearch").button(); //add jquery
  // button styling to 'Go' button

  // add folder look
  var $ICON = $( '<i class="fa"/>' );
  $( "#tree li" ).each( function()
  {
    var $li = $( this );
    var $file = $li.children( "span.file" );
    if ( $file.siblings( "ul" ).size() > 0 )
    {
      $file.addClass( "folder" ).removeClass( "file" );
      var $icon = $ICON.clone().insertBefore( $file );
      setIcon( $li, $icon );
    }
    else
    {
      $ICON.clone().addClass( "fa-file-o" ).insertBefore( $file );
    }
  } );
}

/**
 * If an user moved to another page by clicking on a toc link, and then clicked on #searchDiv, search should be
 * performed if the cookie textToSearch is not empty.
 */
function doSearch()
{
  // 'ui-tabs-1' is the cookie name which is used for the persistence of the
  // tabs.(Content/Search tab)
  var searchText = $.cookie( 'textToSearch' );
  if ( searchText != undefined && searchText.length > 0 )
  {
    document.getElementById( 'textToSearch' ).value = searchText;
    Verifie( 'searchForm' );
    
  }
}

/**
 * Synchronize with the tableOfContents
 */
function syncToc( a_ )
{
  if ( a_ === undefined )
  {
    return;
  }
  var a = a_.parentNode.parentNode;
  if ( a === undefined )
  {
    return;
  }
  // Expanding the child sections of the selected node.
  var nodeClass = a.getAttribute( "class" );
  if ( nodeClass != null && !nodeClass.match( /collapsable/ ) )
  {
    a.setAttribute( "class", "collapsable" );
    // remove display:none; css style from <ul> block in the selected
    // node.
    var ulNode = a.getElementsByTagName( "ul" )[0];
    if ( ulNode != undefined )
    {
      if ( ulNode.hasAttribute( "style" ) )
      {
        ulNode.setAttribute( "style", "display: block; background-color: #D8D8D8 !important;" );
      }
      else
      {
        var ulStyle = document.createAttribute( "style" );
        ulStyle.nodeValue = "display: block; background-color: #D8D8D8 !important;";
        ulNode.setAttributeNode( ulStyle );
      }
    }
    // adjust tree's + sign to -
    var divNode = a.getElementsByTagName( "div" )[0];
    if ( divNode != undefined )
    {
      if ( divNode.hasAttribute( "class" ) )
      {
        divNode.setAttribute( "class", "hitarea collapsable-hitarea" );
      }
      else
      {
        var divClass = document.createAttribute( "class" );
        divClass.nodeValue = "hitarea collapsable-hitarea";
        divNode.setAttributeNode( divClass );
      }
    }
    // set persistence cookie when a node is auto expanded
    // setCookieForExpandedNode("webhelp-currentid");
  }
  var b = a.getElementsByTagName( "a" )[0];

  if ( b != undefined )
  {
    // Setting the background for selected node.
    var style = a.getAttribute( "style", 2 );
    if ( style != null && !style.match( /background-color: Background;/ ) )
    {
      a.setAttribute( "style", "background-color: #D8D8D8; " + style );
      b.setAttribute( "style", "color: black;" );
    }
    else if ( style != null )
    {
      a.setAttribute( "style", "background-color: #D8D8D8; " + style );
      b.setAttribute( "style", "color: black;" );
    }
    else
    {
      a.setAttribute( "style", "background-color: #D8D8D8; " );
      b.setAttribute( "style", "color: black;" );
    }
  }

  // shows the node related to current content.
  // goes a recursive call from current node to ancestor nodes, displaying
  // all of them.
  while ( a.parentNode && a.parentNode.nodeName )
  {
    var parentNode = a.parentNode;
    var nodeName = parentNode.nodeName;

    if ( nodeName.toLowerCase() == "ul" )
    {
      parentNode.setAttribute( "style", "display: block;" );
    }
    else if ( nodeName.toLocaleLowerCase() == "li" )
    {
      parentNode.setAttribute( "class", "collapsable" );
      parentNode.firstChild.setAttribute( "class", "hitarea collapsable-hitarea " );
    }
    a = parentNode;
  }
}
/*
 * function setCookieForExpandedNode(nodeName) { var tocDiv = document.getElementById("tree"); //get table of contents
 * Div var divs = tocDiv.getElementsByTagName("div"); var matchedDivNumber; var i; for (i = 0; i < divs.length; i++) {
 * //1101001 var div = divs[i]; var liNode = div.parentNode; } //create a new cookie if a treeview does not exist if
 * ($.cookie(treeCookieId) == null || $.cookie(treeCookieId) == "") { var branches =
 * $("#tree").find("li");//.prepareBranches(treesettings); var data = []; branches.each(function(i, e) { data[i] =
 * $(e).is(":has(>ul:visible)") ? 1 : 0; }); $.cookie(treeCookieId, data.join("")); }
 * 
 * if (i < divs.length) { var treeviewCookie = $.cookie(treeCookieId); var tvCookie1 = treeviewCookie.substring(0, i);
 * var tvCookie2 = treeviewCookie.substring(i + 1); var newTVCookie = tvCookie1 + "1" + tvCookie2;
 * $.cookie(treeCookieId, newTVCookie); } }
 */

/**
 * Code for search highlighting
 */
var highlightOn = true;
function searchHighlight( searchText_ )
{
  var searchText = searchText_;
  highlightOn = true;
  if ( searchText != undefined )
  {
    var wList;
    var sList = new Array(); // stem list
    // Highlight the search terms
    searchText = searchText.toLowerCase().replace( /<\//g, "_st_" ).replace( /\$_/g, "_di_" ).replace(
        /\.|%2C|%3B|%21|%3A|@|\/|\*/g, " " ).replace( /(%20)+/g, " " ).replace( /_st_/g, "</" ).replace( /_di_/g,
        "%24_" );
    searchText = searchText.replace( / +/g, " " );
    searchText = searchText.replace( / $/, "" ).replace( /^ /, "" );

    wList = searchText.split( " " );
    $( "#content" ).highlight( wList ); // Highlight the search input

    if ( typeof stemmer != "undefined" )
    {
      // Highlight the stems
      for ( var i = 0; i < wList.length; i++ )
      {
        var stemW = stemmer( wList[i] );
        sList.push( stemW );
      }
    }
    else
    {
      sList = wList;
    }
    $( "#content" ).highlight( sList ); // Highlight the search input's all
    // stems
  }
}

function toggleHighlight()
{
  if ( highlightOn )
  {
    $('span.highlight', '#content').addClass('unhighlight');
    $('#showHideHighlight i').css('color', 'inherit');
  }
  else
  {
    $('span.highlight', '#content').removeClass('unhighlight');
    $('#showHideHighlight i').css('color', '#ffe48b');
  }
  highlightOn = !highlightOn;
}