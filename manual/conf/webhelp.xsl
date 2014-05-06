<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
  xmlns:d="http://docbook.org/ns/docbook" xmlns="http://www.w3.org/1999/xhtml" exclude-result-prefixes="d">

  <xsl:import href="urn:docbkx:stylesheet" />

  <xsl:import href="common.xsl"/>
  <xsl:import href="html-params.xsl"/>
  <xsl:import href="head.xsl"/>
  <xsl:import href="syntaxhighlight.xsl"/>
  <xsl:import href="xhtml-table.xsl"/>
  <xsl:import href="xhtml-admon.xsl"/>
  <xsl:import href="xhtml-titlepage.xsl"/>
  <xsl:import href="footer.xsl"/>

<!-- 
  <xsl:template match="d:formalpara[@role = 'cypherconsole']" />
  <xsl:template match="d:simpara[@role = 'cypherconsole']" />
 -->

  <!-- Get rid of table numbering -->
  <xsl:param name="local.l10n.xml" select="document('')" />
  <l:i18n xmlns:l="http://docbook.sourceforge.net/xmlns/l10n/1.0">
    <l:l10n language="en">
      <l:context name="title">
        <l:template name="table" text="%t" />
      </l:context>
      <l:context name="xref-number-and-title">
        <l:template name="table" text="%t" />
      </l:context>
    </l:l10n>
  </l:i18n>

  <xsl:template name="webhelpheader.logo">
    <a href="index.html" id="logo">
      <img src='common/images/logo-neo4j.svg' alt="" />
    </a>
  </xsl:template>

  <xsl:template name="user.footer.navigation">
    <xsl:if test="@id = 'preface'">
      <xsl:call-template name="write.chunk">
        <xsl:with-param name="filename">
          <xsl:value-of select="concat($webhelp.base.dir,'/webhelp-tree.html')" />
        </xsl:with-param>
        <xsl:with-param name="method" select="'xml'" />
        <xsl:with-param name="omit-xml-declaration" select="'yes'" />
        <xsl:with-param name="encoding" select="'utf-8'" />
        <xsl:with-param name="indent" select="'no'" />
        <xsl:with-param name="doctype-public" select="''" />
        <xsl:with-param name="doctype-system" select="''" />
        <xsl:with-param name="content">
          <xsl:call-template name="webhelptoc">
            <xsl:with-param name="currentid" select="generate-id(.)" />
          </xsl:call-template>
        </xsl:with-param>
      </xsl:call-template>
    </xsl:if>
  </xsl:template>

  <!-- Included to get rid of the webhelp-currentid getting set on an element. -->
  <!-- Generates the webhelp table-of-contents (TOC). -->
  <xsl:template
    match="d:book|d:part|d:reference|d:preface|d:chapter|d:bibliography|d:appendix|d:article|d:topic|d:glossary|d:section|d:simplesect|d:sect1|d:sect2|d:sect3|d:sect4|d:sect5|d:refentry|d:colophon|d:bibliodiv|d:index|d:setindex"
    mode="webhelptoc">
    <xsl:param name="currentid" />
    <xsl:param name="level" select="0" />
    <xsl:variable name="title">
      <xsl:if test="$webhelp.autolabel=1">
        <xsl:variable name="label.markup">
          <xsl:apply-templates select="." mode="label.markup" />
        </xsl:variable>
        <xsl:if test="normalize-space($label.markup)">
          <xsl:value-of select="concat($label.markup,$autotoc.label.separator)" />
        </xsl:if>
      </xsl:if>
      <xsl:apply-templates select="." mode="titleabbrev.markup" />
    </xsl:variable>

    <xsl:variable name="href">
      <xsl:choose>
        <xsl:when test="$manifest.in.base.dir != 0">
          <xsl:call-template name="href.target" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="href.target.with.base.dir" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:variable name="id" select="generate-id(.)" />

    <xsl:if test="not(self::d:index) or (self::d:index and not($generate.index = 0))">
      <li>
        <span class="file">
          <a href="{substring-after($href, $base.dir)}" tabindex="1">
            <xsl:value-of select="$title" />
          </a>
        </span>
        <xsl:if
          test="$level &lt; 2 and (d:part|d:reference|d:preface|d:chapter|d:bibliography|d:appendix|d:article|d:topic|d:glossary|d:section|d:simplesect|d:sect1|d:sect2|d:sect3|d:sect4|d:sect5|d:refentry|d:colophon|d:bibliodiv)">
          <ul>
            <xsl:apply-templates
              select="d:part|d:reference|d:preface|d:chapter|d:bibliography|d:appendix|d:article|d:topic|d:glossary|d:section|d:simplesect|d:sect1|d:sect2|d:sect3|d:sect4|d:sect5|d:refentry|d:colophon|d:bibliodiv"
              mode="webhelptoc">
              <xsl:with-param name="currentid" select="$currentid" />
              <xsl:with-param name="level" select="$level + 1" />
            </xsl:apply-templates>
          </ul>
        </xsl:if>
      </li>
    </xsl:if>
  </xsl:template>

  <!-- Included to remove event handler in html on the sidebar toggle button. -->
  <!-- The Header with the company logo -->
  <xsl:template name="webhelpheader">
    <xsl:param name="prev" />
    <xsl:param name="next" />
    <xsl:param name="nav.context" />

    <xsl:variable name="home" select="/*[1]" />
    <xsl:variable name="up" select="parent::*" />

    <div id="header">
      <xsl:call-template name="webhelpheader.logo" />
      <!-- Display the page title and the main heading(parent) of it -->
      <h1>
        <xsl:apply-templates select="/*[1]" mode="title.markup" />
        <xsl:choose>
          <xsl:when test="not(generate-id(.) = generate-id(/*))">
            <span>
              <xsl:apply-templates select="." mode="object.title.markup" />
            </span>
          </xsl:when>
        </xsl:choose>
      </h1>
      <!-- Prev and Next links generation -->
      <div id="navheader">
        <xsl:call-template name="user.webhelp.navheader.content" />
        <xsl:comment>
          <!-- KEEP this code. In case of neither prev nor next links are available, this will help to 
            keep the integrity of the DOM tree -->
        </xsl:comment>
        <!--xsl:with-param name="prev" select="$prev"/> <xsl:with-param name="next" select="$next"/> 
          <xsl:with-param name="nav.context" select="$nav.context"/ -->
        <div id="navLinks">
          <span>
            <a id="showHideButton" href="#" class="pointLeft" tabindex="5" title="Show/Hide TOC tree"><i class="fa fa-columns"></i> Contents
            </a>
          </span>
          <xsl:if
            test="count($prev) &gt; 0
                          or (count($up) &gt; 0
                          and generate-id($up) != generate-id($home)
                          and $navig.showtitles != 0)
                          or count($next) &gt; 0">
            <xsl:if test="count($prev)>0">
              <span>
                <a accesskey="p" id="navLinkPrevious" tabindex="5"> 
                  <xsl:attribute name="href">
                                          <xsl:call-template name="href.target">
                                              <xsl:with-param name="object"
                    select="$prev" />
                                          </xsl:call-template>
                                      </xsl:attribute>
                  <i class="fa fa-arrow-circle-left"></i>
                  <xsl:call-template name="navig.content">
                    <xsl:with-param name="direction" select="'prev'" />
                  </xsl:call-template>
                </a>
              </span>
            </xsl:if>

            <!-- "Up" link -->
            <xsl:choose>
              <xsl:when
                test="count($up)&gt;0 and generate-id($up) != generate-id($home)">
                <span>
                  <a accesskey="u" id="navLinkUp" tabindex="5">
                    <xsl:attribute name="href">
                                              <xsl:call-template name="href.target">
                                                  <xsl:with-param name="object"
                      select="$up" />
                                              </xsl:call-template>
                                          </xsl:attribute>
                    <xsl:call-template name="navig.content">
                      <xsl:with-param name="direction" select="'up'" />
                    </xsl:call-template>
                  <i class="fa fa-arrow-circle-up"></i></a>
                </span>
              </xsl:when>
            </xsl:choose>

            <xsl:if test="count($next)>0">
              <span>
                <a accesskey="n" id="navLinkNext" tabindex="5">
                  <xsl:attribute name="href">
                                          <xsl:call-template name="href.target">
                                              <xsl:with-param name="object"
                    select="$next" />
                                          </xsl:call-template>
                                      </xsl:attribute>
                  <xsl:call-template name="navig.content">
                    <xsl:with-param name="direction" select="'next'" />
                  </xsl:call-template>
                <i class="fa fa-arrow-circle-right"></i></a>
              </span>
            </xsl:if>
          </xsl:if>
        </div>
      </div>
    </div>
  </xsl:template>

<!-- No need for navigation in footer too. -->
<xsl:template name="footer.navigation"/>

  <!-- Included to load main.js as early as possible and to set
   display:block on the sidebar, which now loaded dynamically.
   Also sets a fixed width on leftnavigation to fit in the sidebar.
   And makes webhelp-currentid a dynamicallly injected class, not an id.-->
  <xsl:template name="system.head.content">
    <xsl:param name="node" select="." />
    <xsl:text>
    </xsl:text>
    <!-- The meta tag tells the IE rendering engine that it should use the latest, or edge, version of 
      the IE rendering environment;It prevents IE from entring compatibility mode. -->
    <meta http-equiv="X-UA-Compatible" content="IE=edge" />
    <xsl:text>
    </xsl:text>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <xsl:text>
    </xsl:text>
  </xsl:template>

  <!-- HTML <head> section customizations -->
  <xsl:template name="user.head.content">
    <xsl:param name="title">
      <xsl:apply-templates select="." mode="object.title.markup.textonly" />
    </xsl:param>
    <meta name="Section-title" content="{$title}" />

    <!-- <xsl:message> webhelp.tree.cookie.id = <xsl:value-of select="$webhelp.tree.cookie.id"/> +++ 
      <xsl:value-of select="count(//node())"/> $webhelp.indexer.language = <xsl:value-of select="$webhelp.indexer.language"/> 
      +++ <xsl:value-of select="count(//node())"/> </xsl:message> -->
    <script type="text/javascript">
      //The id for tree cookie
      var treeCookieId = "<xsl:value-of select="$webhelp.tree.cookie.id"/>";
      var language = "<xsl:value-of select="$webhelp.indexer.language"/>";
      var w = new Object();
      //Localization
      txt_filesfound = '<xsl:call-template name="gentext.template">
        <xsl:with-param name="name" select="'txt_filesfound'" />
        <xsl:with-param name="context" select="'webhelp'" />
      </xsl:call-template>';
      txt_enter_at_least_1_char = "<xsl:call-template name="gentext.template">
        <xsl:with-param name="name" select="'txt_enter_at_least_1_char'" />
        <xsl:with-param name="context" select="'webhelp'" />
      </xsl:call-template>";
      txt_browser_not_supported = "<xsl:call-template name="gentext.template">
        <xsl:with-param name="name" select="'txt_browser_not_supported'" />
        <xsl:with-param name="context" select="'webhelp'" />
      </xsl:call-template>";
      txt_please_wait = "<xsl:call-template name="gentext.template">
        <xsl:with-param name="name" select="'txt_please_wait'" />
        <xsl:with-param name="context" select="'webhelp'" />
      </xsl:call-template>";
      txt_results_for = "<xsl:call-template name="gentext.template">
        <xsl:with-param name="name" select="'txt_results_for'" />
        <xsl:with-param name="context" select="'webhelp'" />
      </xsl:call-template>";
    </script>

    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link href="http://netdna.bootstrapcdn.com/bootstrap/3.1.1/css/bootstrap.min.css" rel="stylesheet" />

    <!-- kasunbg: Order is important between the in-html-file css and the linked css files. Some css 
      declarations in jquery-ui-1.8.2.custom.css are over-ridden. If that's a concern, just remove the additional 
      css contents inside these default jquery css files. I thought of keeping them intact for easier maintenance! -->
    <link rel="stylesheet" type="text/css"
      href="http://ajax.googleapis.com/ajax/libs/jqueryui/1.10.4/themes/redmond/jquery-ui.min.css" />
    <link rel="stylesheet" type="text/css" href="{$webhelp.common.dir}jquery/treeview/jquery.treeview.css" />

    <link rel="stylesheet" type="text/css" href="{$webhelp.common.dir}css/positioning.css" />
    <link rel="stylesheet" type="text/css" href="{$webhelp.common.dir}css/style.css" />
    <xsl:comment>
      <xsl:text>[if IE]>
  &lt;link rel="stylesheet" type="text/css" href="../common/css/ie.css"/>
  &lt;![endif]</xsl:text>
    </xsl:comment>
    
    <link href="http://netdna.bootstrapcdn.com/font-awesome/4.0.3/css/font-awesome.css" rel="stylesheet" />
    
    <!-- browserDetect is an Oxygen addition to warn the user if they're using chrome from the file system. 
      This breaks the Oxygen search highlighting. -->
    <script type="text/javascript" src="{$webhelp.common.dir}browserDetect.js">
    </script>
    <script src="http://ajax.googleapis.com/ajax/libs/jquery/2.1.0/jquery.min.js">
    </script>
    <script src="http://ajax.googleapis.com/ajax/libs/jqueryui/1.10.4/jquery-ui.min.js">
    </script>
    <script type="text/javascript" src="{$webhelp.common.dir}jquery/jquery.highlight.js">
    </script>
    <script type="text/javascript" src="http://cdnjs.cloudflare.com/ajax/libs/jquery-cookie/1.4.0/jquery.cookie.min.js">
    </script>
    <script type="text/javascript" src="{$webhelp.common.dir}jquery/treeview/jquery.treeview.min.js">
    </script>
    <script type="text/javascript" src="http://cdnjs.cloudflare.com/ajax/libs/jquery-scrollTo/1.4.11/jquery.scrollTo.min.js">
    </script>

    <script src="http://netdna.bootstrapcdn.com/bootstrap/3.1.1/js/bootstrap.min.js"></script>

    <script type="text/javascript" src="{$webhelp.common.dir}main.js">
    </script>
    <xsl:if test="$webhelp.include.search.tab != '0'">
      <!--Scripts/css stylesheets for Search -->
      <!-- TODO: Why THREE files? There's absolutely no need for having separate files. These should 
        have been identified at the optimization phase! -->
      <script type="text/javascript" src="search/l10n.js">
      </script>
      <script type="text/javascript" src="search/htmlFileInfoList.js">
      </script>
      <script type="text/javascript" src="search/nwSearchFnt.js">
      </script>

      <!-- NOTE: Stemmer javascript files should be in format <language>_stemmer.js. For example, for 
        English(en), source should be: "search/stemmers/en_stemmer.js" For country codes, see: http://www.uspto.gov/patft/help/helpctry.htm -->
      <!--<xsl:message><xsl:value-of select="concat('search/stemmers/',$webhelp.indexer.language,'_stemmer.js')"/></xsl:message> -->
      <script type="text/javascript" src="{concat('search/stemmers/',$webhelp.indexer.language,'_stemmer.js')}">
      </script>

      <!--Index Files: Index is broken in to three equal sized(number of index items) files. This is 
        to help parallel downloading of files to make it faster. TODO: Generate webhelp index for largest docbook 
        document that can be find, and analyze the file sizes. IF the file size is still around ~50KB for a given 
        file, we should consider merging these files together. again. -->
      <script type="text/javascript" src="search/index-1.js">
      </script>
      <script type="text/javascript" src="search/index-2.js">
      </script>
      <script type="text/javascript" src="search/index-3.js">
      </script>
      <!--End of index files -->
    </xsl:if>
    <xsl:call-template name="user.webhelp.head.content" />
  </xsl:template>

  <xsl:template name="webhelptoc">
    <xsl:param name="currentid" />
    <xsl:choose>
      <xsl:when test="$rootid != ''">
        <xsl:variable name="title">
          <xsl:if test="$webhelp.autolabel=1">
            <xsl:variable name="label.markup">
              <xsl:apply-templates select="key('id',$rootid)" mode="label.markup" />
            </xsl:variable>
            <xsl:if test="normalize-space($label.markup)">
              <xsl:value-of select="concat($label.markup,$autotoc.label.separator)" />
            </xsl:if>
          </xsl:if>
          <xsl:apply-templates select="key('id',$rootid)" mode="titleabbrev.markup" />
        </xsl:variable>
        <xsl:variable name="href">
          <xsl:choose>
            <xsl:when test="$manifest.in.base.dir != 0">
              <xsl:call-template name="href.target">
                <xsl:with-param name="object" select="key('id',$rootid)" />
              </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="href.target.with.base.dir">
                <xsl:with-param name="object" select="key('id',$rootid)" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
      </xsl:when>

      <xsl:otherwise>
        <xsl:variable name="title">
          <xsl:if test="$webhelp.autolabel=1">
            <xsl:variable name="label.markup">
              <xsl:apply-templates select="/*" mode="label.markup" />
            </xsl:variable>
            <xsl:if test="normalize-space($label.markup)">
              <xsl:value-of select="concat($label.markup,$autotoc.label.separator)" />
            </xsl:if>
          </xsl:if>
          <xsl:apply-templates select="/*" mode="titleabbrev.markup" />
        </xsl:variable>
        <xsl:variable name="href">
          <xsl:choose>
            <xsl:when test="$manifest.in.base.dir != 0">
              <xsl:call-template name="href.target">
                <xsl:with-param name="object" select="/" />
              </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="href.target.with.base.dir">
                <xsl:with-param name="object" select="/" />
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>

        <div id="sidebar"> <!--#sidebar id is used for showing and hiding the side bar -->
          <div id="leftnavigation" style="padding-top:3px;">
            <div id="tabs">
              <ul>
                <li>
                  <a href="#treeDiv" style="outline:0;" tabindex="1">
                    <span class="contentsTab">
                      <i class="fa fa-list-ul"></i>
                      <xsl:call-template name="gentext.template">
                        <xsl:with-param name="name" select="'TableofContents'" />
                        <xsl:with-param name="context" select="'webhelp'" />
                      </xsl:call-template>
                    </span>
                  </a>
                </li>
                <xsl:if test="$webhelp.include.search.tab != '0'">
                  <li>
                    <a href="#searchDiv" style="outline:0;" tabindex="1" onclick="doSearch()">
                      <span class="searchTab">
                        <i class="fa fa-search"></i>
                        <xsl:call-template name="gentext.template">
                          <xsl:with-param name="name" select="'Search'" />
                          <xsl:with-param name="context" select="'webhelp'" />
                        </xsl:call-template>
                      </span>
                    </a>
                  </li>
                </xsl:if>
                <xsl:call-template name="user.webhelp.tabs.title" />
              </ul>
              <div id="treeDiv">
                <img src="{$webhelp.common.dir}images/loading.gif" alt="loading table of contents..."
                  id="tocLoading" style="display:block;" />
                <div id="ulTreeDiv" style="display:none">
                  <ul id="tree" class="filetree">
                    <xsl:apply-templates select="/*/*" mode="webhelptoc">
                      <xsl:with-param name="currentid" select="$currentid" />
                    </xsl:apply-templates>
                  </ul>
                </div>

              </div>
              <xsl:if test="$webhelp.include.search.tab != '0'">
                <div id="searchDiv">
                  <div id="search">
                    <form onsubmit="Verifie(searchForm);return false" name="searchForm" class="searchForm">
                      <div>

                        <!-- <xsl:call-template name="gentext.template"> <xsl:with-param name="name" 
                          select="'Search'"/> <xsl:with-param name="context" select="'webhelp'"/> </xsl:call-template> -->


                        <input id="textToSearch" name="textToSearch" type="search"
                          placeholder="Search" class="searchText" tabindex="1" />
                        <xsl:text disable-output-escaping="yes"> <![CDATA[&nbsp;]]> </xsl:text>
                        <input onclick="Verifie(searchForm)" type="button" class="searchButton"
                          value="Go" id="doSearch" tabindex="1" />

                      </div>
                    </form>
                  </div>
                  <div id="searchResults">
                    <center>
                    </center>
                  </div>
                  <p class="searchHighlight">
                    <a href="#" onclick="toggleHighlight()">Search Highlighter (On/Off)</a>
                  </p>
                </div>
              </xsl:if>
              <xsl:call-template name="user.webhelp.tabs.content" />
            </div>
          </div>
        </div>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="chunk-element-content">
      <xsl:param name="prev"/>
      <xsl:param name="next"/>
      <xsl:param name="nav.context"/>
      <xsl:param name="content">
          <xsl:apply-imports/>
      </xsl:param>

      <xsl:call-template name="user.preroot"/>

      <html>
          <xsl:call-template name="html.head">
              <xsl:with-param name="prev" select="$prev"/>
              <xsl:with-param name="next" select="$next"/>
          </xsl:call-template>

          <body>
              <xsl:call-template name="body.attributes"/>

              <xsl:call-template name="user.header.navigation">
                  <xsl:with-param name="prev" select="$prev"/>
                  <xsl:with-param name="next" select="$next"/>
                  <xsl:with-param name="nav.context" select="$nav.context"/>
              </xsl:call-template>

              <div id="content">

                  <xsl:copy-of select="$content"/>

                  <xsl:call-template name="user.footer.content"/>

              </div>

              <xsl:call-template name="user.footer.navigation"/>
          </body>
      </html>
      <xsl:value-of select="$chunk.append"/>
  </xsl:template>

</xsl:stylesheet>

