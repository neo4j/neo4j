<!--
  Generates single FO document from DocBook XML source using DocBook XSL
  stylesheets.

  See xsl-stylesheets/fo/param.xsl for all parameters.

  NOTE: The URL reference to the current DocBook XSL stylesheets is
  rewritten to point to the copy on the local disk drive by the XML catalog
  rewrite directives so it doesn't need to go out to the Internet for the
  stylesheets. This means you don't need to edit the <xsl:import> elements on
  a machine by machine basis.
-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format">
<xsl:import href="http://docbook.sourceforge.net/release/xsl/current/fo/docbook.xsl"/>
<xsl:import href="common.xsl"/>
<xsl:param name="qanda.defaultlabel">qanda</xsl:param>

<xsl:param name="fop1.extensions" select="1" />
<xsl:param name="variablelist.as.blocks" select="1" />

<xsl:param name="paper.type" select="'A4'"/>
<!--
<xsl:param name="paper.type" select="'USletter'"/>
-->
<xsl:param name="hyphenate">false</xsl:param>
<!-- justify, left or right -->
<xsl:param name="alignment">left</xsl:param>

<xsl:param name="body.font.family" select="'FreeSerif'"/> <!-- neo -->
<xsl:param name="body.font.master">12</xsl:param>
<xsl:param name="body.font.size">
 <xsl:value-of select="$body.font.master"/><xsl:text>pt</xsl:text>
</xsl:param>

<xsl:param name="body.margin.bottom" select="'0.5in'"/>
<xsl:param name="body.margin.top" select="'0.5in'"/>
<xsl:param name="bridgehead.in.toc" select="0"/>

<!-- overide setting in common.xsl -->
<xsl:param name="table.frame.border.thickness" select="'2px'"/>

<!-- Default fetches image from Internet (long timeouts) -->
<xsl:param name="draft.watermark.image" select="''"/>

<!-- Line break -->
<xsl:template match="processing-instruction('asciidoc-br')">
  <fo:block/>
</xsl:template>

<!-- Horizontal ruler -->
<xsl:template match="processing-instruction('asciidoc-hr')">
  <fo:block space-after="1em">
    <fo:leader leader-pattern="rule" rule-thickness="0.5pt"  rule-style="solid" leader-length.minimum="100%"/>
  </fo:block>
</xsl:template>

<!-- Hard page break -->
<xsl:template match="processing-instruction('asciidoc-pagebreak')">
   <fo:block break-after='page'/>
</xsl:template>

<!-- Sets title to body text indent -->
<xsl:param name="body.start.indent">
  <xsl:choose>
    <xsl:when test="$fop.extensions != 0">0pt</xsl:when>
    <xsl:when test="$passivetex.extensions != 0">0pt</xsl:when>
    <xsl:otherwise>1pc</xsl:otherwise>
  </xsl:choose>
</xsl:param>
<xsl:param name="title.margin.left">
  <xsl:choose>
    <xsl:when test="$fop.extensions != 0">-1pc</xsl:when>
    <xsl:when test="$passivetex.extensions != 0">0pt</xsl:when>
    <xsl:otherwise>0pt</xsl:otherwise>
  </xsl:choose>
</xsl:param>
<xsl:param name="page.margin.bottom" select="'0.25in'"/>
<xsl:param name="page.margin.inner">
  <xsl:choose>
    <xsl:when test="$double.sided != 0">0.75in</xsl:when>
    <xsl:otherwise>0.75in</xsl:otherwise>
  </xsl:choose>
</xsl:param>
<xsl:param name="page.margin.outer">
  <xsl:choose>
    <xsl:when test="$double.sided != 0">0.5in</xsl:when>
    <xsl:otherwise>0.5in</xsl:otherwise>
  </xsl:choose>
</xsl:param>

<xsl:param name="page.margin.top" select="'0.5in'"/>
<xsl:param name="page.orientation" select="'portrait'"/>
<xsl:param name="page.width">
  <xsl:choose>
    <xsl:when test="$page.orientation = 'portrait'">
      <xsl:value-of select="$page.width.portrait"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$page.height.portrait"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:param>

<xsl:attribute-set name="monospace.properties">
  <xsl:attribute name="font-size">10pt</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="admonition.title.properties">
  <xsl:attribute name="font-size">14pt</xsl:attribute>
  <xsl:attribute name="font-weight">bold</xsl:attribute>
  <xsl:attribute name="hyphenate">false</xsl:attribute>
  <xsl:attribute name="keep-with-next.within-column">always</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="sidebar.properties" use-attribute-sets="formal.object.properties">
  <xsl:attribute name="border-style">solid</xsl:attribute>
  <xsl:attribute name="border-width">1pt</xsl:attribute>
  <xsl:attribute name="border-color">silver</xsl:attribute>
  <xsl:attribute name="background-color">#ffffee</xsl:attribute>
  <xsl:attribute name="padding-left">12pt</xsl:attribute>
  <xsl:attribute name="padding-right">12pt</xsl:attribute>
  <xsl:attribute name="padding-top">6pt</xsl:attribute>
  <xsl:attribute name="padding-bottom">6pt</xsl:attribute>
  <xsl:attribute name="margin-left">0pt</xsl:attribute>
  <xsl:attribute name="margin-right">12pt</xsl:attribute>
  <xsl:attribute name="margin-top">6pt</xsl:attribute>
  <xsl:attribute name="margin-bottom">6pt</xsl:attribute>
</xsl:attribute-set>

<!-- Only shade programlisting and screen verbatim elements -->
<xsl:param name="shade.verbatim" select="1"/>
<xsl:attribute-set name="shade.verbatim.style">
  <xsl:attribute name="background-color">
    <xsl:choose>
      <xsl:when test="self::programlisting|self::screen">#E0E0E0</xsl:when>
      <xsl:otherwise>inherit</xsl:otherwise>
    </xsl:choose>
  </xsl:attribute>
</xsl:attribute-set>

<!--
  Force XSL Stylesheets 1.72 default table breaks to be the same as the current
  version (1.74) default which (for tables) is keep-together="auto".
-->
<xsl:attribute-set name="table.properties">
  <xsl:attribute name="keep-together.within-column">auto</xsl:attribute>
</xsl:attribute-set>

<!-- Neo4j customizations -->

<xsl:param name="admon.graphics" select="1"></xsl:param>
<xsl:param name="admon.textlabel" select="1"></xsl:param>
<xsl:param name="admon.graphics.extension">.svg</xsl:param>
<xsl:param name="admon.graphics.path">images/icons/admon/</xsl:param>

<xsl:param name="toc.section.depth" select="1"></xsl:param>

<xsl:param name="callout.graphics" select="1"></xsl:param>
<xsl:param name="callout.graphics.extension">.svg</xsl:param>

<xsl:param name="navig.graphics" select="0"></xsl:param>

<xsl:param name="generate.section.toc.level" select="0"></xsl:param>

<xsl:param name="title.font.family" select="'FreeSans'"/>
<xsl:param name="monospace.font.family" select="'Inconsolata'"/>

<!-- turned off due to https://issues.apache.org/bugzilla/show_bug.cgi?id=37579 -->
<xsl:param name="ulink.footnotes" select="0"></xsl:param>

<xsl:param name="ulink.show" select="1"></xsl:param>

<xsl:attribute-set name="xref.properties">
  <xsl:attribute name="color">blue</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="normal.para.spacing">
  <xsl:attribute name="space-before.optimum">0em</xsl:attribute>
  <xsl:attribute name="space-before.minimum">0em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">0em</xsl:attribute>
  <xsl:attribute name="space-after.optimum">0.5em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.4em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">0.6em</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="section.title.properties">
  <xsl:attribute name="space-before.minimum">0.8em</xsl:attribute>
  <xsl:attribute name="space-before.optimum">1.0em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">1.2em</xsl:attribute>
  <xsl:attribute name="space-after.optimum">0.3em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.2em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">0.4em</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="list.item.spacing">
  <xsl:attribute name="space-before.optimum">0.20em</xsl:attribute>
  <xsl:attribute name="space-before.minimum">0.15em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">0.25em</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="variablelist.term.properties">
  <xsl:attribute name="space-before.optimum">0.20em</xsl:attribute>
  <xsl:attribute name="space-before.minimum">0.15em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">0.25em</xsl:attribute>
</xsl:attribute-set>

<xsl:param name="variablelist.as.blocks" select="0"></xsl:param>

<xsl:param name="header.column.widths">1 10 1</xsl:param>

<xsl:attribute-set name="formal.title.properties" use-attribute-sets="normal.para.spacing">
  <xsl:attribute name="font-size">12pt</xsl:attribute>
  <xsl:attribute name="font-weight">normal</xsl:attribute>
  <xsl:attribute name="font-style">italic</xsl:attribute>
  <xsl:attribute name="hyphenate">false</xsl:attribute>
  <xsl:attribute name="space-before.minimum">0.8em</xsl:attribute>
  <xsl:attribute name="space-before.optimum">1.0em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">1.2em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.10em</xsl:attribute>
  <xsl:attribute name="space-after.optimum">0.15em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">0.20em</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="formal.object.properties">
  <xsl:attribute name="space-before.minimum">0em</xsl:attribute>
  <xsl:attribute name="space-before.optimum">0em</xsl:attribute>
  <xsl:attribute name="space-before.maximum">0em</xsl:attribute>
  <xsl:attribute name="space-after.optimum">0.5em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.4em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">0.6em</xsl:attribute>
  <xsl:attribute name="keep-together.within-column">always</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="section.title.level1.properties">
  <xsl:attribute name="font-size">18pt</xsl:attribute>
  <xsl:attribute name="space-after.optimum">0.15em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">0.10em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">0.20em</xsl:attribute>
</xsl:attribute-set>
<xsl:attribute-set name="section.title.level2.properties">
  <xsl:attribute name="font-size">14pt</xsl:attribute>
</xsl:attribute-set>
<xsl:attribute-set name="section.title.level3.properties">
  <xsl:attribute name="font-size">
    <xsl:value-of select="$body.font.master"></xsl:value-of>
    <xsl:text>pt</xsl:text>
  </xsl:attribute>
</xsl:attribute-set>
<xsl:attribute-set name="section.title.level4.properties">
  <xsl:attribute name="font-size">
    <xsl:value-of select="$body.font.master"></xsl:value-of>
    <xsl:text>pt</xsl:text>
  </xsl:attribute>
  <xsl:attribute name="font-weight">normal</xsl:attribute>
  <xsl:attribute name="font-style">italic</xsl:attribute>
</xsl:attribute-set>
<xsl:attribute-set name="section.title.level5.properties">
  <xsl:attribute name="font-size">10pt</xsl:attribute>
</xsl:attribute-set>
<xsl:attribute-set name="section.title.level6.properties">
  <xsl:attribute name="font-size">10pt</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="section.level1.properties">
  <xsl:attribute name="break-before">page</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="component.title.properties">
  <xsl:attribute name="font-size">18pt</xsl:attribute>
  <xsl:attribute name="space-after.optimum">1.0em</xsl:attribute>
  <xsl:attribute name="space-after.minimum">1.0em</xsl:attribute>
  <xsl:attribute name="space-after.maximum">1.0em</xsl:attribute>
</xsl:attribute-set>

<xsl:template match="row/entry/simpara/literal">
  <fo:inline hyphenate="true" xsl:use-attribute-sets="monospace.properties">
    <xsl:call-template name="intersperse-with-zero-spaces">
      <xsl:with-param name="str" select="text()"/>
    </xsl:call-template>
  </fo:inline>
</xsl:template>

<xsl:attribute-set name="monospace.verbatim.properties" use-attribute-sets="verbatim.properties">
  <xsl:attribute name="font-size">8pt</xsl:attribute>
  <xsl:attribute name="line-height">11pt</xsl:attribute>
  <xsl:attribute name="background-color">#F0F0F0</xsl:attribute>
</xsl:attribute-set>

<!-- color for links -->
<xsl:attribute-set name="xref.properties">
  <xsl:attribute name="color">
    <xsl:choose>
      <xsl:when test="self::ulink">blue</xsl:when>
      <xsl:otherwise>inherit</xsl:otherwise>
    </xsl:choose>
  </xsl:attribute>
</xsl:attribute-set>

<!-- Actual space intercalation: recursive -->
<xsl:template name="intersperse-with-zero-spaces">
    <xsl:param name="str"/>
    <xsl:variable name="spacechars">
        &#x9;&#xA;&#x2000;&#x2001;&#x2002;&#x2003;&#x2004;&#x2005;&#x2006;&#x2007;&#x2008;&#x2009;&#x200A;&#x200B;
    </xsl:variable>

    <xsl:if test="string-length($str) &gt; 0">
        <xsl:variable name="c1"
                select="substring($str, 1, 1)"/>
        <xsl:variable name="c2"
                select="substring($str, 2, 1)"/>

        <xsl:value-of select="$c1"/>
        <xsl:if test="$c2 != '' and ($c1 = '.' or $c1 = ',') and
                not(contains($spacechars, $c1) or
                contains($spacechars, $c2))">
            <xsl:text>&#x200A;</xsl:text>
        </xsl:if>

        <xsl:call-template name="intersperse-with-zero-spaces">
            <xsl:with-param name="str" select="substring($str, 2)"/>
        </xsl:call-template>
    </xsl:if>
</xsl:template>

<xsl:template name="front.cover">
 <xsl:call-template name="page.sequence">
  <xsl:with-param name="master-reference">titlepage</xsl:with-param>
  <xsl:with-param name="content">
    <fo:block text-align="center" margin-top="0cm">
      <fo:external-graphic src="url(images/front-cover.svg)" content-width="21.0cm" content-height="29.7cm" scaling="uniform" />
    </fo:block>
  </xsl:with-param>
 </xsl:call-template>
</xsl:template>

<!-- included to replace [square brackets] with <angle brackets> -->
<xsl:template name="hyperlink.url.display">
  <!-- * This template is called for all external hyperlinks (ulinks and -->
  <!-- * for all simple xlinks); it determines whether the URL for the -->
  <!-- * hyperlink is displayed, and how to display it (either inline or -->
  <!-- * as a numbered footnote). -->
  <xsl:param name="url"/>
  <xsl:param name="ulink.url">
    <!-- * ulink.url is just the value of the URL wrapped in 'url(...)' -->
    <xsl:call-template name="fo-external-image">
      <xsl:with-param name="filename" select="$url"/>
    </xsl:call-template>
  </xsl:param>

  <xsl:if test="count(child::node()) != 0
                and string(.) != $url
                and $ulink.show != 0">
    <!-- * Display the URL for this hyperlink only if it is non-empty, -->
    <!-- * and the value of its content is not a URL that is the same as -->
    <!-- * URL it links to, and if ulink.show is non-zero. -->
    <xsl:choose>
      <xsl:when test="$ulink.footnotes != 0 and not(ancestor::footnote)">
        <!-- * ulink.show and ulink.footnote are both non-zero; that -->
        <!-- * means we display the URL as a footnote (instead of inline) -->
        <fo:footnote>
          <xsl:call-template name="ulink.footnote.number"/>
          <fo:footnote-body xsl:use-attribute-sets="footnote.properties">
            <fo:block>
              <xsl:call-template name="ulink.footnote.number"/>
              <xsl:text> </xsl:text>
              <fo:basic-link external-destination="{$ulink.url}">
                <xsl:value-of select="$url"/>
              </fo:basic-link>
            </fo:block>
          </fo:footnote-body>
        </fo:footnote>
      </xsl:when>
      <xsl:otherwise>
        <!-- * ulink.show is non-zero, but ulink.footnote is not; that -->
        <!-- * means we display the URL inline -->
        <fo:inline hyphenate="false">
          <!-- * put square brackets around the URL -->
          <xsl:text> &lt;</xsl:text>
          <fo:basic-link external-destination="{$ulink.url}">
            <xsl:call-template name="hyphenate-url">
              <xsl:with-param name="url" select="$url"/>
            </xsl:call-template>
          </fo:basic-link>
          <xsl:text>&gt;</xsl:text>
        </fo:inline>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:if>

</xsl:template>

<xsl:attribute-set name="informalfigure.properties">
  <xsl:attribute name="text-align">center</xsl:attribute>
</xsl:attribute-set>

<xsl:attribute-set name="figure.properties">
  <xsl:attribute name="text-align">center</xsl:attribute>
</xsl:attribute-set>

</xsl:stylesheet>

