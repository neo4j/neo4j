<?xml version="1.0" encoding="iso-8859-1"?>
<!--
dblatex(1) XSL user stylesheet for asciidoc(1).
See dblatex(1) -p option.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <!-- TOC links in the titles, and in blue. -->
  <xsl:param name="latex.hyperparam">colorlinks,linkcolor=blue,pdfstartview=FitH</xsl:param>
  <xsl:param name="doc.publisher.show">1</xsl:param>
  <xsl:param name="doc.lot.show"></xsl:param>
  <xsl:param name="term.breakline">1</xsl:param>
  <xsl:param name="doc.collab.show">0</xsl:param>
  <xsl:param name="doc.section.depth">3</xsl:param>
  <xsl:param name="table.in.float">0</xsl:param>
  <xsl:param name="monoseq.hyphenation">0</xsl:param>
  <xsl:param name="latex.output.revhistory">1</xsl:param>

  <!-- This doesn't work, don't know why, see:
  http://dblatex.sourceforge.net/html/manual/apas03.html
  ./docbook-xsl/common.xsl
  -->
  <!--
  <xsl:param name="doc.toc.show">
    <xsl:choose>
      <xsl:when test="/processing-instruction('asciidoc-toc')">
1
      </xsl:when>
      <xsl:otherwise>
0
      </xsl:otherwise>
    </xsl:choose>
  </xsl:param>
  <xsl:param name="doc.lot.show">
    <xsl:choose>
      <xsl:when test="/book">
figure,table,equation,example
      </xsl:when>
    </xsl:choose>
  </xsl:param>
  -->
  <xsl:param name="doc.toc.show">1</xsl:param>

  <!--
    Override default literallayout template.
    See `./dblatex/dblatex-readme.txt`.
  -->
  <xsl:template match="address|literallayout[@class!='monospaced']">
    <xsl:text>\begin{alltt}</xsl:text>
    <xsl:text>&#10;\normalfont{}&#10;</xsl:text>
    <xsl:apply-templates/>
    <xsl:text>&#10;\end{alltt}</xsl:text>
  </xsl:template>

  <xsl:template match="processing-instruction('asciidoc-pagebreak')">
    <!-- force hard pagebreak, varies from 0(low) to 4(high) -->
    <xsl:text>\pagebreak[4] </xsl:text>
    <xsl:apply-templates />
    <xsl:text>&#10;</xsl:text>
  </xsl:template>

  <xsl:template match="processing-instruction('asciidoc-br')">
    <xsl:text>\newline&#10;</xsl:text>
  </xsl:template>

  <xsl:template match="processing-instruction('asciidoc-hr')">
    <!-- draw a 444 pt line (centered) -->
    <xsl:text>\begin{center}&#10; </xsl:text>
    <xsl:text>\line(1,0){444}&#10; </xsl:text>
    <xsl:text>\end{center}&#10; </xsl:text>
  </xsl:template>

</xsl:stylesheet>

