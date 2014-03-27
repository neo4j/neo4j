<?xml version="1.0" encoding="ASCII"?>
<xsl:stylesheet xmlns="http://www.w3.org/1999/xhtml" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:d="http://docbook.org/ns/docbook"
 version="1.0" exclude-result-prefixes="xsl d">

<xsl:template name="empty.table.cell">
  <xsl:param name="colnum" select="0"/>

  <xsl:variable name="rowsep">
    <xsl:choose>
      <!-- If this is the last row, rowsep never applies. -->
      <xsl:when test="not(ancestor-or-self::d:row[1]/following-sibling::d:row                           or ancestor-or-self::d:thead/following-sibling::d:tbody                           or ancestor-or-self::d:tbody/preceding-sibling::d:tfoot)">
        <xsl:value-of select="0"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="inherited.table.attribute">
          <xsl:with-param name="entry" select="NOT-AN-ELEMENT-NAME"/>
          <xsl:with-param name="row" select="ancestor-or-self::d:row[1]"/>
          <xsl:with-param name="colnum" select="$colnum"/>
          <xsl:with-param name="attribute" select="'rowsep'"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="colsep">
    <xsl:choose>
      <!-- If this is the last column, colsep never applies. -->
      <xsl:when test="number($colnum) &gt;= ancestor::d:tgroup/@cols">0</xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="inherited.table.attribute">
          <xsl:with-param name="entry" select="NOT-AN-ELEMENT-NAME"/>
          <xsl:with-param name="row" select="ancestor-or-self::d:row[1]"/>
          <xsl:with-param name="colnum" select="$colnum"/>
          <xsl:with-param name="attribute" select="'colsep'"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <td>
    <xsl:text>&#160;</xsl:text>
  </td>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template name="border" />

<!-- ==================================================================== -->

<xsl:template match="d:tgroup" name="tgroup">
  <xsl:if test="not(@cols) or @cols = '' or string(number(@cols)) = 'NaN'">
    <xsl:message terminate="yes">
      <xsl:text>Error: CALS tables must specify the number of columns.</xsl:text>
    </xsl:message>
  </xsl:if>

  <xsl:variable name="summary">
    <xsl:call-template name="pi.dbhtml_table-summary"/>
  </xsl:variable>

  <table>
    <xsl:attribute name="cellspacing">
      <xsl:value-of select="0" />
    </xsl:attribute>

    <xsl:attribute name="cellpadding">
      <xsl:value-of select="0" />
    </xsl:attribute>

    <xsl:variable name="colgroup">
      <colgroup>
        <xsl:call-template name="generate.colgroup">
          <xsl:with-param name="cols" select="@cols"/>
        </xsl:call-template>
      </colgroup>
    </xsl:variable>

    <xsl:apply-templates select="d:thead"/>
    <xsl:apply-templates select="d:tfoot"/>
    <xsl:apply-templates select="d:tbody"/>

    <xsl:if test=".//d:footnote|../d:title//d:footnote">
      <tbody class="footnotes">
        <tr>
          <td colspan="{@cols}">
            <xsl:apply-templates select=".//d:footnote|../d:title//d:footnote" mode="table.footnote.mode"/>
          </td>
        </tr>
      </tbody>
    </xsl:if>
  </table>
</xsl:template>

<xsl:template match="d:tgroup/processing-instruction('dbhtml')"/>

<xsl:template match="d:colspec"/>

<xsl:template match="d:spanspec"/>

<xsl:template match="d:thead|d:tfoot">
  <xsl:element name="{local-name(.)}" namespace="http://www.w3.org/1999/xhtml">
    <xsl:choose>
      <!-- recurse on rows only if @morerows is present -->
      <xsl:when test="d:row/d:entry/@morerows|d:row/d:entrytbl/@morerows">
        <xsl:apply-templates select="d:row[1]">
          <xsl:with-param name="spans">
            <xsl:call-template name="blank.spans">
              <xsl:with-param name="cols" select="../@cols"/>
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="browserows" select="'recurse'"/>
        </xsl:apply-templates>
      </xsl:when>
      <xsl:otherwise>
        <xsl:apply-templates select="d:row">
          <xsl:with-param name="spans">
            <xsl:call-template name="blank.spans">
              <xsl:with-param name="cols" select="../@cols"/>
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="browserows" select="'loop'"/>
        </xsl:apply-templates>
      </xsl:otherwise>
    </xsl:choose>

  </xsl:element>
</xsl:template>

<xsl:template match="d:tbody">
  <tbody>
    <xsl:choose>
      <xsl:when test="d:row/d:entry/@morerows|d:row/d:entrytbl/@morerows">
        <xsl:apply-templates select="d:row[1]">
          <xsl:with-param name="spans">
            <xsl:call-template name="blank.spans">
              <xsl:with-param name="cols" select="../@cols"/>
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="browserows" select="'recurse'"/>
        </xsl:apply-templates>
      </xsl:when>
      <xsl:otherwise>
        <xsl:apply-templates select="d:row">
          <xsl:with-param name="spans">
            <xsl:call-template name="blank.spans">
              <xsl:with-param name="cols" select="../@cols"/>
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="browserows" select="'loop'"/>
        </xsl:apply-templates>
      </xsl:otherwise>
    </xsl:choose>

  </tbody>
</xsl:template>

<xsl:template match="d:row">
  <xsl:param name="spans"/>
  <xsl:param name="browserows"/>

  <xsl:choose>
    <xsl:when test="contains($spans, '0')">
      <xsl:call-template name="normal-row">
        <xsl:with-param name="spans" select="$spans"/>
        <xsl:with-param name="browserows" select="$browserows"/>
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <!--
      <xsl:message>
        <xsl:text>Ignoring row: </xsl:text>
        <xsl:value-of select="$spans"/>
        <xsl:text> = </xsl:text>
        <xsl:call-template name="consume-row">
          <xsl:with-param name="spans" select="$spans"/>
        </xsl:call-template>
      </xsl:message>
      -->

      <xsl:if test="normalize-space(.//text()) != ''">
        <xsl:message>Warning: overlapped row contains content!</xsl:message>
      </xsl:if>

      <tr><xsl:comment> This row intentionally left blank </xsl:comment></tr>

      <xsl:if test="$browserows = 'recurse'">
        <xsl:apply-templates select="following-sibling::d:row[1]">
          <xsl:with-param name="spans">
            <xsl:call-template name="consume-row">
              <xsl:with-param name="spans" select="$spans"/>
            </xsl:call-template>
          </xsl:with-param>
          <xsl:with-param name="browserows" select="$browserows"/>
        </xsl:apply-templates>
      </xsl:if>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="normal-row">
  <xsl:param name="spans"/>
  <xsl:param name="browserows"/>

  <xsl:variable name="class">
    <xsl:if test="processing-instruction('dbhtml')">
      <xsl:call-template name="pi.dbhtml_class"/>
    </xsl:if>
  </xsl:variable>

  <tr>
    <xsl:call-template name="id.attribute"/>
    <xsl:call-template name="tr.attributes">
      <xsl:with-param name="rownum">
        <xsl:number from="d:tgroup" count="d:row"/>
      </xsl:with-param>
    </xsl:call-template>

    <xsl:if test="$class != ''">
      <xsl:attribute name="class">
        <xsl:value-of select="$class"/>
      </xsl:attribute>
    </xsl:if>

    <xsl:apply-templates select="(d:entry|d:entrytbl)[1]">
      <xsl:with-param name="spans" select="$spans"/>
    </xsl:apply-templates>
  </tr>

  <xsl:if test="$browserows = 'recurse'">
    <xsl:if test="following-sibling::d:row">
      <xsl:variable name="nextspans">
        <xsl:apply-templates select="(d:entry|d:entrytbl)[1]" mode="span">
          <xsl:with-param name="spans" select="$spans"/>
        </xsl:apply-templates>
      </xsl:variable>
  
      <xsl:apply-templates select="following-sibling::d:row[1]">
        <xsl:with-param name="spans" select="$nextspans"/>
        <xsl:with-param name="browserows" select="$browserows"/>
      </xsl:apply-templates>
    </xsl:if>
  </xsl:if>
</xsl:template>

<xsl:template match="d:entry|d:entrytbl" name="entry">
  <xsl:param name="col">
    <xsl:choose>
      <xsl:when test="@revisionflag">
        <xsl:number from="d:row"/>
      </xsl:when>
      <xsl:otherwise>1</xsl:otherwise>
    </xsl:choose>
  </xsl:param>

  <xsl:param name="spans"/>

  <xsl:variable name="cellgi">
    <xsl:choose>
      <xsl:when test="ancestor::d:thead">th</xsl:when>
      <xsl:when test="ancestor::d:tfoot">th</xsl:when>
      <xsl:when test="ancestor::d:tbody and                        (ancestor::d:table[@rowheader = 'firstcol'] or                       ancestor::d:informaltable[@rowheader = 'firstcol']) and                       ancestor-or-self::d:entry[1][count(preceding-sibling::d:entry) = 0]">
        <xsl:text>th</xsl:text>
      </xsl:when>
      <xsl:otherwise>td</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="empty.cell" select="count(node()) = 0"/>

  <xsl:variable name="named.colnum">
    <xsl:call-template name="entry.colnum"/>
  </xsl:variable>

  <xsl:variable name="entry.colnum">
    <xsl:choose>
      <xsl:when test="$named.colnum &gt; 0">
        <xsl:value-of select="$named.colnum"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$col"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="entry.colspan">
    <xsl:choose>
      <xsl:when test="@spanname or @namest">
        <xsl:call-template name="calculate.colspan"/>
      </xsl:when>
      <xsl:otherwise>1</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="following.spans">
    <xsl:call-template name="calculate.following.spans">
      <xsl:with-param name="colspan" select="$entry.colspan"/>
      <xsl:with-param name="spans" select="$spans"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="rowsep">
    <xsl:choose>
      <!-- If this is the last row, rowsep never applies. -->
      <xsl:when test="ancestor::d:entrytbl                       and not (ancestor-or-self::d:row[1]/following-sibling::d:row)                       and not (ancestor::d:thead)">
        <xsl:value-of select="0"/>
      </xsl:when>
      <xsl:when test="not(ancestor-or-self::d:row[1]/following-sibling::d:row                           or ancestor-or-self::d:thead/following-sibling::d:tbody                           or ancestor-or-self::d:tbody/preceding-sibling::d:tfoot)">
        <xsl:value-of select="0"/>
      </xsl:when>
      <xsl:when test="@morerows and not(@morerows &lt;                   count(ancestor-or-self::d:row[1]/following-sibling::d:row))">
        <xsl:value-of select="0"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="inherited.table.attribute">
          <xsl:with-param name="entry" select="."/>
          <xsl:with-param name="colnum" select="$entry.colnum"/>
          <xsl:with-param name="attribute" select="'rowsep'"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="colsep">
    <xsl:choose>
      <!-- If this is the last column, colsep never applies. -->
      <xsl:when test="$following.spans = ''">0</xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="inherited.table.attribute">
          <xsl:with-param name="entry" select="."/>
          <xsl:with-param name="colnum" select="$entry.colnum"/>
          <xsl:with-param name="attribute" select="'colsep'"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="$spans != '' and not(starts-with($spans,'0:'))">
      <xsl:call-template name="entry">
        <xsl:with-param name="col" select="$col+1"/>
        <xsl:with-param name="spans" select="substring-after($spans,':')"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:when test="number($entry.colnum) &gt; $col">
      <xsl:call-template name="empty.table.cell"/>
      <xsl:call-template name="entry">
        <xsl:with-param name="col" select="$col+1"/>
        <xsl:with-param name="spans" select="substring-after($spans,':')"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <xsl:element name="{$cellgi}" namespace="http://www.w3.org/1999/xhtml">
        <xsl:call-template name="id.attribute"/>

        <xsl:call-template name="locale.html.attributes"/>
        <xsl:choose>
          <xsl:when test="$entry.propagates.style != 0 and @role">
            <xsl:apply-templates select="." mode="class.attribute">
              <xsl:with-param name="class" select="@role"/>
            </xsl:apply-templates>
          </xsl:when>
          <xsl:otherwise>
            <xsl:apply-templates select="." mode="class.attribute">
              <xsl:with-param name="class" select="''"/>
            </xsl:apply-templates>
          </xsl:otherwise>
        </xsl:choose>

        <xsl:if test="$show.revisionflag and @revisionflag">
          <xsl:attribute name="class">
            <xsl:value-of select="@revisionflag"/>
          </xsl:attribute>
        </xsl:if>

        <xsl:if test="@morerows &gt; 0">
          <xsl:attribute name="rowspan">
            <xsl:value-of select="1+@morerows"/>
          </xsl:attribute>
        </xsl:if>

        <xsl:if test="$entry.colspan &gt; 1">
          <xsl:attribute name="colspan">
            <xsl:value-of select="$entry.colspan"/>
          </xsl:attribute>
        </xsl:if>

        <xsl:if test="not(preceding-sibling::*) and (ancestor::d:row[1]/@id or ancestor::d:row[1]/@xml:id)">
          <xsl:call-template name="anchor">
            <xsl:with-param name="node" select="ancestor::d:row[1]"/>
          </xsl:call-template>
        </xsl:if>

        <xsl:call-template name="anchor"/>

        <xsl:choose>
          <xsl:when test="$empty.cell">
            <xsl:text>&#160;</xsl:text>
          </xsl:when>
          <xsl:when test="self::d:entrytbl">
            <xsl:call-template name="tgroup"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:apply-templates/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:element>

      <xsl:choose>
        <xsl:when test="following-sibling::d:entry|following-sibling::d:entrytbl">
          <xsl:apply-templates select="(following-sibling::d:entry                                        |following-sibling::d:entrytbl)[1]">
            <xsl:with-param name="col" select="$col+$entry.colspan"/>
            <xsl:with-param name="spans" select="$following.spans"/>
          </xsl:apply-templates>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="finaltd">
            <xsl:with-param name="spans" select="$following.spans"/>
            <xsl:with-param name="col" select="$col+$entry.colspan"/>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="d:entry|d:entrytbl" name="sentry" mode="span">
  <xsl:param name="col" select="1"/>
  <xsl:param name="spans"/>

  <xsl:variable name="entry.colnum">
    <xsl:call-template name="entry.colnum"/>
  </xsl:variable>

  <xsl:variable name="entry.colspan">
    <xsl:choose>
      <xsl:when test="@spanname or @namest">
        <xsl:call-template name="calculate.colspan"/>
      </xsl:when>
      <xsl:otherwise>1</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="following.spans">
    <xsl:call-template name="calculate.following.spans">
      <xsl:with-param name="colspan" select="$entry.colspan"/>
      <xsl:with-param name="spans" select="$spans"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="$spans != '' and not(starts-with($spans,'0:'))">
      <xsl:value-of select="substring-before($spans,':')-1"/>
      <xsl:text>:</xsl:text>
      <xsl:call-template name="sentry">
        <xsl:with-param name="col" select="$col+1"/>
        <xsl:with-param name="spans" select="substring-after($spans,':')"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:when test="number($entry.colnum) &gt; $col">
      <xsl:text>0:</xsl:text>
      <xsl:call-template name="sentry">
        <xsl:with-param name="col" select="$col + 1"/>
        <xsl:with-param name="spans" select="substring-after($spans,':')"/>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <xsl:call-template name="copy-string">
        <xsl:with-param name="count" select="$entry.colspan"/>
        <xsl:with-param name="string">
          <xsl:choose>
            <xsl:when test="@morerows">
              <xsl:value-of select="@morerows"/>
            </xsl:when>
            <xsl:otherwise>0</xsl:otherwise>
          </xsl:choose>
          <xsl:text>:</xsl:text>
        </xsl:with-param>
      </xsl:call-template>

      <xsl:choose>
        <xsl:when test="following-sibling::d:entry|following-sibling::d:entrytbl">
          <xsl:apply-templates select="(following-sibling::d:entry                                         |following-sibling::d:entrytbl)[1]" mode="span">
            <xsl:with-param name="col" select="$col+$entry.colspan"/>
            <xsl:with-param name="spans" select="$following.spans"/>
          </xsl:apply-templates>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="sfinaltd">
            <xsl:with-param name="spans" select="$following.spans"/>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="generate.colgroup">
  <xsl:param name="cols" select="1"/>
  <xsl:param name="count" select="1"/>
  <xsl:choose>
    <xsl:when test="$count &gt; $cols"/>
    <xsl:otherwise>
      <xsl:call-template name="generate.col">
        <xsl:with-param name="countcol" select="$count"/>
      </xsl:call-template>
      <xsl:call-template name="generate.colgroup">
        <xsl:with-param name="cols" select="$cols"/>
        <xsl:with-param name="count" select="$count+1"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="generate.col">
  <xsl:param name="countcol">1</xsl:param>
  <xsl:param name="colspecs" select="./d:colspec"/>
  <xsl:param name="count">1</xsl:param>
  <xsl:param name="colnum">1</xsl:param>

  <xsl:choose>
    <xsl:when test="$count&gt;count($colspecs)">
      <col/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:variable name="colspec" select="$colspecs[$count=position()]"/>
      <xsl:variable name="colspec.colnum">
        <xsl:choose>
          <xsl:when test="$colspec/@colnum">
            <xsl:value-of select="$colspec/@colnum"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="$colnum"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:variable>

      <xsl:choose>
        <xsl:when test="$colspec.colnum=$countcol">
          <col>
            <xsl:choose>
              <xsl:when test="$colspec/@colwidth                             and $use.extensions != 0                             and $tablecolumns.extension != 0">
                <xsl:attribute name="width">
                  <xsl:choose>
                    <xsl:when test="normalize-space($colspec/@colwidth) = '*'">
                      <xsl:value-of select="'1*'"/>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:value-of select="$colspec/@colwidth"/>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:attribute>
              </xsl:when>
              <!-- pass through to HTML if no * in colspecs -->
              <xsl:when test="$colspec/@colwidth and                              not($colspec/parent::*/d:colspec/@colwidth[contains(.,'*')])">
                <xsl:attribute name="width">
                  <xsl:choose>
                    <xsl:when test="normalize-space($colspec/@colwidth) = '*'">
                      <xsl:value-of select="'1*'"/>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:value-of select="$colspec/@colwidth"/>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:attribute>
              </xsl:when>
            </xsl:choose>

            <xsl:choose>
              <xsl:when test="$colspec/@align">
                <xsl:attribute name="align">
                  <xsl:value-of select="$colspec/@align"/>
                </xsl:attribute>
              </xsl:when>
              <!-- Suggested by Pavel ZAMPACH <zampach@nemcb.cz> -->
              <xsl:when test="$colspecs/ancestor::d:tgroup/@align">
                <xsl:attribute name="align">
                  <xsl:value-of select="$colspecs/ancestor::d:tgroup/@align"/>
                </xsl:attribute>
              </xsl:when>
            </xsl:choose>

            <xsl:if test="$colspec/@colname">
              <xsl:attribute name="class">
                <xsl:value-of select="$colspec/@colname"/>
              </xsl:attribute>
            </xsl:if>
          </col>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="generate.col">
            <xsl:with-param name="countcol" select="$countcol"/>
            <xsl:with-param name="colspecs" select="$colspecs"/>
            <xsl:with-param name="count" select="$count+1"/>
            <xsl:with-param name="colnum">
              <xsl:choose>
                <xsl:when test="$colspec/@colnum">
                  <xsl:value-of select="$colspec/@colnum + 1"/>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="$colnum + 1"/>
                </xsl:otherwise>
              </xsl:choose>
            </xsl:with-param>
           </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="colspec.colwidth">
  <!-- when this macro is called, the current context must be an entry -->
  <xsl:param name="colname"/>
  <!-- .. = row, ../.. = thead|tbody, ../../.. = tgroup -->
  <xsl:param name="colspecs" select="../../../../d:tgroup/d:colspec"/>
  <xsl:param name="count">1</xsl:param>
  <xsl:choose>
    <xsl:when test="$count&gt;count($colspecs)"/>
    <xsl:otherwise>
      <xsl:variable name="colspec" select="$colspecs[$count=position()]"/>
      <xsl:choose>
        <xsl:when test="$colspec/@colname=$colname">
          <xsl:value-of select="$colspec/@colwidth"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="colspec.colwidth">
            <xsl:with-param name="colname" select="$colname"/>
            <xsl:with-param name="colspecs" select="$colspecs"/>
            <xsl:with-param name="count" select="$count+1"/>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<!-- ====================================================================== -->

<xsl:template name="tr.attributes">
  <xsl:param name="row" select="."/>
  <xsl:param name="rownum" select="0"/>

  <!-- by default, do nothing. But you might want to say:

  <xsl:if test="$rownum mod 2 = 0">
    <xsl:attribute name="class">oddrow</xsl:attribute>
  </xsl:if>

  -->
</xsl:template>

</xsl:stylesheet>
