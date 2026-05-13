/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.LogicalVariable

/**
 * Shared helpers for the variable-checking / scoping test suites.
 *
 * Intended home for formatting, comparison, and dump utilities that are
 * useful across `VariableCheckingTestSuite`, `ScopeSurveyorReferenceTest`,
 * and ad-hoc dump probes (e.g. `NamespacerParityTestSupport.dumpSymbolGroupsAtNamespacer`).
 */
private[scoping] object VariableCheckingTestUtil {

  /** Formats a variable as `name@offset`. */
  def fmtVar(v: LogicalVariable): String = s"${v.name}@${v.position.offset}"

  /** Formats a collection of variables as `[name@offset, ...]`, sorted by offset then name. */
  def fmtUses(us: Iterable[LogicalVariable]): String =
    us.toSeq.sortBy(v => (v.position.offset, v.name)).map(fmtVar).mkString("[", ", ", "]")

  /** Formats an optional rename string as `'<value>'` or `-` when absent. */
  def fmtRen(r: Option[String]): String = r.map(s => s"'$s'").getOrElse("-")

  /** One row of the diff table: `(decl, renaming, uses)`, each pre-formatted. */
  type SymbolGroupRow = (String, String, String)

  /**
   * Renders a side-by-side table of `Actual` vs `Expected` rows.
   *
   * Rows are aligned by index. A row whose triple differs between sides
   * (including size-mismatch padding) is marked with `!`. The shorter side
   * is padded with `-`. Column widths adapt to the widest value across
   * both sides, including the header labels.
   */
  def sideBySideSymbolGroupTable(
    actualRows: Seq[SymbolGroupRow],
    expectedRows: Seq[SymbolGroupRow],
    headerLabels: (String, String) = ("ACTUAL", "EXPECTED"),
    colLabels: (String, String, String) = ("decl", "renaming", "uses")
  ): String = {
    val (colDecl, colRen, colUses) = colLabels
    val (hdrActual, hdrExpected) = headerLabels

    val declW = (actualRows.map(_._1) ++ expectedRows.map(_._1) ++ Seq(colDecl)).map(_.length).max
    val renW = (actualRows.map(_._2) ++ expectedRows.map(_._2) ++ Seq(colRen)).map(_.length).max
    val usesW = (actualRows.map(_._3) ++ expectedRows.map(_._3) ++ Seq(colUses)).map(_.length).max

    def renderSide(row: Option[SymbolGroupRow], mismatch: Boolean): String = row match {
      case Some((d, r, u)) =>
        val marker = if (mismatch) "!" else " "
        s"$marker ${d.padTo(declW, ' ')}  ${r.padTo(renW, ' ')}  ${u.padTo(usesW, ' ')}"
      case None =>
        val dash = "-"
        s"  ${dash.padTo(declW, ' ')}  ${dash.padTo(renW, ' ')}  ${dash.padTo(usesW, ' ')}"
    }

    val colsRow =
      s"     ${colDecl.padTo(declW, ' ')}  ${colRen.padTo(renW, ' ')}  ${colUses.padTo(usesW, ' ')}"
    val labelWidth = colsRow.length
    val header =
      s"${hdrActual.padTo(labelWidth, ' ')}  |  ${hdrExpected.padTo(labelWidth, ' ')}\n" +
        s"$colsRow  |  $colsRow"

    val maxRows = math.max(actualRows.size, expectedRows.size)
    val rowLines = (0 until maxRows).map { i =>
      val a = actualRows.lift(i)
      val e = expectedRows.lift(i)
      val mismatch = a != e
      f"$i%2d ${renderSide(a, mismatch)}  |  ${renderSide(e, mismatch)}"
    }

    (Seq(header) ++ rowLines).mkString("\n")
  }

  /**
   * Render a compact tree-summary diff of two `WorkingScope`s.
   *
   * Each scope becomes one line: `ScopeKind[AstClass]  decl={…}  in={…}  out={…}  ref=N  kids=M`.
   * Trees are flattened depth-first and walked index-aligned. Rows where the summaries
   * differ are marked with `!`. The output fits on screen even for large queries and is
   * intended as the "where does the tree differ?" overview before drilling into
   * `pprint.apply` or `unifiedLineDiff`.
   *
   * Indentation uses `·  ` per depth level (plain dots, no box-drawing characters —
   * the column widths adapt to the indented tree so unicode weirdness is avoided).
   *
   * Alignment is naive (no LCS). When the two trees have different shapes — different
   * child counts at some node — rows drift after the structural divergence; the drift
   * itself is a useful signal but the per-line markers past that point should be
   * interpreted with the tree structure in mind.
   */
  def sideBySideScopeTreeDiff(
    actual: WorkingScope,
    expected: WorkingScope
  ): String = {
    def varSet(vs: Iterable[LogicalVariable]): String =
      vs.toSeq.sortBy(v => (v.position.offset, v.name)).map(fmtVar).mkString("{", ",", "}")

    def line(scope: WorkingScope, depth: Int): String = {
      val indent = "·  " * depth
      val kind = scope.getClass.getSimpleName
      val ast = scope.astNode.getClass.getSimpleName
      val decl = varSet(scope.declared.variables ++ scope.declared.constants)
      val in = varSet(scope.incoming.allSymbols)
      val out = varSet(scope.outgoing.variables ++ scope.outgoing.constants)
      val refs = scope.referenced.references.size
      val kids = scope.children.size
      s"$indent$kind[$ast]  decl=$decl  in=$in  out=$out  ref=$refs  kids=$kids"
    }

    def flatten(scope: WorkingScope, depth: Int): Seq[String] =
      line(scope, depth) +: scope.children.flatMap(flatten(_, depth + 1))

    val actualRows = flatten(actual, 0)
    val expectedRows = flatten(expected, 0)
    val maxRows = math.max(actualRows.size, expectedRows.size)
    val colW = (actualRows ++ expectedRows).map(_.length).maxOption.getOrElse(0)

    val header = {
      val a = "ACTUAL".padTo(colW, ' ')
      val e = "EXPECTED"
      f"     $a  |  $e"
    }

    val rowLines = (0 until maxRows).map { i =>
      val a = actualRows.lift(i).getOrElse("-")
      val e = expectedRows.lift(i).getOrElse("-")
      val marker = if (a != e) "!" else " "
      f"$i%3d $marker ${a.padTo(colW, ' ')}  |  $e"
    }

    (Seq(header) ++ rowLines).mkString("\n")
  }

  /**
   * Render a unified line-diff of `actual` vs `expected`.
   *
   * Splits both on `\n` and walks them index-aligned. Matching lines are prefixed
   * with `    ` (4 spaces). Differing lines are shown as two lines:
   *
   * {{{
   *   - <actual>
   *   + <expected>
   * }}}
   *
   * Unmatched tail lines (size mismatch) are shown with only `- ` or `+ `.
   *
   * Best for comparing structurally similar multi-line outputs — e.g. two
   * `pprint.apply(workingScope)` dumps that differ in only a handful of fields.
   * Alignment is naive (no LCS), so large insertions/deletions can cause a
   * drift; in that case the diff is still informative but not minimal.
   */
  def unifiedLineDiff(actual: String, expected: String): String = {
    val aLines = actual.split("\n", -1)
    val eLines = expected.split("\n", -1)
    val maxLen = math.max(aLines.length, eLines.length)
    val buf = new StringBuilder
    var i = 0
    while (i < maxLen) {
      val a = if (i < aLines.length) Some(aLines(i)) else None
      val e = if (i < eLines.length) Some(eLines(i)) else None
      (a, e) match {
        case (Some(as), Some(es)) if as == es =>
          buf.append("    ").append(as).append('\n')
        case (Some(as), Some(es)) =>
          buf.append("  - ").append(as).append('\n')
          buf.append("  + ").append(es).append('\n')
        case (Some(as), None) =>
          buf.append("  - ").append(as).append('\n')
        case (None, Some(es)) =>
          buf.append("  + ").append(es).append('\n')
        case _ => ()
      }
      i += 1
    }
    buf.toString
  }
}
