/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.cucumber.synthesise.generator

import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Variable

/**
 * Exercises the import pathway [[CompositeSeparate]] misses — the scope-clause `CALL (x) { … }` wrap. Mode A relocates
 * a source's leading clause to the outer and imports the vars its body uses, but only when result-preserving;
 * everything else falls back to Mode B, a single-row `WITH true AS x CALL (x) { … WHERE x … }` wrap. UNION/WHEN
 * prepend that driver to every leaf ([[modeCompound]]). `RETURN *` leaves and NEXT/braces test queries are excluded.
 */
class CompositeSeparateImporting(args: CucumberSalad.Ingredients) extends CompositeSeparate(args) {
  override val name: String = "composite-separate-importing"
  override protected def namePrefix: String = "Composite-separate-importing"

  override protected def wrapDescription: String =
    s"composite-separate-importing-wrapping (CALL (x) { USE $constituent ... })"

  override protected def testWrap(cypher: String): String =
    CompositeSeparateImporting.wrap(args.parser, cypher, usePrefix)

  override def filter: Filter = separateFilter.testQueries(_.forall(CompositeSeparateImporting.isImportWrappable))
}

object CompositeSeparateImporting {

  /** A plain, UNION, or WHEN query whose every top-level leaf has a non-`RETURN *` terminal. */
  def isImportWrappable(q: ParsedQuery): Boolean = q.ast match {
    case query: Query if importWrappableShape(query) => leavesExplicitlyTerminated(query)
    case _                                           => false
  }

  // Shapes whose leaves all emit results, so `mapEachSingleQuery` drives every one and a leaf-body `USE`/`WITH` is
  // legal: plain, UNION, WHEN. NEXT is excluded — its `mapEachSingleQuery` targets only the last-in-chain operand and
  // a non-first operand can't take a leading `USE`, so it needs a dedicated wrap. Braces/local-defs left for later.
  private def importWrappableShape(query: Query): Boolean = query match {
    case _: SingleQuery          => true
    case u: Union                => importWrappableShape(u.lhs) && importWrappableShape(u.rhs)
    case w: ConditionalQueryWhen => (w.branches ++ w.default).forall(b => importWrappableShape(b.query))
    case _                       => false
  }

  // A `RETURN *` leaf would return the injected import variable as a spurious column, so every leaf must have an
  // explicit (or non-returning) terminal. `mapEachSingleQuery` is the only accessor for the top-level leaves (a plain
  // fold would also descend into subquery bodies), so it doubles as the traversal here.
  private def leavesExplicitlyTerminated(query: Query): Boolean = {
    val leaves = Seq.newBuilder[SingleQuery]
    query.mapEachSingleQuery { sq =>
      leaves += sq; sq
    }
    leaves.result().forall(_.clauses.lastOption match {
      case Some(r: Return) => !r.returnItems.includeExisting
      case _               => true
    })
  }

  def wrap(parser: CachingParser, cypher: String, usePrefix: String = CompositeUse.usePrefix): String =
    CompositeUse.peelDirectives(cypher)(wrapBody(parser, usePrefix, _))

  private def wrapBody(parser: CachingParser, usePrefix: String, cypher: String): String = {
    val parsed = parser.parse(cypher)
    parsed.ast match {
      case sq: SingleQuery         => modeA(parsed, sq, usePrefix).getOrElse(modeB(parsed, usePrefix))
      case u: Union                => modeCompound(parser, u, usePrefix)
      case w: ConditionalQueryWhen => modeCompound(parser, w, usePrefix)
      case _                       => modeB(parsed, usePrefix)
    }
  }

  /** Relocate the first clause to the outer and import the producer vars the body uses; None (→ Mode B) unless result-preserving. */
  private[generator] def modeA(
    parsed: ParsedQuery,
    sq: SingleQuery,
    usePrefix: String = CompositeUse.usePrefix
  ): Option[String] = {
    val clauses = sq.clauses
    if (clauses.size < 2) return None
    val terminal = clauses.last match {
      case r: Return => r
      case _         => return None
    }
    if (terminal.returnItems.includeExisting) return None
    val first = clauses.head
    val vp: Seq[String] = first match {
      case u: Unwind                   => Seq(u.variable.name)
      case w: With if rowPreserving(w) => w.returnItems.items.map(_.name)
      case _                           => return None
    }
    if (!firstClauseReadFree(first)) return None
    val rest = clauses.tail
    // Relocating the driver makes a collapsing/aggregating body per-row, changing the result — only take per-row bodies.
    if (!rest.forall(rowPreserving)) return None
    // A multi-row driver (UNWIND) over an updating body lets each row observe earlier rows' writes, changing the
    // result versus the original single statement — leave those to Mode B, which keeps the driver inside one CALL.
    if (first.isInstanceOf[Unwind] && sq.containsUpdates) return None
    val vout = terminal.returnItems.items.map(_.name)
    if (vp.exists(vout.contains)) return None
    val vpUsed = vp.filter(v => rest.exists(_.folder.treeExists { case x: Variable => x.name == v }))
    if (vpUsed.isEmpty) return None
    // Re-declaring an imported producer var inside the scope clause shadows the import (42N07); leave to Mode B,
    // where the body's re-projections stay ordinary linear clauses rather than scope-clause imports.
    if (redeclaresImport(rest, vpUsed)) return None

    val stmt = parsed.statement
    val off2 = clauses(1).position.offset
    val firstText = stmt.substring(0, off2).trim
    val restText = stmt.substring(off2).trim
    // Built with mkString, not stripMargin: restText is raw source and may itself contain a line starting with
    // `|` (e.g. a map comprehension broken across lines), which stripMargin would mistake for its own delimiter.
    Some(
      Seq(
        firstText,
        s"CALL (${vpUsed.map(CompositeUse.backtick).mkString(", ")}) {",
        usePrefix.trim,
        restText,
        "}",
        s"RETURN ${vout.map(CompositeUse.backtick).mkString(", ")}"
      ).mkString("\n")
    )
  }

  /** Generic single-row importing wrap: one driving row, so it never changes grouping/cardinality. */
  private[generator] def modeB(parsed: ParsedQuery, usePrefix: String = CompositeUse.usePrefix): String = {
    val importVar = freshImportName(parsed.ast)
    // mkString, not stripMargin: parsed.statement is raw source and may contain a line starting with `|`.
    val inner =
      Seq(usePrefix.trim, s"WITH * WHERE ${CompositeUse.backtick(importVar)}", parsed.statement).mkString("\n")
    val call =
      Seq(
        s"WITH true AS ${CompositeUse.backtick(importVar)}",
        s"CALL (${CompositeUse.backtick(importVar)}) {",
        inner,
        "}"
      ).mkString("\n")
    CompositeUse.outerReturn(parsed.ast, keepStar = false) match {
      case Some(ret) => s"$call\n$ret"
      case None      => call
    }
  }

  private val prettifier = Prettifier(ExpressionStringifier(alwaysBacktick = true))

  /**
   * Compound queries (UNION / WHEN): prepend the `USE <c>` + importing `WITH * WHERE x` driver to every top-level leaf
   * (a UNION branch or WHEN body — a position where a leading `USE` is legal that a statement-leading clause is not),
   * then render the rewritten AST and wrap it in the scope-clause `CALL (x) { … }`.
   */
  private def modeCompound(parser: CachingParser, q: Query, usePrefix: String): String = {
    val importVar = freshImportName(q)
    val preface =
      parser.parse(
        s"${usePrefix}WITH * WHERE ${CompositeUse.backtick(importVar)}"
      ).ast.asInstanceOf[SingleQuery].clauses
    val rewritten = q.mapEachSingleQuery(sq => sq.copy(preface ++ sq.clauses)(sq.position))
    // mkString, not stripMargin: the prettified text may contain a line starting with `|` (e.g. a rendered
    // comprehension), which stripMargin would mistake for its own delimiter.
    val call =
      Seq(
        s"WITH true AS ${CompositeUse.backtick(importVar)}",
        s"CALL (${CompositeUse.backtick(importVar)}) {",
        prettifier.asString(rewritten),
        "}"
      ).mkString("\n")
    CompositeUse.outerReturn(q, keepStar = false) match {
      case Some(ret) => s"$call\n$ret"
      case None      => call
    }
  }

  /** No DISTINCT / GROUP BY / aggregation / ORDER BY / SKIP / LIMIT — so per-driving-row evaluation matches the whole set. */
  private def rowPreserving(clause: Clause): Boolean = clause match {
    case r: Return =>
      !r.distinct && r.groupBy.isEmpty && r.orderBy.isEmpty && r.skip.isEmpty && r.limit.isEmpty &&
      !r.returnItems.items.exists(_.expression.containsAggregate) && !referencesUserDefinedAggregation(r)
    case w: With =>
      !w.distinct && w.groupBy.isEmpty && w.orderBy.isEmpty && w.skip.isEmpty && w.limit.isEmpty &&
      !w.returnItems.items.exists(_.expression.containsAggregate) && !referencesUserDefinedAggregation(w)
    case _ => true
  }

  // A UDAF only gets its aggregating flag after resolution, so `containsAggregate` misses it pre-resolution; match the
  // known ones by name instead. Currently only the feature suite's `test.aggCount`.
  private val userDefinedAggregations = Set("test.aggCount")

  private def referencesUserDefinedAggregation(clause: Clause): Boolean = clause.folder.treeExists {
    case fi: FunctionInvocation => userDefinedAggregations.exists(_.equalsIgnoreCase(fi.name))
  }

  // A pass-through re-projection (`x AS x`) of an imported var is legal inside a scope clause; only a non-pass-through
  // alias (or UNWIND) that re-binds an imported name introduces the shadowing declaration that trips 42N07.
  private def redeclaresImport(clauses: Seq[Clause], imported: Seq[String]): Boolean =
    clauses.exists {
      case w: With   => w.returnItems.items.exists(i => !i.isPassThrough && imported.contains(i.name))
      case r: Return => r.returnItems.items.exists(i => !i.isPassThrough && imported.contains(i.name))
      case u: Unwind => imported.contains(u.variable.name)
      case _         => false
    }

  private def firstClauseReadFree(clause: Clause): Boolean = !clause.folder.treeExists {
    case _: Match | _: UpdateClause | _: UnresolvedCall | _: PatternExpression | _: PatternComprehension => true
  }

  private def freshImportName(ast: Statement): String = {
    def present(n: String): Boolean = ast.folder.treeExists { case v: Variable => v.name == n }
    val base = "outerImport"
    if (!present(base)) base
    else Iterator.from(1).map(base + _).find(n => !present(n)).get
  }
}
