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

import org.neo4j.cypher.cucumber.glue.regular.CompositeExecutorPool
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.doNotContainAst
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.isNotCommand
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertApproxResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlWarning
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControl
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControlInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecuted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecutedInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.QueryExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UseGraph

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import scala.collection.View
import scala.util.Try

/**
 * Wraps existing scenarios so that every query is executed through a composite database: `USE comp.data` is
 * injected into each top-level leaf of the query (see `wrapStatement`), which routes it to a self-remote
 * constituent and therefore exercises the fabric planner/fragmenter/stitcher and the remote-fragment execution
 * path. See [[CompositeExecutorPool]] for the composite topology this relies on.
 *
 * Read, updating, and expected-error scenarios are all wrapped; queries that already select a graph are excluded.
 * Notification assertions are dropped from the generated scenario. Gated by
 * `@fails:composite` / `@ignore:composite` tags.
 */
class CompositeWrap(val args: CucumberSalad.Ingredients) extends ScenarioGenerator with ScenarioRenderer {
  override val name: String = "composite-wrap"

  private val counter = new AtomicLong(0)
  private val usePrefix = s"USE ${CompositeExecutorPool.Constituent}\n"

  private val includeMuted: Boolean = java.lang.Boolean.getBoolean("cypher.synthesise.composite.include_muted")
  private val MuteTag = "@fails:composite"

  // A leading `CYPHER <version> [options]` preparser directive and EXPLAIN / PROFILE must stay ahead of the injected
  // USE clause. Peel them off, wrap the remainder, and re-prepend -- otherwise the version prefix is lost and the
  // query runs under the wrong language (e.g. a `CYPHER 5`-only error scenario silently succeeds under Cypher 25).
  private val leadingCypherOptions = "(?is)^(\\s*CYPHER\\s+(?:\\d+\\b\\s*)?(?:[a-zA-Z_.]+\\s*=\\s*\\S+\\s*)*)(.*)$".r
  private val leadingExplainOrProfile = "(?is)^(\\s*(?:explain|profile)\\b\\s*)(.*)$".r

  private def wrap(cypher: String): String = cypher match {
    case leadingCypherOptions(prefix, rest)     => prefix + wrap(rest)
    case leadingExplainOrProfile(keyword, rest) => keyword + wrapStatement(rest)
    case _                                      => wrapStatement(cypher)
  }

  private def wrapStatement(cypher: String): String =
    CompositeWrap.route(args.parser, usePrefix, cypher)

  private def compatibilityBase: Filter =
    if (includeMuted)
      Filter(args.parser, Seq.empty)
        .scenario(Filter.excludeTags("@ignore", "@ignore:composite", "@ignore:generator", s"@ignore:generator:$name"))
        .scenario(s => !s.tags.exists(_.startsWith("@conf:")))
    else super.filter

  override def filter: Filter = {
    compatibilityBase
      .scenario(s => Try(Filter.steps[QueryExecution](s).foreach(e => args.parser.parse(e.cypher))).isSuccess)
      .steps[AssertApproxResults](_.isEmpty)
      .steps[AssertResults](_.forall(r => !r.assertion.isInstanceOf[Result.ParallelOverride]))
      .testQueries(qs => qs.nonEmpty && qs.forall(isNotCommand))
      .setupQueries(_.forall(q => isNotCommand(q) && isSingleGraphWrappable(q)))
      .testQueries(_.forall(isSingleGraphWrappable))
  }

  private def isSingleGraphWrappable(query: ParsedQuery): Boolean =
    doNotContainAst[UseGraph](query) && nextReturnsAreAliased(query.ast)

  private def nextReturnsAreAliased(statement: Statement): Boolean = statement match {
    case n: NextStatement => n.getReturns.forall(_.returnItems.items.forall(_.isInstanceOf[AliasedReturnItem]))
    case _                => true
  }

  override def generateScenarios(filteredScenarios: View[RecordedScenario]): IterableOnce[GeneratedScenario] =
    filteredScenarios.map(generateScenario)

  private def generateScenario(scenario: RecordedScenario): GeneratedScenario = {
    val steps = scenario.steps.collect {
      case HavingExecuted(c)                            => HavingExecuted(wrap(c))
      case HavingExecutedInOpenTx(c)                    => HavingExecutedInOpenTx(wrap(c))
      case Execute(c)                                   => Execute(wrap(c))
      case ExecuteInOpenTx(c)                           => ExecuteInOpenTx(wrap(c))
      case ExecuteControl(c)                            => ExecuteControl(wrap(c))
      case ExecuteControlInOpenTx(c)                    => ExecuteControlInOpenTx(wrap(c))
      case step if !step.isInstanceOf[AssertGqlWarning] => step
    }
    GeneratedScenario(
      name = s"Composite ${counter.incrementAndGet()}: ${scenario.name}",
      steps = steps,
      featurePath = Paths.get(scenario.uri.getSchemeSpecificPart),
      comment =
        s"Generated by composite-wrapping (USE ${CompositeExecutorPool.Constituent}), based on ${scenario.source}",
      tags =
        if (includeMuted) scenario.tags - MuteTag
        else scenario.tags
    )
  }
}

object CompositeWrap {

  /**
   * Route a query to the constituent, dispatching on its top-level construct so the produced query stays valid:
   *  - plain query / UNION: inject `USE` as the first clause of every top-level leaf (see [[queryLeafOffsets]]).
   *    The query stays at top level, so an unaliased final RETURN and bare procedure calls remain legal.
   *  - WHEN / NEXT / local definitions: wrap in `USE <constituent> { ... }` so the graph selection is inherited --
   *    a leading USE cannot precede WHEN, nor a non-first NEXT operand. This is a subquery form, so the returns
   *    must be aliased; the filter (`isSingleGraphWrappable`) guarantees that before we get here.
   *  - top-level braces: attach the selection to the existing braces (`USE <constituent> { ... }`), no double wrap.
   * Splices/concatenates into the original text, preserving parameters, quoting and backticks verbatim. Falls back
   * to a plain prefix if the query does not parse.
   */
  def route(parser: CachingParser, usePrefix: String, cypher: String): String =
    Try(parser.parse(cypher)).toOption.map { parsed =>
      parsed.ast match {
        case _: ConditionalQueryWhen | _: NextStatement | _: QueryWithLocalDefinitions =>
          s"${usePrefix.trim} {\n${parsed.statement}\n}"
        case _: TopLevelBraces =>
          s"${usePrefix.trim} ${parsed.statement}"
        case query: Query =>
          spliceAt(usePrefix, parsed.statement, queryLeafOffsets(query))
        case _ =>
          usePrefix + cypher
      }
    }.getOrElse(usePrefix + cypher)

  private def queryLeafOffsets(query: Query): Seq[Int] = query match {
    case sq: SingleQuery     => sq.clauses.headOption.map(_.position.offset).toSeq
    case tlb: TopLevelBraces => Seq(tlb.position.offset)
    case u: Union            => queryLeafOffsets(u.lhs) ++ queryLeafOffsets(u.rhs)
    case other               => Seq(other.position.offset)
  }

  private def spliceAt(usePrefix: String, statement: String, offsets: Seq[Int]): String =
    offsets.distinct.sorted.reverse.foldLeft(statement) { (acc, offset) =>
      acc.substring(0, offset) + usePrefix + acc.substring(offset)
    }
}
