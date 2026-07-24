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

import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.doNotContainAst
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.isNotCommand
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertApproxResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlError
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlWarning
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControl
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControlInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.QueryExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.TestExecution
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.Variable

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import scala.collection.View
import scala.util.Try

/**
 * Wraps the query under test so every variable of the original query `Q` also lives in a second, independent scope,
 * stressing the Cypher Namespacer and JVM-identity-dependent code. Both wraps below are empirically proven to
 * preserve results and side effects (including single-write for updating queries): the first copy of `Q` is gated to
 * run on zero rows, so it never executes; the second copy runs normally and its result/updates are what the
 * scenario asserts. See [[Namespacing.cypher25Wrap]] / [[Namespacing.cypher5Wrap]] for the exact shapes.
 *
 * Read, updating, and expected-error scenarios other than those already excluded by the filter are wrapped;
 * notification assertions are dropped from the generated scenario. Gated by `@fails:namespacing` / `@ignore` tags.
 */
class Namespacing(val args: CucumberSalad.Ingredients) extends ScenarioGenerator with ScenarioRenderer {
  override val name: String = "namespacing"

  private val counter = new AtomicLong(0)

  // A leading `CYPHER <version> [options]` preparser directive and EXPLAIN / PROFILE must stay ahead of the
  // namespacing wrap. Peel them off, wrap the remainder, and re-prepend -- otherwise the version prefix is lost and
  // the query runs under the wrong language.
  private val leadingCypherOptions = "(?is)^(\\s*CYPHER\\s+(?:\\d+\\b\\s*)?(?:[a-zA-Z_.]+\\s*=\\s*\\S+\\s*)*)(.*)$".r
  private val leadingExplainOrProfile = "(?is)^(\\s*(?:explain|profile)\\b\\s*)(.*)$".r

  private def wrap(cypher: String): String = cypher match {
    case leadingCypherOptions(prefix, rest)     => prefix + wrap(rest)
    case leadingExplainOrProfile(keyword, rest) => keyword + wrapStatement(rest)
    case _                                      => wrapStatement(cypher)
  }

  private def wrapStatement(cypher: String): String = {
    val parsed = args.parser.parse(cypher)
    val returning = parsed.ast match {
      case q: Query => q.isReturning
      case _        => true
    }
    Namespacing.wrapStatement(args.cypherVersion, cypher, returning, Namespacing.connectiveFor(parsed))
  }

  override def filter: Filter = super.filter
    .scenario(s => Try(Filter.steps[QueryExecution](s).foreach(e => args.parser.parse(e.cypher))).isSuccess)
    .steps[AssertGqlError](_.isEmpty)
    .steps[AssertApproxResults](_.isEmpty)
    .steps[AssertResults](_.forall(r => !r.assertion.isInstanceOf[Result.ParallelOverride]))
    .testQueries(qs => qs.nonEmpty && qs.forall(isNotCommand))
    .testQueries(_.forall(Namespacing.returnsAreSubqueryLegal))
    .testQueries(_.forall(Namespacing.isNotStandaloneCall))
    .testQueries(_.forall(doNotContainAst[UseGraph]))
    .queries[Execute](_.forall(doNotContainAst[InTransactionsParameters]))
    .scenario(s => Try(Filter.steps[TestExecution](s).foreach(e => args.parser.parse(wrap(e.cypher)))).isSuccess)

  override def generateScenarios(filteredScenarios: View[RecordedScenario]): IterableOnce[GeneratedScenario] =
    filteredScenarios.map(generateScenario)

  private def generateScenario(scenario: RecordedScenario): GeneratedScenario = {
    val steps = scenario.steps.collect {
      case Execute(c)                                   => Execute(wrap(c))
      case ExecuteInOpenTx(c)                           => ExecuteInOpenTx(wrap(c))
      case ExecuteControl(c)                            => ExecuteControl(wrap(c))
      case ExecuteControlInOpenTx(c)                    => ExecuteControlInOpenTx(wrap(c))
      case step if !step.isInstanceOf[AssertGqlWarning] => step
    }
    GeneratedScenario(
      name = s"Namespacing ${counter.incrementAndGet()}: ${scenario.name}",
      steps = steps,
      featurePath = Paths.get(scenario.uri.getSchemeSpecificPart),
      comment = s"Generated by namespacing-wrapping, based on ${scenario.source}",
      tags = scenario.tags
    )
  }
}

object Namespacing {

  /**
   * In a subquery, a RETURN item must be aliased (`expr AS name`) or a bare variable (`RETURN n`); `RETURN *` is
   * fine, but an unaliased expression like `RETURN n.foo` is illegal. A valid standalone `Q` can only have such
   * unaliased-expression items at the top level (nested subqueries would already be invalid), so rejecting ANY
   * unaliased-non-variable return item is correct and safe once `Q` is embedded inside `CALL () { Q }`.
   */
  def returnsAreSubqueryLegal(query: ParsedQuery): Boolean =
    !query.ast.folder.treeExists {
      case UnaliasedReturnItem(expr, _) => !expr.isInstanceOf[Variable]
    }

  /**
   * A standalone procedure call (`CALL db.labels`, or `CALL proc()` without YIELD) relies on top-level-only leniencies
   * and becomes illegal once embedded inside `CALL () { Q }` / a UNION branch, so it must be filtered out.
   */
  def isNotStandaloneCall(query: ParsedQuery): Boolean =
    !query.ast.folder.treeExists {
      case uc: UnresolvedCall => uc.isStandalone
    }

  /**
   * The connective for the appended gated copy must match Q's own top-level union kind: Cypher forbids mixing UNION and
   * UNION ALL within a query, so appending `UNION ALL` after a `UNION` query (or vice versa) is an illegal combination.
   */
  def connectiveFor(query: ParsedQuery): String = query.ast match {
    case _: UnionDistinct => "UNION"
    case _                => "UNION ALL"
  }

  def wrapStatement(version: CypherVersion, q: String, returning: Boolean, connective: String): String = version match {
    case CypherVersion.Cypher5 => cypher5Wrap(q, returning, connective)
    case _                     => cypher25Wrap(q)
  }

  def cypher25Wrap(q: String): String =
    s"""FILTER false
       |CALL () {
       |$q
       |}
       |FINISH
       |NEXT
       |FINISH
       |NEXT
       |$q""".stripMargin

  def cypher5Wrap(q: String, returning: Boolean, connective: String): String =
    s"""$q
       |$connective
       |WITH * WHERE false
       |CALL () {
       |$q
       |}
       |${if (returning) "RETURN *" else "FINISH"}""".stripMargin
}
