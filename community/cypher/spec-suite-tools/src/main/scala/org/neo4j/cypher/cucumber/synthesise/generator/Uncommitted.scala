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
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.containsAst
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.doNotContainAst
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.isNotCommand
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlWarning
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.CommitTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControl
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControlInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExpectError
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecuted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecutedInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.OpenTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.QueryExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedStep
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RegisterProcedure
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RegisterUserFunction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.TransactionHandling
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.expressions.PatternPart

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import scala.collection.View

class Uncommitted(val args: CucumberSalad.Ingredients) extends ScenarioGenerator with ScenarioRenderer {
  override val name: String = "uncommitted"

  private val counter = new AtomicLong(0)

  override def filter: Filter = super.filter
    .steps[ExpectError](_.isEmpty)
    .steps[AssertGqlWarning](_.isEmpty)
    .steps[TransactionHandling](_.isEmpty)
    .testQueries(_.exists(containsAst[PatternPart]))
    // SEARCH hits eventually-consistent indexes that never see uncommitted writes.
    .testQueries(_.forall(doNotContainAst[Search]))
    .queries[Execute](_.forall(doNotContainAst[InTransactionsParameters]))

  def generateScenarios(filteredScenarios: View[RecordedScenario]): IterableOnce[GeneratedScenario] = {
    filteredScenarios.map(generateScenario)
  }

  private def generateScenario(scenario: RecordedScenario): GeneratedScenario = {
    val generatedSteps = Uncommitted.transformSteps(args.parser, scenario.steps)
    val name = s"Generated ${counter.incrementAndGet()}: ${scenario.name}"
    val featurePath = Paths.get(scenario.uri.getSchemeSpecificPart)
    val comment = s"# Generated uncommited variant of ${scenario.source}: ${scenario.name}"
    GeneratedScenario(name, generatedSteps, featurePath, comment, scenario.tags)
  }
}

object Uncommitted {

  def transformSteps(parser: CachingParser, steps: Seq[RecordedStep]): Seq[RecordedStep] = {
    steps
      .flatMap {
        case e: QueryExecution if isNotCommand(parser.parse(e.cypher)) =>
          e match {
            case HavingExecuted(cypher) => Seq(HavingExecutedInOpenTx(cypher))
            case Execute(cypher)        => Seq(ExecuteInOpenTx(cypher))
            case ExecuteControl(cypher) => Seq(ExecuteControlInOpenTx(cypher))
            case step                   => Seq(step)
          }
        case e: QueryExecution       => Seq(CommitTransaction, e, OpenTransaction)
        case p: RegisterProcedure    => Seq(CommitTransaction, p, OpenTransaction)
        case f: RegisterUserFunction => Seq(CommitTransaction, f, OpenTransaction)
        case step                    => Seq(step)
      }
      .prepended(OpenTransaction)
      .appended(CommitTransaction)
  }
}
