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
package org.neo4j.cypher.cucumber.synthesise.glue.scenario

import com.google.inject.AbstractModule
import com.google.inject.Inject
import com.google.inject.Injector
import io.cucumber.datatable.DataTable
import io.cucumber.scala.Scenario
import org.neo4j.cypher.cucumber.glue.regular.BeforeAndAfterAll
import org.neo4j.cypher.cucumber.glue.regular.DynamicExpectations
import org.neo4j.cypher.cucumber.glue.regular.Expectations
import org.neo4j.cypher.cucumber.glue.regular.GuiceObjectFactory
import org.neo4j.cypher.cucumber.glue.regular.NoOpBeforeAndAfterAll
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlError
import org.neo4j.cypher.cucumber.steps.CypherCucumberSteps.ExpectedGqlNotification
import org.neo4j.cypher.cucumber.steps.Result

import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** These steps are not safe to use with parallel execution. */
final class ScenarioRecordingSteps @Inject() (
  recorder: ScenarioRecorder
) extends CypherCucumberSteps {

  private var steps: ArrayBuffer[RecordedStep] = _

  Before { (_: Scenario) =>
    steps = new ArrayBuffer(8)
  }

  After { (scenario: Scenario) =>
    recorder.record(RecordedScenario(
      name = scenario.getName,
      uri = scenario.getUri,
      line = scenario.getLine,
      steps = steps.toSeq,
      tags = scenario.getSourceTagNames.asScala.toSet
    ))
  }

  private def add(step: RecordedStep): Unit = steps.addOne(step)
  override def parametersAre(params: Map[String, String]): Unit = add(SetParams(params))
  override def registerUserFunction(name: String): Unit = add(RegisterUserFunction(name))
  override def givenCsvFile(param: String, content: DataTable): Unit = add(CreateCsvFile(param, content))
  override def havingExecuted(cypher: String): Unit = add(HavingExecuted(cypher))
  override def executingQuery(cypher: String): Unit = add(Execute(cypher))
  override def executingControlQuery(cypher: String): Unit = add(ExecuteControl(cypher))

  override def resultShouldBe(expected: DataTable, assert: Result.Assertions): Unit =
    add(AssertResults(expected, assert))

  override def approximateResultShouldBe(expected: DataTable, rowCount: Int): Unit =
    add(AssertApproxResults(expected, rowCount))
  override def sideEffectsShouldBe(expected: DataTable): Unit = add(SideEffects(expected))
  override def errorShouldBeRaised(expected: ExpectedGqlError): Unit = add(AssertGqlError(expected))

  override def notificationsShouldBeRaised(expectedWarning: ExpectedGqlNotification): Unit =
    add(AssertGqlWarning(expectedWarning))
  override def openTransaction(): Unit = add(OpenTransaction)
  override def havingExecutedInOpenTx(cypher: String): Unit = add(HavingExecutedInOpenTx(cypher))
  override def executingQueryInOpenTx(cypher: String): Unit = add(ExecuteInOpenTx(cypher))
  override def executingControlQueryInOpenTx(cypher: String): Unit = add(ExecuteControlInOpenTx(cypher))
  override def commitOpenTx(): Unit = add(CommitTransaction)

  override def registerProcedure(signature: String, results: DataTable): Unit =
    add(RegisterProcedure(signature, results))

  override def queryLogShouldContain(expectedJsonLog: String): Unit = {}
}

object ScenarioRecordingSteps {

  // Ugly Hack to be able to retrieve the recording from the outside
  val recorder = new AtomicReference[ScenarioRecorder](null)

  class ObjectFactory extends GuiceObjectFactory {

    override val injector: Injector = GuiceObjectFactory.injector(new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[ScenarioRecorder]).toInstance(Option(recorder.get()).getOrElse(new ScenarioRecorder))
        bind(classOf[BeforeAndAfterAll]).toInstance(NoOpBeforeAndAfterAll)
        // What we use this for do not depend on the configuration
        bind(classOf[Expectations]).toInstance(new DynamicExpectations(TestConf.Default.Cypher5.conf))
      }
    })
  }
}

class ScenarioRecorder {
  private[this] val scenarios = new ConcurrentLinkedQueue[RecordedScenario]()
  def record(scenario: RecordedScenario): Unit = scenarios.add(scenario)
  def recordedScenarios: Seq[RecordedScenario] = scenarios.toArray(Array[RecordedScenario]())
}

case class RecordedScenario(
  uri: URI,
  line: Int,
  name: String,
  steps: Seq[RecordedStep],
  tags: Set[String],
  comment: Option[String] = None
) {
  def source: String = uri.toString + ":" + line
}
sealed trait RecordedStep

sealed trait QueryExecution extends RecordedStep {
  def cypher: String
}
sealed trait TransactionHandling extends RecordedStep
case object OpenTransaction extends TransactionHandling
case object CommitTransaction extends TransactionHandling
case class SetParams(params: Map[String, String]) extends RecordedStep
case class RegisterProcedure(signature: String, results: DataTable) extends RecordedStep
case class CreateCsvFile(urlParam: String, content: DataTable) extends RecordedStep
case class RegisterUserFunction(name: String) extends RecordedStep
sealed trait SetupExecution extends QueryExecution
case class HavingExecuted(override val cypher: String) extends SetupExecution
case class HavingExecutedInOpenTx(override val cypher: String) extends SetupExecution
sealed trait TestExecution extends QueryExecution
case class Execute(override val cypher: String) extends TestExecution
case class ExecuteInOpenTx(override val cypher: String) extends TestExecution
sealed trait ControlExecution extends TestExecution
case class ExecuteControl(override val cypher: String) extends ControlExecution
case class ExecuteControlInOpenTx(override val cypher: String) extends ControlExecution
sealed trait ExpectResults extends RecordedStep

case class AssertResults(expected: DataTable, assertion: Result.Assertions) extends ExpectResults {
  def rowCount: Int = expected.height() - 1
}
case class AssertApproxResults(expected: DataTable, rowCount: Int) extends ExpectResults
sealed trait ExpectError extends RecordedStep
case class AssertGqlError(expected: ExpectedGqlError) extends ExpectError

/** GQL notifications are not errors; kept out of [[ExpectError]] so filters like `steps[ExpectError](_.isEmpty)` stay meaningful. */
case class AssertGqlWarning(expected: ExpectedGqlNotification) extends RecordedStep
case class SideEffects(expected: DataTable) extends RecordedStep
case class Comment(comment: String) extends RecordedStep
