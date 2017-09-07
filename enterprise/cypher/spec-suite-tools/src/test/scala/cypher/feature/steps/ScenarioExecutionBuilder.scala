/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.steps

import java.util

import cypher.cucumber.BlacklistPlugin.blacklisted
import cypher.feature.parser.SideEffects
import cypher.feature.parser.SideEffects.Values
import org.neo4j.graphdb.{Result, Transaction}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.opencypher.tools.tck.InvalidFeatureFormatException
import org.scalatest.Matchers._

import scala.util.Try

class ScenarioExecutionBuilder {

  import scala.collection.JavaConverters._

  var skip: Boolean = _
  var name: String = _
  def register(name: String, skip: Boolean): Unit = {
    this.name = name
    this.skip = skip
  }

  var db: GraphDatabaseAPI = _
  def setDb(db: GraphDatabaseAPI): Unit = this.db = db

  var params: util.Map[String, AnyRef] = Map.empty[String, AnyRef].asJava
  def setParams(params: util.Map[String, AnyRef]): Unit = this.params = params

  var initF: Seq[(GraphDatabaseAPI) => Unit] = Seq.empty
  def init(f: (GraphDatabaseAPI) => Unit): Unit = initF = initF :+ f

  var procReg: (GraphDatabaseAPI) => Unit = _
  def procedureRegistration(f: (GraphDatabaseAPI) => Unit): Unit = procReg = f

  var executions: Seq[(GraphDatabaseAPI, util.Map[String, Object]) => Result] = Seq.empty
  def exec(function: (GraphDatabaseAPI, util.Map[String, Object]) => Result) = executions = executions :+ function

  var expectations: Seq[(Result) => Unit] = Seq.empty
  def expect(expectation: (Result) => Unit): Unit = expectations = expectations :+ expectation

  var expectedError: (Try[Result], Transaction) => Unit = _
  def expectError(function: (Try[Result], Transaction) => Unit) = expectedError = function

  var sideEffects: Values = _
  def sideEffects(v: Values): Unit = sideEffects = v

  def build(): ScenarioExecution = {
    if (skip) {
      SkippedScenario(name)
    } else if (expectedError != null) {
      NegativeScenario(name, blacklisted(name.toLowerCase), db, params, initF, Option(procReg), executions.head, expectedError)
    } else {
      RegularScenario(name, blacklisted(name.toLowerCase), db, params, initF, Option(procReg), executions, expectations, sideEffects)
    }
  }
}

trait ScenarioExecution {
  def validate(): Unit
  def run(): Unit
  def name(): String
}

case class SkippedScenario(name: String) extends ScenarioExecution {
  override def run(): Unit = () // skip
  override def validate(): Unit = ()
}

case class NegativeScenario(name: String,
                            blacklisted: Boolean,
                            db: GraphDatabaseAPI,
                            params: util.Map[String, Object],
                            init: Seq[(GraphDatabaseAPI) => Unit],
                            procedureRegistration: Option[(GraphDatabaseAPI) => Unit],
                            execution: (GraphDatabaseAPI, util.Map[String, Object]) => Result,
                            errorExpectation: (Try[Result], Transaction) => Unit
                           ) extends ScenarioExecution {
  override def run(): Unit = {
    if (blacklisted) {
      // TODO: Report when a blacklisted negative scenario does fail with the expected error
    } else {
      init.foreach(f => f(db))
      procedureRegistration.foreach(f => f(db))

      val tx = db.beginTx()
      val attempt = Try(execution(db, params))
      try {
        errorExpectation(attempt, tx)
      } finally {
        tx.close()
        db.shutdown()
      }
    }
  }

  override def validate(): Unit = {
    if (execution == null)
      throw InvalidFeatureFormatException(s"No execution specified for scenario $name")
    if (errorExpectation == null)
      throw InvalidFeatureFormatException(s"No expectation specified for scenario $name")
  }
}

case class RegularScenario(name: String,
                           blacklisted: Boolean,
                           db: GraphDatabaseAPI,
                           params: util.Map[String, Object],
                           init: Seq[(GraphDatabaseAPI) => Unit],
                           procedureRegistration: Option[(GraphDatabaseAPI) => Unit],
                           executions: Seq[(GraphDatabaseAPI, util.Map[String, Object]) => Result],
                           expectations: Seq[(Result) => Unit],
                           expectedSideEffects: Values
                          ) extends ScenarioExecution {
  override def run(): Unit = {
    // Sometimes the scenario just can't be run for some reason
    // NOTE: Scenarios that are ignored like this will not signal when they start working again
    if (blacklisted && deeperProblems(name))
      return

    try {
      init.foreach {
        f => f(db)
      }
      procedureRegistration.foreach(f => f(db))

      val zeroState = SideEffects.measureState(db)

      var seenBlackListedFail = false
      executions.zip(expectations).foreach {
        case (execute, expect) =>
          val tx = db.beginTx()
          try {
            val result = execute(db, params)

            expect(result)

            if (!blacklisted) {
              tx.success()
              tx.close()
            }

            val afterState = SideEffects.measureState(db)
            val actual = zeroState diff afterState
            withClue("Incorrect side effects:") {
              actual should equal(expectedSideEffects)
            }

          } catch {
            case e if !blacklisted =>
              throw new ScenarioFailedException(s"Scenario '$name' failed with ${e.getMessage}", e)
            case _ if blacklisted =>
              seenBlackListedFail = true
          } finally {
            tx.close()
          }
      }

      if (blacklisted && !seenBlackListedFail)
        throw new BlacklistException(s"Scenario '$name' was blacklisted, but succeeded")

    } finally {
      db.shutdown()
    }
  }

  override def validate() = {
    if (!blacklisted) {
      if (executions.isEmpty)
        throw InvalidFeatureFormatException(s"No execution specified for scenario $name")
      if (expectations.isEmpty)
        throw InvalidFeatureFormatException(s"No expectation specified for scenario $name")
      if (expectedSideEffects == null)
        throw InvalidFeatureFormatException(s"No side effects expectation specified for scenario $name")
      if (executions.size != expectations.size)
        throw InvalidFeatureFormatException(s"Execution and expectation mismatch; must be same amount (scenario $name)")
    }
  }

  private def deeperProblems(scenarioName: String) =
    Set(
      // TODO: This scenario causes StackOverflowException in the compiled runtime ... not sure how to handle
      "Many CREATE clauses",
      // TODO: There are two scenarios with this name, one that works and one that doesn't - fixed by updating TCK later
      "Concatenating and returning the size of literal lists",
      // TODO: Once this is supported for reals in the slotted runtime, this should go away
      "Add labels inside FOREACH"
    ) contains scenarioName
}

class ScenarioFailedException(message: String, cause: Throwable) extends Exception(message, cause)

class BlacklistException(message: String) extends Exception(message)
