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
import org.neo4j.graphdb.{QueryStatistics, Result, Transaction}
import org.neo4j.kernel.internal.GraphDatabaseAPI

import scala.util.{Failure, Success, Try}

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

  var sideEffects: (QueryStatistics) => Unit = _
  def sideEffects(f: (QueryStatistics) => Unit): Unit = sideEffects = f

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
  def run(): Unit
  def name(): String
}

case class SkippedScenario(name: String) extends ScenarioExecution {
  override def run(): Unit = () // skip
}

case class NegativeScenario(name: String, blacklisted: Boolean, db: GraphDatabaseAPI, params: util.Map[String, Object],
                            init: Seq[(GraphDatabaseAPI) => Unit], procedureRegistration: Option[(GraphDatabaseAPI) => Unit],
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
}

case class RegularScenario(name: String, blacklisted: Boolean,
                           db: GraphDatabaseAPI, params: util.Map[String, Object],
                           init: Seq[(GraphDatabaseAPI) => Unit], procedureRegistration: Option[(GraphDatabaseAPI) => Unit],
                           executions: Seq[(GraphDatabaseAPI, util.Map[String, Object]) => Result],
                           expectations: Seq[(Result) => Unit], sideEffects: (QueryStatistics) => Unit
                          ) extends ScenarioExecution {
  override def run(): Unit = {
    if (name.equals("Many CREATE clauses") && blacklisted) {
      // TODO: This scenario causes StackOverflowException in the compiled runtime ... not sure how to handle
      return
    }

    if (name.equals("Concatenating and returning the size of literal lists") && blacklisted) {
      // TODO: There are two scenarios with this name, one that works and one that doesn't - fixed by updating TCK later
      return
    }

    init.foreach(f => f(db))
    procedureRegistration.foreach(f => f(db))

    try {
      executions.zip(expectations).foreach {
        case (execute, expect) =>
          val tx = db.beginTx()
          try {
            Try(execute(db, params)) match {
              case Success(result) =>
                if (!blacklisted) {
                  expect(result)
                  tx.success()
                } else {
                  Try(expect(result)) match {
                    case Success(_) =>
                      throw new BlacklistException(s"Scenario '$name' was blacklisted, but succeeded")
                    case _ => // failure is expected
                  }
                }
              case Failure(throwable) =>
                if (!blacklisted)
                  throw new ScenarioFailedException(s"Scenario '$name' failed with ${throwable.getMessage}", throwable)
            }
          } catch {
            case e: Error =>
              if (!blacklisted)
                throw new ScenarioFailedException(s"Scenario '$name' failed with ${e.getMessage}", e)
          } finally {
            tx.close()
          }
      }
    } finally {
      db.shutdown()
    }
  }
}

class ScenarioFailedException(message: String, cause: Throwable) extends Exception(message, cause)
class BlacklistException(message: String) extends Exception(message)
