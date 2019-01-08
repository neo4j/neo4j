/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_3

import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.InternalExecutionResult
import org.scalacheck.{Gen, Shrink}
import org.scalatest.prop.PropertyChecks

class SemanticIndexAcceptanceTest extends ExecutionEngineFunSuite with PropertyChecks {

  //we don't want scala check to shrink since it hides the actual error
  implicit val dontShrink: Shrink[String] = Shrink(s => Stream.empty)

  //the actual test
  List("<", "<=", "=", ">", ">=").foreach(testOperator)

  override protected def initTest(): Unit = {
    super.initTest()
    graph.createIndex("Label", "indexed")
    graph.inTx {
      graph.schema().awaitIndexesOnline(10, TimeUnit.SECONDS)
    }
    for (_ <- 1 to 1000) createLabeledNode("Label")
  }

  def changeLastChar(f: Char => Char)(in: String): String =
    if (in.isEmpty) ""
    else
      in.substring(0, in.length - 1) + (in.last - 1).toChar

  def testOperator(operator: String): Unit = {

    val queryNotUsingIndex = s"match (n:Label) where n.nonIndexed $operator {prop} return n order by id(n)"
    val queryUsingIndex = s"match (n:Label) where n.indexed $operator {prop} return n order by id(n)"

    def testValue(queryNotUsingIndex: String, queryUsingIndex: String, value: Any): Unit = {
      val indexedResult = assertingExecute(queryUsingIndex, "prop" -> value)
      assertingExecute(queryNotUsingIndex, "prop" -> value).toList should equal(indexedResult.toList)
      indexedResult.executionPlanDescription().toString should include("NodeIndexSeek")
    }

    def tester[T](propertyValue: T, prev: T => T, next: T => T): Unit = {
      graph.inTx {
        createLabeledNode(Map("nonIndexed" -> propertyValue, "indexed" -> propertyValue), "Label")

        withClue("with TxState") {
          testValue(queryNotUsingIndex, queryUsingIndex, propertyValue)
          testValue(queryNotUsingIndex, queryUsingIndex, prev(propertyValue))
          testValue(queryNotUsingIndex, queryUsingIndex, next(propertyValue))
        }
      }
      withClue("without TxState") {
        testValue(queryNotUsingIndex, queryUsingIndex, propertyValue)
        testValue(queryNotUsingIndex, queryUsingIndex, prev(propertyValue))
        testValue(queryNotUsingIndex, queryUsingIndex, next(propertyValue))
      }
    }

    test(s"testing long with $operator") {

      forAll { propertyValue: Long =>
        tester(propertyValue, (l: Long) => l - 1L, (l: Long) => l + 1)
      }
    }

    test(s"testing double with $operator") {
      forAll { propertyValue: Double =>
        tester(propertyValue, (d: Double) => d - 0.5, (d: Double) => d + 0.5)
      }
    }

    test(s"testing string with $operator") {
      forAll (Gen.alphaStr){ propertyValue: String =>
        tester(propertyValue, changeLastChar(c => (c - 1).toChar), changeLastChar(c => (c + 1).toChar))
      }
    }

  }

  /*
   * This test has been flaky on ibm jdk and we don't get the full stack trace to the failure using execute.
   */
  private def assertingExecute(q: String, params: (String, Any)*): InternalExecutionResult = try {
    execute(q, params: _*)
  } catch {
    case e: Throwable =>
      e.printStackTrace()
      fail(e.getMessage)
  }
}
