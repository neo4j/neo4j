/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

class SemanticIndexAcceptanceTest extends ExecutionEngineFunSuite with PropertyChecks {

  //the actual test
  List("<", "<=", "=", ">", ">=").foreach(testOperator)

  override protected def initTest(): Unit = {
    super.initTest()
    for(i <- 1 to 1000) createLabeledNode("Label")
  }

  def changeLastChar(f: Char => Char)(in: String) =
    if (in.isEmpty) ""
    else
      in.substring(0, in.length - 1) + (in.last - 1).toChar

  def testOperator(operator: String) = {

    val queryNotUsingIndex = s"match (n:Label) where n.nonIndexed $operator {prop} return n order by id(n)"
    val queryUsingIndex = s"match (n:Label) where n.indexed $operator {prop} return n order by id(n)"

    def testValue(queryNotUsingIndex: String, queryUsingIndex: String, value: Any): Unit = {
      val indexedResult = execute(queryUsingIndex, "prop" -> value)
      execute(queryNotUsingIndex, "prop" -> value).toList should equal(indexedResult.toList)
      indexedResult.executionPlanDescription().toString should include("NodeIndexSeek")
    }

    def tester[T](propertyValue: T, prev: T => T, next: T => T) = {
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
      graph.createIndex("Label", "indexed")
      forAll { propertyValue: Long =>
        tester(propertyValue, (l: Long) => l - 1L, (l: Long) => l + 1)
      }
    }

    test(s"testing double with $operator") {
      graph.createIndex("Label", "indexed")
      forAll { propertyValue: Double =>
        tester(propertyValue, (d: Double) => d - 0.5, (d: Double) => d + 0.5)
      }
    }

    test(s"testing string with $operator") {
      graph.createIndex("Label", "indexed")
      forAll (Gen.alphaStr){ propertyValue: String =>
        tester(propertyValue, changeLastChar(c => (c - 1).toChar), changeLastChar(c => (c + 1).toChar))
      }
    }

  }
}
