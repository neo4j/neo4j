/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.frontend

import org.neo4j.cypher.internal.v4_0.frontend.helpers.ErrorCollectingContext.failWith
import org.neo4j.cypher.internal.v4_0.frontend.helpers.{ErrorCollectingContext, InputDataStreamTestInitialState, InputDataStreamTestParsing, NoPlannerName}
import org.neo4j.cypher.internal.v4_0.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class InputDataStreamSemanticAnalysisTest extends CypherFunSuite {

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private val pipeline = InputDataStreamTestParsing andThen SemanticAnalysis(warn = true) andThen SemanticAnalysis(warn = false)

  test("can parse INPUT DATA STREAM") {
    val query = "INPUT DATA STREAM a, b, c RETURN *"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("cannot redeclare variable") {
    val query = "INPUT DATA STREAM a, b, c UNWIND [] AS a RETURN *"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("Variable `a` already declared")
  }

  test("INPUT DATA STREAM must be the first clause in a query") {
    val query = "UNWIND [0, 1] AS x INPUT DATA STREAM a, b, c RETURN *"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("INPUT DATA STREAM must be the first clause in a query")
  }

  test("There can be only one INPUT DATA STREAM in a query") {
    val query = "INPUT DATA STREAM a INPUT DATA STREAM b RETURN *"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("There can be only one INPUT DATA STREAM in a query")
  }

  test("INPUT DATA STREAM is not supported in UNION queries") {
    val query = "INPUT DATA STREAM x RETURN * UNION MATCH (x) RETURN *"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("INPUT DATA STREAM is not supported in UNION queries")
  }

  test("INPUT DATA STREAM is not supported in UNION queries 2") {
    val query = "MATCH (x) RETURN * UNION INPUT DATA STREAM x RETURN *"
    val startState = initStartState(query, Map("name" -> CTString))

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("INPUT DATA STREAM is not supported in UNION queries")
  }

  private def initStartState(query: String, initialFields: Map[String, CypherType]) =
    InputDataStreamTestInitialState(query, "RETURN 1", None, NoPlannerName, initialFields)
}
