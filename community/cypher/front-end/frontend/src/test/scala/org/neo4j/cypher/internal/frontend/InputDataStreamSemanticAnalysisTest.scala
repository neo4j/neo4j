/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext.failWith
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class InputDataStreamSemanticAnalysisTest extends CypherFunSuite with AstConstructionTestSupport {

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private val pipeline = SemanticAnalysis(warn = true) andThen SemanticAnalysis(warn = false)

  test("can parse INPUT DATA STREAM") {
    // "INPUT DATA STREAM a, b, c RETURN *"
    val ast = singleQuery(input(varFor("a"), varFor("b"), varFor("c")), returnAll)
    val startState = initStartState(ast)

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("cannot redeclare variable") {
    // "INPUT DATA STREAM a, b, c UNWIND [] AS a RETURN *"
    val ast = singleQuery(input(varFor("a"), varFor("b"), varFor("c")), unwind(listOf(), varFor("a")), returnAll)
    val startState = initStartState(ast)

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("Variable `a` already declared")
  }

  test("INPUT DATA STREAM must be the first clause in a query") {
    // "UNWIND [0, 1] AS x INPUT DATA STREAM a, b, c RETURN *"
    val ast = singleQuery(
      unwind(listOf(literalInt(0), literalInt(1)), varFor("x")),
      input(varFor("a"), varFor("b"), varFor("c")),
      returnAll
    )

    val startState = initStartState(ast)

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("INPUT DATA STREAM must be the first clause in a query")
  }

  test("There can be only one INPUT DATA STREAM in a query") {
    // "INPUT DATA STREAM a INPUT DATA STREAM b RETURN *"
    val ast = singleQuery(input(varFor("a")), input(varFor("b")), returnAll)
    val startState = initStartState(ast)

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("There can be only one INPUT DATA STREAM in a query")
  }

  test("INPUT DATA STREAM is not supported in UNION queries") {
    // "INPUT DATA STREAM x RETURN * UNION MATCH (x) RETURN *"
    val ast = union(
      singleQuery(input(varFor("x")), returnAll),
      singleQuery(match_(nodePat(Some("x"))), returnAll)
    )
    val startState = initStartState(ast)

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("INPUT DATA STREAM is not supported in UNION queries")
  }

  test("INPUT DATA STREAM is not supported in UNION queries 2") {
    // "MATCH (x) RETURN * UNION INPUT DATA STREAM x RETURN *"
    val ast = union(
      singleQuery(match_(nodePat(Some("x"))), returnAll),
      singleQuery(input(varFor("x")), returnAll)
    )
    val startState = initStartState(ast)

    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("INPUT DATA STREAM is not supported in UNION queries")
  }

  private def initStartState(statement: Statement) = {
    // As the test only checks ast -> semantic analysis, the query isn't used.
    InitialState("whatever", NoPlannerName, new AnonymousVariableNameGenerator, maybeStatement = Some(statement))
  }
}
