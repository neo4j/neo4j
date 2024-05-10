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

import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.OpenCypherJavaCCParsing
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SemanticScopeRecordingTest extends CypherFunSuite {

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private val pipeline = OpenCypherJavaCCParsing andThen
    PreparatoryRewriting andThen
    SemanticAnalysis(warn = true) andThen
    SemanticAnalysis(warn = false)

  // Fabric needs to know a scope of a clause immediately preceding a remote subquery,
  // because such subqueries act as delimiters between 'local' and 'remote' parts of a query
  test("record semantic scope of a clause preceding a subquery") {
    val query =
      """UNWIND [1, 2] AS x
        |CALL {
        |  MATCH (y)
        |  RETURN y
        |}
        |RETURN x, y
      """.stripMargin
    val startState = initStartState(query)

    val context = new ErrorCollectingContext()
    val state = pipeline.transform(startState, context)

    context.errors shouldBe empty

    val unwindScope = state.semantics().recordedScopes
      .map { case (k, v) => (k.node, v) }
      .collect { case (_: Unwind, scope) => scope }
      .head

    unwindScope.symbolNames should equal(Set("x"))
  }

  private def initStartState(query: String) =
    InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
}
