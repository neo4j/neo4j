/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext.failWith
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.OpenCypherJavaCCWithFallbackParsing
import org.neo4j.cypher.internal.frontend.phases.Parsing
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

case class SemanticAnalysisResult(context: ErrorCollectingContext, state: BaseState) {
  def errors: Seq[SemanticErrorDef] = context.errors

  def errorMessages: Seq[String] = errors.map(_.msg)

  def semanticTable: SemanticTable = state.semanticTable()
}

class SemanticAnalysisTest extends CypherFunSuite {

  private val pipeline = pipelineWithSemanticFeatures()

  type Pipeline = Transformer[BaseContext, BaseState, BaseState]

  def runSemanticAnalysisWithPipelineAndState(pipeline: Pipeline, initialState: BaseState): SemanticAnalysisResult = {
    val context = new ErrorCollectingContext()
    val state = pipeline.transform(initialState, context)
    SemanticAnalysisResult(context, state)
  }

  def initialStateWithQuery(query: String): InitialState =
    InitialState(query, None, NoPlannerName, new AnonymousVariableNameGenerator)

  def expectErrorsFrom(
                        query: String,
                        expectedErrors: Set[SemanticError],
                        pipeline: Transformer[BaseContext, BaseState, BaseState] = pipelineWithSemanticFeatures()
                      ): Unit =
    runSemanticAnalysisWithPipeline(pipeline, query).errors.toSet shouldEqual expectedErrors

  def runSemanticAnalysisWithPipeline(pipeline: Pipeline, query: String): SemanticAnalysisResult =
    runSemanticAnalysisWithPipelineAndState(pipeline, initialStateWithQuery(query))

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  private def pipelineWithSemanticFeatures(semanticFeatures: SemanticFeature*) =
    OpenCypherJavaCCWithFallbackParsing andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true, semanticFeatures: _*) andThen
      SemanticAnalysis(warn = false, semanticFeatures: _*)

  test("should fail for max() with no arguments") {
    val query = "RETURN max() AS max"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context should failWith("Insufficient parameters for function 'max'")
  }

  test("Should allow overriding variable name in RETURN clause with an ORDER BY") {
    val query = "MATCH (n) RETURN n.prop AS n ORDER BY n + 2"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("Should not allow multiple columns with the same name in WITH") {
    val query = "MATCH (n) WITH n.prop AS n, n.foo AS n ORDER BY n + 2 RETURN 1 AS one"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Multiple result columns with the same name are not supported"))
  }

  test("Should not allow duplicate variable name") {
    val query = "CREATE (n),(n) RETURN 1 as one"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors.map(_.msg) should equal(List("Variable `n` already declared"))
  }

  test("Should not allow Distinct in functions that aren't aggregate") {
    val nonAggregateFunctions = Seq(
      ("localdatetime", "'param1'"),
      ("duration", "'param1'"),
      ("left", "'param1', 4"),
      ("right", "'param1', 4"),
      ("reverse", "'param1'"),
      ("trim", "'param1'"),
      ("ceil", "0.1"),
      ("floor", "0.1"),
      ("sign", "0.1"),
      ("round", "0.1"),
      ("abs", "0.1"),
      ("asin", "0.1"),
      ("isEmpty", "'param1'"),
      ("toBoolean", "'param1'")
    )
    nonAggregateFunctions.foreach {
      case (func, params) =>
        val query = s"RETURN $func(DISTINCT $params)"

        val startState = initStartState(query)
        val context = new ErrorCollectingContext()

        pipeline.transform(startState, context)

        context.errors shouldBe Seq(SemanticError(s"Invalid use of DISTINCT with function '$func'", InputPosition(7, 1, 8)))
    }
  }

  test("Should not allow parameter maps in node pattern in MATCH") {
    val query = "MATCH (n $foo) RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")",
        InputPosition(9, 1, 10)
      ))
    )
  }

  test("Should not allow parameter maps in node pattern in MERGE") {
    val query = "MERGE (n $foo) RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")",
        InputPosition(9, 1, 10)
      ))
    )
  }

  test("Should not allow parameter maps in relationship pattern in MATCH") {
    val query = "MATCH (n)-[r $foo]->() RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")",
        InputPosition(13, 1, 14)
      ))
    )
  }

  test("Should not allow parameter maps in relationship pattern in MERGE") {
    val query = "MERGE (n)-[r:R $foo]->() RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")",
        InputPosition(15, 1, 16)
      ))
    )
  }

  test("Should allow parameter as valid predicate in FilteringExpression") {
    val queries = Seq(
      "RETURN [x IN [1,2,3] WHERE $p | x + 1] AS foo",
      "RETURN all(x IN [1,2,3] WHERE $p) AS foo",
      "RETURN any(x IN [1,2,3] WHERE $p) AS foo",
      "RETURN none(x IN [1,2,3] WHERE $p) AS foo",
      "RETURN single(x IN [1,2,3] WHERE $p) AS foo",
    )
    queries.foreach { query =>
      withClue(query) {
        val context = new ErrorCollectingContext()
        pipeline.transform(initStartState(query).withParams(Map("p" -> 42)), context)
        context.errors shouldBe empty
      }
    }
  }

  test("Should allow pattern as valid predicate in FilteringExpression") {
    val queries = Seq(
      "MATCH (n) RETURN [x IN [1,2,3] WHERE (n)--() | x + 1] AS foo",
      "MATCH (n) RETURN all(x IN [1,2,3] WHERE (n)--()) AS foo",
      "MATCH (n) RETURN any(x IN [1,2,3] WHERE (n)--()) AS foo",
      "MATCH (n) RETURN none(x IN [1,2,3] WHERE (n)--()) AS foo",
      "MATCH (n) RETURN single(x IN [1,2,3] WHERE (n)--()) AS foo",
    )
    queries.foreach { query =>
      withClue(query) {
        val context = new ErrorCollectingContext()
        pipeline.transform(initStartState(query), context)
        context.errors shouldBe empty
      }
    }
  }

  // Escaped backticks in tokens

  test("Should allow escaped backticks in node property key name") {
    // Property without escaping: `abc123``
    val query = "CREATE ({prop: 5, ```abc123`````: 1})"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors should be(empty)
  }

  test("Should allow escaped backticks in relationship property key name") {
    // Property without escaping: abc`123
    val query = "MATCH ()-[r]->() RETURN r.`abc``123` as result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors should be(empty)
  }

  test("Should allow escaped backticks in label") {
    // Label without escaping: `abc123
    val query = "MATCH (n) SET n:```abc123`"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors should be(empty)
  }

  test("Should allow escaped backtick in relationship type") {
    // Relationship type without escaping: abc123``
    val query = "MERGE ()-[r:`abc123`````]->()"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors should be(empty)
  }

  test("Should allow escaped backtick in indexes") {
    // Query without proper escaping: CREATE INDEX `abc`123`` FOR (n:`Per`son`) ON (n.first``name`, n.``last`name)
    val query = "CREATE INDEX ```abc``123````` FOR (n:```Per``son```) ON (n.`first````name```, n.`````last``name`)"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors should be(empty)
  }

  test("Should allow escaped backtick in constraints") {
    // Query without proper escaping: CREATE CONSTRAINT abc123` FOR (n:``Label) REQUIRE (n.pr``op) IS NODE KEY
    val query = "CREATE CONSTRAINT `abc123``` FOR (n:`````Label`) REQUIRE (n.`pr````op`) IS NODE KEY"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors should be(empty)
  }

  test("Should register uses in PathExpressions") {
    val query = "MATCH p = (a)-[r]-(b) RETURN p AS p"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    val pipeline = Parsing andThen ProjectNamedPathsPhase andThen SemanticAnalysis(warn = true)

    val result = pipeline.transform(startState, context)
    val scopeTree = result.semantics().scopeTree

    Set("a", "r", "b").foreach { name =>
      scopeTree.allSymbols(name).head.uses shouldNot be(empty)
    }
  }

  test("nested CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CREATE (x) } IN TRANSACTIONS } IN TRANSACTIONS RETURN 1 AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("Nested CALL { ... } IN TRANSACTIONS is not supported", InputPosition(7, 1, 8))
    )
  }

  test("regular CALL nested in CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CREATE (x) } } IN TRANSACTIONS RETURN 1 AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("CALL { ... } IN TRANSACTIONS nested in a regular CALL") {
    val query = "CALL { CALL { CREATE (x) } IN TRANSACTIONS } RETURN 1 AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported", InputPosition(7, 1, 8))
    )
  }

  test("CALL { ... } IN TRANSACTIONS nested in a regular CALL and nested CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CALL { CREATE (x) } IN TRANSACTIONS } IN TRANSACTIONS } RETURN 1 AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("Nested CALL { ... } IN TRANSACTIONS is not supported", InputPosition(14, 1, 15)),
      SemanticError("CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported", InputPosition(7, 1, 8))
    )
  }

  test("CALL { ... } IN TRANSACTIONS in a UNION") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 1 AS result
        |UNION
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 2 AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(0, 1, 1)),
      SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(61, 4, 1))
    )
  }

  test("CALL { ... } IN TRANSACTIONS in first part of UNION") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 1 AS result
        |UNION
        |RETURN 2 AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(0, 1, 1))
    )
  }

  test("CALL { ... } IN TRANSACTIONS in second part of UNION") {
    val query =
      """RETURN 1 AS result
        |UNION
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 2 AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(25, 3, 1))
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding write clause") {
    val query =
      """CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(29, 3, 1))
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS with preceding write clauses") {
    val query =
      """CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(29, 3, 1)),
      SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(65, 4, 1)),
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS with a write clause between them") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(65, 4, 1)),
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding nested write clause") {
    val query =
      """CALL { CREATE (foo) RETURN foo AS foo }
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(56, 3, 1))
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding nested write clause in a unit subquery") {
    val query =
      """CALL { CREATE (x) }
        |WITH 1 AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(34, 3, 1))
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS that contain write clauses, but no write clauses in between") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |WITH 1 AS foo
        |CALL { CREATE (y) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("CALL { ... } IN TRANSACTIONS with a following write clause") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |CREATE (foo)
        |RETURN foo AS foo""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("CALL { ... } IN TRANSACTIONS in a PERIODIC COMMIT query") {
    val query =
      """USING PERIODIC COMMIT 500 LOAD CSV FROM 'file:///artists.csv' AS line
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN line AS line""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("CALL { ... } IN TRANSACTIONS in a PERIODIC COMMIT query is not supported", InputPosition(70, 2, 1))
    )
  }

  test("should allow node pattern predicates in MATCH") {
    val query = "WITH 123 AS minValue MATCH (n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) RETURN n AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should not allow node pattern predicates in MATCH to refer to other nodes") {
    val query = "MATCH (start)-->(end:Label WHERE start.prop = 42) RETURN start AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Variable `start` not defined"
    )
  }

  test("should not allow node pattern predicates in CREATE") {
    val query = "CREATE (n WHERE n.prop = 123)"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in CREATE, but only in MATCH clause or inside a pattern comprehension"
    )
  }

  test("should not allow node pattern predicates in MERGE") {
    val query = "MERGE (n WHERE n.prop = 123)"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in MERGE, but only in MATCH clause or inside a pattern comprehension"
    )
  }

  test("should allow node pattern predicates in pattern comprehension") {
    val query = "WITH 123 AS minValue RETURN [(n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) | n] AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should not allow node pattern predicates in pattern comprehension to refer to other nodes") {
    val query = "RETURN [(start)-->(end:Label WHERE start.prop = 42) | start] AS result"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Variable `start` not defined"
    )
  }

  test("should not allow node pattern predicates in pattern expression") {
    val query =
      """MATCH (a), (b)
        |RETURN exists((a WHERE a.prop > 123)-->(b)) AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in expression, but only in MATCH clause or inside a pattern comprehension"
    )
  }

  test("should allow node pattern predicates in MATCH with shortestPath") {
    val query =
      """
        |WITH 123 AS minValue
        |MATCH p = shortestPath((n {prop: 42} WHERE n.otherProp > minValue)-[:REL*]->(m:Label WHERE m.prop = 42))
        |RETURN n AS result
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("should not allow node pattern predicates in MATCH with shortestPath to refer to other nodes") {
    val query =
      """
        |MATCH p = shortestPath((start)-[:REL*]->(end:Label WHERE start.prop = 42))
        |RETURN start AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Variable `start` not defined"
    )
  }

  test("should not allow node pattern predicates in shortestPath expression") {
    val query =
      """
        |MATCH (a), (b)
        |WITH shortestPath((a WHERE a.prop > 123)-[:REL*]->(b)) AS p
        |RETURN length(p) AS result""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors.map(_.msg) shouldBe Seq(
      "Node pattern predicates are not allowed in expression, but only in MATCH clause or inside a pattern comprehension"
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 1 ROW
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("CALL IN TRANSACTIONS with batchSize 0") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 0 ROWS
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("Invalid input. '0' is not a valid value. Must be a positive integer.", InputPosition(40, 3, 22))
    )
  }

  test("CALL IN TRANSACTIONS with batchSize -1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF -1 ROWS
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("Invalid input. '-1' is not a valid value. Must be a positive integer.", InputPosition(40, 3, 22))
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 1.5") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 1.5 ROWS
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("Invalid input. '1.5' is not a valid value. Must be a positive integer.", InputPosition(40, 3, 22)),
      SemanticError("Type mismatch: expected Integer but was Float", InputPosition(40, 3, 22)),
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 'foo'") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 'foo' ROWS
        |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("Invalid input. 'foo' is not a valid value. Must be a positive integer.", InputPosition(40, 3, 22)),
      SemanticError("Type mismatch: expected Integer but was String", InputPosition(40, 3, 22)),
    )
  }

  test("CALL IN TRANSACTIONS with batchSize larger than Long.Max") {
    val batchSize = Long.MaxValue.toString + "0"
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF $batchSize ROWS
         |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("integer is too large", InputPosition(40, 3, 22))
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a variable reference") {
    val query =
      s"""WITH 1 AS b
         |CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF b ROWS
         |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("It is not allowed to refer to variables in OF ... ROWS", InputPosition(52, 4, 22))
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a PatternComprehension") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF [path IN ()--() | 5] ROWS
         |""".stripMargin

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)

    context.errors shouldBe Seq(
      SemanticError("It is not allowed to refer to variables in OF ... ROWS", InputPosition(40, 3, 22)),
      SemanticError("Type mismatch: expected Integer but was List<Integer>", InputPosition(40, 3, 22)),
    )
  }

  test("UNION with missing return in first part") {
    val query = "CALL db.labels() YIELD label UNION CALL db.labels() YIELD label RETURN label"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)


    context.errors shouldBe Seq(
      SemanticError("All sub queries in an UNION must have the same return column names", InputPosition(29, 1, 30)),
    )
  }

  test("UNION with missing return in second part") {
    val query = "CALL db.labels() YIELD label RETURN label UNION CALL db.labels() YIELD label"

    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)


    context.errors shouldBe Seq(
      SemanticError("All sub queries in an UNION must have the same return column names", InputPosition(42, 1, 43)),
    )
  }

  test("subquery without RETURN should not declare variables from YIELD in the outer scope") {
    val q =
      """CALL {
        |  CALL dbms.procedures() YIELD name
        |}
        |RETURN name
        |""".stripMargin

    val startState = initStartState(q)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)
    context.errors shouldBe Seq(
      SemanticError("Variable `name` not defined", InputPosition(52, 4, 8))
    )
  }

  // Utilities for the following tests
  private val returnStarNMCombinations = Seq(
    "n",
    "m",
    "n, m",
    "*",
    "*, n",
    "*, m",
    "*, n, m"
  )

  private def containedVariables(returnStarNMCombination: String): Set[String] = {
    val strings = returnStarNMCombination.split(',').map(_.trim)
    if (strings.contains("*")) {
      Set("n", "m")
    } else {
      strings.toSet
    }
  }

  test("RETURN * in a CALL should export variables") {
    for {
      subqueryReturn <- returnStarNMCombinations
      subqueryReturnVars = containedVariables(subqueryReturn)
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(subqueryReturnVars)
    } {
      val query =
        s"""
           |CALL {
           |  MATCH (n), (m)
           |  RETURN $subqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val startState = initStartState(query)
        val context = new ErrorCollectingContext()
        val result = pipeline.transform(startState, context)

        context.errors should be(empty)

        val statement = result.statement().asInstanceOf[Query].part.asInstanceOf[SingleQuery]
        val semanticState = result.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in UNION in a CALL should export variables") {
    for {
      firstSubqueryReturn <- returnStarNMCombinations
      firstSubqueryReturnVars = containedVariables(firstSubqueryReturn)
      secondSubqueryReturn <- returnStarNMCombinations
      secondSubqueryReturnVars = containedVariables(secondSubqueryReturn)
      if secondSubqueryReturnVars == firstSubqueryReturnVars
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(firstSubqueryReturnVars)
    } {
      val query =
        s"""
           |CALL {
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $secondSubqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val startState = initStartState(query)
        val context = new ErrorCollectingContext()
        val result = pipeline.transform(startState, context)
        context.errors should be(empty)

        val statement = result.statement().asInstanceOf[Query].part.asInstanceOf[SingleQuery]
        val semanticState = result.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  test("RETURN * in 3-way-UNION in a CALL should export variables") {
    for {
      firstSubqueryReturn <- returnStarNMCombinations
      firstSubqueryReturnVars = containedVariables(firstSubqueryReturn)
      secondSubqueryReturn <- returnStarNMCombinations
      secondSubqueryReturnVars = containedVariables(secondSubqueryReturn)
      if secondSubqueryReturnVars == firstSubqueryReturnVars
      thirdSubqueryReturn <- returnStarNMCombinations
      thirdSubqueryReturnVars = containedVariables(thirdSubqueryReturn)
      if thirdSubqueryReturnVars == firstSubqueryReturnVars
      finalReturn <- returnStarNMCombinations
      finalReturnVars = containedVariables(finalReturn)
      if finalReturnVars.subsetOf(firstSubqueryReturnVars)
    } {
      val query =
        s"""
           |CALL {
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $firstSubqueryReturn
           |    UNION
           |  MATCH (n), (m)
           |  RETURN $thirdSubqueryReturn
           |}
           |RETURN $finalReturn
           |""".stripMargin
      withClue(query) {
        val startState = initStartState(query)
        val context = new ErrorCollectingContext()
        val result = pipeline.transform(startState, context)
        context.errors should be(empty)

        val statement = result.statement().asInstanceOf[Query].part.asInstanceOf[SingleQuery]
        val semanticState = result.semantics()
        val finalVariables = semanticState.scope(statement.clauses.last).get.symbolNames
        finalVariables should equal(finalReturnVars)
      }
    }
  }

  // Literal Map Entry on a MAP, NODE or RELATIONSHIP should work
  test("WITH {g: 1} AS l RETURN l{p: 2}") {
    val query = "WITH {g: 1} AS l RETURN l{p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("WITH {g: 1} AS l RETURN l{.*, p: 2}") {
    val query = "WITH {g: 1} AS l RETURN l{.*, p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("MATCH (n) RETURN n{p: 2}") {
    val query = "MATCH (n) RETURN n{p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("MATCH (n) RETURN n{.*, p: 2}") {
    val query = "MATCH (n) RETURN n{.*, p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("MATCH ()-[r]->() RETURN r{.*, p: 2}") {
    val query = "MATCH ()-[r]->() RETURN r{.*, p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  test("MATCH ()-[r]->() RETURN r{p: 2}") {
    val query = "MATCH ()-[r]->() RETURN r{p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()

    pipeline.transform(startState, context)

    context.errors shouldBe empty
  }

  // Literal Map Entry not on a map should not work
  test("WITH [1] AS l RETURN l{p: 2}") {
    val query = "WITH [1] AS l RETURN l{p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)
    context.errors shouldBe Seq(
      SemanticError("Type mismatch: expected Map, Node or Relationship but was List<Integer>", InputPosition(21, 1, 22))
    )
  }

  test("WITH [1] AS l RETURN l{.*, p: 2}") {
    val query = "WITH [1] AS l RETURN l{.*, p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)
    context.errors shouldBe Seq(
      SemanticError("Type mismatch: expected Map, Node or Relationship but was List<Integer>", InputPosition(21, 1, 22))
    )
  }

  test("WITH 2 AS l RETURN l{.*, p: 2}") {
    val query = "WITH 2 AS l RETURN l{.*, p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)
    context.errors shouldBe Seq(
      SemanticError("Type mismatch: expected Map, Node or Relationship but was Integer", InputPosition(19, 1, 20))
    )
  }

  test("WITH \"hello\" AS l RETURN l{.*, p: 2}") {
    val query = "WITH \"hello\" AS l RETURN l{.*, p: 2}"
    val startState = initStartState(query)
    val context = new ErrorCollectingContext()
    pipeline.transform(startState, context)
    context.errors shouldBe Seq(
      SemanticError("Type mismatch: expected Map, Node or Relationship but was String", InputPosition(25, 1, 26))
    )
  }

  private def initStartState(query: String) =
    InitialState(query, None, NoPlannerName, new AnonymousVariableNameGenerator)

  final case object ProjectNamedPathsPhase extends Phase[BaseContext, BaseState, BaseState] {
    override def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE
    override def process(from: BaseState, context: BaseContext): BaseState = {
      from.withStatement(from.statement().endoRewrite(projectNamedPaths))
    }
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }
}
