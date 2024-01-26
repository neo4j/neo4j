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

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.frontend.phases.OpenCypherJavaCCParsing
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.RepeatedRelationshipReference
import org.neo4j.cypher.internal.util.symbols.CTAny

class SemanticAnalysisTest extends SemanticAnalysisTestSuite {

  private val pipelineWithMultiGraphs = pipelineWithSemanticFeatures(
    SemanticFeature.MultipleGraphs
  )

  private val pipelineWithUseAsMultipleGraphsSelector = pipelineWithSemanticFeatures(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.UseAsMultipleGraphsSelector
  )

  private val pipelineWithUseAsSingleGraphSelector = pipelineWithSemanticFeatures(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.UseAsSingleGraphSelector
  )

  private val emptyTokenErrorMessage =
    "'' is not a valid token name. Token names cannot be empty or contain any null-bytes."

  test("should fail for max() with no arguments") {
    val query = "RETURN max() AS max"
    expectErrorsFrom(
      query,
      Set(SemanticError("Insufficient parameters for function 'max'", InputPosition(7, 1, 8)))
    )
  }

  test("Should allow overriding variable name in RETURN clause with an ORDER BY") {
    val query = "MATCH (n) RETURN n.prop AS n ORDER BY n + 2"
    expectNoErrorsFrom(query)
  }

  test("Should not allow multiple columns with the same name in WITH") {
    val query = "MATCH (n) WITH n.prop AS n, n.foo AS n ORDER BY n + 2 RETURN 1 AS one"
    expectErrorsFrom(
      query,
      Set(SemanticError("Multiple result columns with the same name are not supported", InputPosition(15, 1, 16)))
    )
  }

  test("Should not allow duplicate variable name in CREATE") {
    val query = "CREATE (n), (n) RETURN 1 as one"
    expectErrorsFrom(
      query,
      Set(SemanticError("Variable `n` already declared", InputPosition(13, 1, 14)))
    )
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
        expectErrorsFrom(
          query,
          Set(SemanticError(s"Invalid use of DISTINCT with function '$func'", InputPosition(7, 1, 8)))
        )
    }
  }

  test("Should not allow parameter maps in node pattern in MATCH") {
    val query = "MATCH (n $foo) RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in `MATCH` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
        InputPosition(9, 1, 10)
      ))
    )
  }

  test("Should not allow parameter maps in node pattern in MERGE") {
    val query = "MERGE (n $foo) RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in `MERGE` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
        InputPosition(9, 1, 10)
      ))
    )
  }

  test("Should not allow parameter maps in relationship pattern in MATCH") {
    val query = "MATCH (n)-[r $foo]->() RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in `MATCH` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
        InputPosition(13, 1, 14)
      ))
    )
  }

  test("Should not allow parameter maps in relationship pattern in MERGE") {
    val query = "MERGE (n)-[r:R $foo]->() RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError(
        "Parameter maps cannot be used in `MERGE` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
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
      "RETURN single(x IN [1,2,3] WHERE $p) AS foo"
    )
    queries.foreach { query =>
      withClue(query) {
        val pipeline = pipelineWithSemanticFeatures()
        val initialState = initialStateWithQuery(query).withParams(Map(AutoExtractedParameter(
          "p",
          CTAny
        )(InputPosition.NONE) -> StringLiteral("hello")(InputPosition.NONE, InputPosition.NONE)))
        val result = runSemanticAnalysisWithPipelineAndState(pipeline, initialState)

        result.errors shouldBe empty
      }
    }
  }

  test("Should allow pattern as valid predicate in FilteringExpression") {
    val queries = Seq(
      "MATCH (n) RETURN [x IN [1,2,3] WHERE (n)--() | x + 1] AS foo",
      "MATCH (n) RETURN all(x IN [1,2,3] WHERE (n)--()) AS foo",
      "MATCH (n) RETURN any(x IN [1,2,3] WHERE (n)--()) AS foo",
      "MATCH (n) RETURN none(x IN [1,2,3] WHERE (n)--()) AS foo",
      "MATCH (n) RETURN single(x IN [1,2,3] WHERE (n)--()) AS foo"
    )
    queries.foreach { query =>
      withClue(query) {
        expectNoErrorsFrom(query)
      }
    }
  }

  // Escaped backticks in tokens

  test("Should allow escaped backticks in node property key name") {
    // Property without escaping: `abc123``
    val query = "CREATE ({prop: 5, ```abc123`````: 1})"
    expectNoErrorsFrom(query)
  }

  test("Should allow escaped backticks in relationship property key name") {
    // Property without escaping: abc`123
    val query = "MATCH ()-[r]->() RETURN r.`abc``123` as result"
    expectNoErrorsFrom(query)
  }

  test("Should allow escaped backticks in label") {
    // Label without escaping: `abc123
    val query = "MATCH (n) SET n:```abc123`"
    expectNoErrorsFrom(query)
  }

  test("Should allow escaped backtick in relationship type") {
    // Relationship type without escaping: abc123``
    val query = "MERGE ()-[r:`abc123`````]->()"
    expectNoErrorsFrom(query)
  }

  test("Should allow escaped backtick in indexes") {
    // Query without proper escaping: CREATE INDEX `abc`123`` FOR (n:`Per`son`) ON (n.first``name`, n.``last`name)
    val query = "CREATE INDEX ```abc``123````` FOR (n:```Per``son```) ON (n.`first````name```, n.`````last``name`)"
    expectNoErrorsFrom(query)
  }

  test("Should allow escaped backtick in constraints") {
    // Query without proper escaping: CREATE CONSTRAINT abc123` FOR (n:``Label) REQUIRE (n.pr``op) IS NODE KEY
    val query = "CREATE CONSTRAINT `abc123``` FOR (n:`````Label`) REQUIRE (n.`pr````op`) IS NODE KEY"
    expectNoErrorsFrom(query)
  }

  test("Should register uses in PathExpressions") {
    val query = "MATCH p = (a)-[r]-(b) RETURN p AS p"

    val pipeline = OpenCypherJavaCCParsing andThen ProjectNamedPathsPhase andThen SemanticAnalysis(warn = true)
    val result = runSemanticAnalysisWithPipeline(pipeline, query)
    val scopeTree = result.state.semantics().scopeTree

    Set("a", "r", "b").foreach { name =>
      scopeTree.allSymbols(name).head.uses shouldNot be(empty)
    }
  }

  test("should allow node pattern predicates in MATCH") {
    val query =
      "WITH 123 AS minValue MATCH (n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) RETURN n AS result"
    expectNoErrorsFrom(query)
  }

  test("should allow node pattern predicates in MATCH to refer to other nodes") {
    val query = "MATCH (start)-->(end:Label WHERE start.prop = 42) RETURN start AS result"
    expectNoErrorsFrom(query)
  }

  test("should allow node pattern predicates in shortest path to refer to other nodes") {
    val query = "MATCH (a), (b) MATCH shortestPath( (a)-->(b WHERE c.prop = 42) ), (c) RETURN count(*) AS result"
    expectNoErrorsFrom(query)
  }

  test("should allow node property predicates in shortest path to refer to other nodes") {
    val query = "MATCH (a), (b) MATCH shortestPath( (a)-->(b {prop: c.prop}) ), (c) RETURN count(*) AS result"
    expectNoErrorsFrom(query)
  }

  test("should not allow node pattern predicates in CREATE") {
    val query = "CREATE (n WHERE n.prop = 123)"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Node pattern predicates are not allowed in a CREATE clause, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(23, 1, 24)
        )
      )
    )
  }

  test("should not allow node pattern predicates in MERGE") {
    val query = "MERGE (n WHERE n.prop = 123)"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Node pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(22, 1, 23)
        )
      )
    )
  }

  test("should allow node pattern predicates in pattern comprehension") {
    val query =
      "WITH 123 AS minValue RETURN [(n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) | n] AS result"
    expectNoErrorsFrom(query)
  }

  test("should allow node pattern predicates in pattern comprehension to refer to other nodes") {
    val query = "RETURN [(start)-->(end:Label WHERE start.prop = 42) | start] AS result"
    expectNoErrorsFrom(query)
  }

  test("should not allow node pattern predicates in pattern expression") {
    val query =
      """MATCH (a), (b)
        |RETURN exists((a WHERE a.prop > 123)-->(b)) AS result""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Node pattern predicates are not allowed in an expression, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(45, 2, 31)
        )
      )
    )
  }

  test("should allow node pattern predicates in MATCH with shortestPath") {
    val query =
      """
        |WITH 123 AS minValue
        |MATCH p = shortestPath((n {prop: 42} WHERE n.otherProp > minValue)-[:REL*]->(m:Label WHERE m.prop = 42))
        |RETURN n AS result
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("should allow node pattern predicates in MATCH with shortestPath to refer to other nodes") {
    val query =
      """
        |MATCH p = shortestPath((start)-[:REL*]->(end:Label WHERE start.prop = 42))
        |RETURN start AS result""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("should not allow node pattern predicates in shortestPath expression") {
    val query =
      """
        |MATCH (a), (b)
        |WITH shortestPath((a WHERE a.prop > 123)-[:REL*]->(b)) AS p
        |RETURN length(p) AS result""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Node pattern predicates are not allowed in an expression, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(50, 3, 35)
        )
      )
    )
  }

  test("should allow relationship pattern predicates in MATCH") {
    val query =
      "WITH 123 AS minValue MATCH (n)-[r:Relationship {prop: 42} WHERE r.otherProp > minValue]->(m) RETURN r AS result"
    expectNoErrorsFrom(query)
  }

  test("should not allow relationship pattern predicates in MATCH when path length is provided") {
    val query =
      "WITH 123 AS minValue MATCH (n)-[r:Relationship*1..3 {prop: 42} WHERE r.otherProp > minValue]->(m) RETURN r AS result"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Relationship pattern predicates are not supported for variable-length relationships.",
          InputPosition(81, 1, 82)
        )
      )
    )
  }

  test("should allow relationship pattern predicates in MATCH to refer to nodes") {
    val query = "MATCH (n)-[r:Relationship WHERE n.prop = 42]->(m:Label) RETURN r AS result"
    expectNoErrorsFrom(query)
  }

  test("should not allow relationship pattern predicates in CREATE") {
    val query = "CREATE (n)-[r:Relationship WHERE r.prop = 42]->(m)"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Relationship pattern predicates are not allowed in a CREATE clause, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(40, 1, 41)
        )
      )
    )
  }

  test("should not allow relationship pattern predicates in MERGE") {
    val query = "MERGE (n)-[r:Relationship WHERE r.prop = 42]->(m)"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Relationship pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(39, 1, 40)
        )
      )
    )
  }

  test("should allow relationship pattern predicates in pattern comprehension") {
    val query =
      "WITH 123 AS minValue RETURN [(n)-[r:Relationship {prop: 42} WHERE r.otherProp > minValue]->(m) | r] AS result"
    expectNoErrorsFrom(query)
  }

  test("should allow relationship pattern predicates in pattern comprehension to refer to nodes") {
    val query = "RETURN [(n)-[r:Relationship WHERE n.prop = 42]->(m:Label) | r] AS result"
    expectNoErrorsFrom(query)
  }

  test("should not allow relationship pattern predicates in pattern expression") {
    val query =
      """MATCH (a)-[r]->(b)
        |RETURN exists((a)-[r WHERE r.prop > 123]->(b)) AS result""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Relationship pattern predicates are not allowed in an expression, but only in a MATCH clause or inside a pattern comprehension",
          InputPosition(53, 2, 35)
        )
      )
    )
  }

  test("should not allow USE when semantic feature is not set") {
    val query = "USE g RETURN 1"
    expectErrorMessagesFrom(
      query,
      Set(
        messageProvider.createUseClauseUnsupportedError()
      ),
      pipelineWithMultiGraphs
    )
  }

  test("should allow USE when UseAsMultipleGraphsSelector feature is set") {
    val query = "USE g RETURN 1"
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("should allow USE when UseAsSingleGraphSelector feature is set") {
    val query = "USE g RETURN 1"
    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test("Allow qualified identifier in USE when UseAsMultipleGraphsSelector feature is set") {
    val query = "USE x.y.z RETURN 1"
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Allow qualified identifier in USE when UseAsSingleGraphSelector feature is set") {
    val query = "USE x.y.z RETURN 1"
    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test("Allow view invocation in USE when UseAsMultipleGraphsSelector feature is set") {
    val query =
      """
        |USE v($g, w($k))
        |RETURN 1
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Don't allow view invocation in USE when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |USE v($g, w($k))
        |RETURN 1
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          messageProvider.createDynamicGraphReferenceUnsupportedError("v($g, w($k))"),
          InputPosition(1, 2, 1)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("Allow qualified view invocation in USE") {
    val query =
      """
        |USE a.b.v($g, x.g(), x.v($k))
        |RETURN 1
        |""".stripMargin
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Allow expressions in view invocations (with feature flag)") {
    val query = "USE v(2, 'x', $x, $x+3) RETURN 1"
    expectNoErrorsFrom(query, pipelineWithUseAsMultipleGraphsSelector)
  }

  test("Expressions in view invocations are checked (with feature flag)") {
    val query = "USE v(2, 'x', y, $x+3) RETURN 1"
    expectErrorsFrom(
      query,
      Set(SemanticError("Variable `y` not defined", InputPosition(14, 1, 15))),
      pipelineWithUseAsMultipleGraphsSelector
    )
  }

  test("should allow multiple USE referencing the same graph when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |USE x
        |RETURN 1
        |UNION
        |USE x
        |RETURN 1
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test(
    "should allow multiple USE with qualified identifier referencing the same graph when UseAsSingleGraphSelector feature is set"
  ) {
    val query =
      """
        |USE x.y.z
        |RETURN 1
        |UNION
        |USE x.y.z
        |RETURN 1
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test("should not allow multiple USE referencing different graphs when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |USE x
        |RETURN 1
        |UNION
        |USE y
        |RETURN 1
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          messageProvider.createMultipleGraphReferencesError("y"),
          InputPosition(22, 5, 1)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("should not allow multiple USE referencing multiple graphs in subquery") {
    val query =
      """
        |CALL {
        |  USE A
        |  CALL {
        |    USE B
        |    RETURN 1 as n
        |  }
        |  RETURN n
        |}
        |RETURN n;
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          messageProvider.createMultipleGraphReferencesError("B"),
          InputPosition(29, 5, 5)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("should not allow multiple USE referencing multiple graphs in nested inner subquery") {
    val query =
      """
        |USE A
        |CALL {
        |  CALL {
        |    USE B
        |    RETURN 1 as n
        |  }
        |  RETURN n
        |}
        |RETURN n;
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          messageProvider.createMultipleGraphReferencesError("B"),
          InputPosition(27, 5, 5)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("should allow combining explicit and ambient graph selection") {
    val query =
      """
        |RETURN 1
        |UNION
        |USE x
        |RETURN 1
        |""".stripMargin

    expectNoErrorsFrom(query, pipelineWithUseAsSingleGraphSelector)
  }

  test("should allow USE only in leading position") {
    val query =
      """
        |MATCH (n)
        |USE g
        |RETURN n
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
          InputPosition(11, 3, 1)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  // WITH is a bit special as importing WITH is allowed in sub-queries,
  // so let's test we accidentally don't allow WITH at a start of a query.
  test("should not allow USE preceded by WITH") {
    val query =
      """
        |WITH 1 AS x
        |USE g
        |MATCH (n)
        |RETURN n
        |""".stripMargin

    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
          InputPosition(13, 3, 1)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  test("should allow USE only in leading position in UNION") {
    val query =
      """
        |MATCH (n)
        |USE g
        |RETURN n
        |UNION
        |MATCH (n)
        |RETURN n
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
          InputPosition(11, 3, 1)
        )
      ),
      pipelineWithUseAsSingleGraphSelector
    )
  }

  // positive tests that we get the error message
  // "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN"

  test("Should give helpful error when accessing illegal variable in ORDER BY after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY p.name RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p",
          InputPosition(49, 1, 50)
        )
      )
    )
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail ORDER BY p.name RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p",
          InputPosition(49, 1, 50)
        )
      )
    )
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after RETURN DISTINCT") {
    val query = "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY p.name"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p",
          InputPosition(51, 1, 52)
        )
      )
    )
  }

  test("Should give helpful error when accessing illegal variable in ORDER BY after RETURN with aggregation") {
    val query = "MATCH (p) RETURN collect(p.email) AS mail ORDER BY p.name"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p",
          InputPosition(51, 1, 52)
        )
      )
    )
  }

  test("Should give helpful error when accessing illegal variable in WHERE after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail WHERE p.name IS NOT NULL RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p",
          InputPosition(46, 1, 47)
        )
      )
    )
  }

  test("Should give helpful error when accessing illegal variable in WHERE after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail WHERE p.name IS NOT NULL RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: p",
          InputPosition(46, 1, 47)
        )
      )
    )
  }

  // negative tests that we do not get this error message otherwise

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY q.name RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `q` not defined", InputPosition(49, 1, 50))
      )
    )
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail ORDER BY q.name RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `q` not defined", InputPosition(49, 1, 50))
      )
    )
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN DISTINCT") {
    val query = "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY q.name"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `q` not defined", InputPosition(51, 1, 52))
      )
    )
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN with aggregation") {
    val query = "MATCH (p) RETURN collect(p.email) AS mail ORDER BY q.name"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `q` not defined", InputPosition(51, 1, 52))
      )
    )
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH DISTINCT") {
    val query = "MATCH (p) WITH DISTINCT p.email AS mail WHERE q.name IS NOT NULL RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `q` not defined", InputPosition(46, 1, 47))
      )
    )
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH with aggregation") {
    val query = "MATCH (p) WITH collect(p.email) AS mail WHERE q.name IS NOT NULL RETURN mail AS mail"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `q` not defined", InputPosition(46, 1, 47))
      )
    )
  }

  // Empty tokens for node property

  test("Should not allow empty node property key name in CREATE clause") {
    val query = "CREATE ({prop: 5, ``: 1})"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(18, 1, 19))
      )
    )
  }

  test("Should not allow empty node property key name in MERGE clause") {
    val query = "MERGE (n {``: 1})"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(10, 1, 11))
      )
    )
  }

  test("Should not allow empty node property key name in ON CREATE SET") {
    val query = "MERGE (n :Label) ON CREATE SET n.`` = 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(33, 1, 34))
      )
    )
  }

  test("Should not allow empty node property key name in ON MATCH SET") {
    val query = "MERGE (n :Label) ON MATCH SET n.`` = 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(32, 1, 33))
      )
    )
  }

  test("Should not allow empty node property key name in MATCH clause") {
    val query = "MATCH (n {``: 1}) RETURN n AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(10, 1, 11))
      )
    )
  }

  test("Should not allow empty node property key name in SET clause") {
    val query = "MATCH (n) SET n.``= 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(16, 1, 17))
      )
    )
  }

  test("Should not allow empty node property key name in REMOVE clause") {
    val query = "MATCH (n) REMOVE n.``"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(19, 1, 20))
      )
    )
  }

  test("Should not allow empty node property key name in WHERE clause") {
    val query = "MATCH (n) WHERE n.``= 1 RETURN n AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(18, 1, 19))
      )
    )
  }

  test("Should not allow empty node property key name in WITH clause") {
    val query = "MATCH (n) WITH n.`` AS prop RETURN prop AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(17, 1, 18))
      )
    )
  }

  test("Should not allow empty node property key name in ORDER BY in WITH") {
    val query = "MATCH (n) WITH n AS invalid ORDER BY n.`` RETURN count(*) AS count"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(39, 1, 40))
      )
    )
  }

  test("Should not allow empty node property key name in RETURN clause") {
    val query = "MATCH (n) RETURN n.`` AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(19, 1, 20))
      )
    )
  }

  test("Should not allow empty node property key name in DISTINCT RETURN clause") {
    val query = "MATCH (n) RETURN DISTINCT n.`` AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(28, 1, 29))
      )
    )
  }

  test("Should not allow empty node property key name in aggregation in RETURN clause") {
    val query = "MATCH (n) RETURN count(n.``) AS count"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(25, 1, 26))
      )
    )
  }

  test("Should not allow empty node property key name in ORDER BY in RETURN") {
    val query = "MATCH (n) RETURN n AS invalid ORDER BY n.`` DESC LIMIT 2"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(41, 1, 42))
      )
    )
  }

  test("Should not allow empty node property key name in CASE clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE n.``
        |WHEN 'val'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(25, 4, 8))
      )
    )
  }

  test("Should not allow empty node property key name in CASE WHEN clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.`` = 'blue'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(30, 5, 8))
      )
    )
  }

  test("Should not allow empty node property key name in CASE THEN clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.prop = 'blue'
        |THEN n.``
        |ELSE 2 END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(51, 6, 8))
      )
    )
  }

  test("Should not allow empty node property key name in CASE ELSE clause") {
    val query =
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.prop = 'blue'
        |THEN 1
        |ELSE n.`` END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(58, 7, 8))
      )
    )
  }

  // Empty tokens for relationship properties

  test("Should not allow empty relationship property key name in CREATE clause") {
    val query = "CREATE ()-[:REL {``: 1}]->()"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(17, 1, 18))
      )
    )
  }

  test("Should not allow empty relationship property key name in MERGE clause") {
    val query = "MERGE ()-[r :REL {``: 1, prop: 42}]->()"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(18, 1, 19))
      )
    )
  }

  test("Should not allow empty relationship property key name in ON CREATE SET") {
    val query = "MERGE ()-[r:REL]->() ON CREATE SET r.`` = 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(37, 1, 38))
      )
    )
  }

  test("Should not allow empty relationship property key name in ON MATCH SET") {
    val query = "MERGE ()-[r:REL]->() ON MATCH SET r.`` = 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(36, 1, 37))
      )
    )
  }

  test("Should not allow empty relationship property key name in MATCH clause") {
    val query = "MATCH ()-[r {prop:1337, ``: 1}]->() RETURN r AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(24, 1, 25))
      )
    )
  }

  test("Should not allow empty relationship property key name in SET clause") {
    val query = "MATCH ()-[r]->() SET r.``= 1 RETURN r AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(23, 1, 24))
      )
    )
  }

  test("Should not allow empty relationship property key name in REMOVE clause") {
    val query = "MATCH ()-[r]->() REMOVE r.``"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(26, 1, 27))
      )
    )
  }

  test("Should not allow empty relationship property key name in WHERE clause") {
    val query = "MATCH (n)-[r]->() WHERE n.prop > r.`` RETURN n AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(35, 1, 36))
      )
    )
  }

  test("Should not allow empty relationship property key name in WITH clause") {
    val query = "MATCH ()-[r]->() WITH r.`` AS prop, r.prop as prop2 RETURN count(*) AS count"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(24, 1, 25))
      )
    )
  }

  test("Should not allow empty relationship property key name in ORDER BY in WITH") {
    val query = "MATCH ()-[r]->() WITH r AS invalid ORDER BY r.`` RETURN count(*) AS count"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(46, 1, 47))
      )
    )
  }

  test("Should not allow empty relationship property key name in RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN r.`` as result"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(26, 1, 27))
      )
    )
  }

  test("Should not allow empty relationship property key name in DISTINCT RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN DISTINCT r.`` as result"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(35, 1, 36))
      )
    )
  }

  test("Should not allow empty relationship property key name in aggregation in RETURN clause") {
    val query = "MATCH ()-[r]->() RETURN max(r.``) AS max"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(30, 1, 31))
      )
    )
  }

  test("Should not allow empty relationship property key name in ORDER BY in RETURN") {
    val query = "MATCH ()-[r]->() RETURN r AS result ORDER BY r.``"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(47, 1, 48))
      )
    )
  }

  test("Should not allow empty relationship property key name in CASE clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE r.``
        |WHEN 'val'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(32, 4, 8))
      )
    )
  }

  test("Should not allow empty relationship property key name in CASE WHEN clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE
        |WHEN r.`` = 'blue'
        |THEN 1
        |ELSE 2 END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(37, 5, 8))
      )
    )
  }

  test("Should not allow empty relationship property key name in CASE THEN clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE
        |WHEN r.prop = 'blue'
        |THEN r.``
        |ELSE 2 END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(58, 6, 8))
      )
    )
  }

  test("Should not allow empty relationship property key name in CASE ELSE clause") {
    val query =
      """
        |MATCH ()-[r]->()
        |RETURN
        |CASE
        |WHEN r.prop = 'blue'
        |THEN 1
        |ELSE r.`` END AS result
      """.stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(65, 7, 8))
      )
    )
  }

  // Empty tokens for labels

  test("Should not allow empty label in CREATE clause") {
    val query = "CREATE (:Valid:``)"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(14, 1, 15))
      )
    )
  }

  test("Should not allow empty label in MERGE clause") {
    val query = "MERGE (n:``)"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(9, 1, 10))
      )
    )
  }

  test("Should not allow empty label in MATCH clause") {
    val query = "MATCH (n:``:Valid) RETURN n AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(11, 1, 12))
      )
    )
  }

  test("Should not allow empty label in label expression") {
    val query = "MATCH (n:``&Valid) RETURN n AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(11, 1, 12))
      )
    )
  }

  test("should not allow empty label name in label expression predicate") {
    val query = "MATCH (n) WHERE n:A&`` RETURN *"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(19, 1, 20))
      )
    )
  }

  test("should not allow empty label name in label expression with legacy symbols") {
    val query = "MATCH (n) WHERE n:A:`` RETURN *"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(19, 1, 20))
      )
    )
  }

  test("Should not allow empty label in SET clause") {
    val query = "MATCH (n) SET n:``"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(14, 1, 15))
      )
    )
  }

  test("Should not allow empty label in REMOVE clause") {
    val query = "MATCH (n) REMOVE n:``"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(17, 1, 18))
      )
    )
  }

  // Empty tokens for relationship type

  test("Should not allow empty relationship type in CREATE clause") {
    val query = "CREATE ()-[:``]->()"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(12, 1, 13))
      )
    )
  }

  test("Should not allow empty relationship type in MERGE clause") {
    val query = "MERGE ()-[r :``]->()"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(13, 1, 14))
      )
    )
  }

  test("Should not allow empty relationship type in MATCH clause") {
    val query = "MATCH ()-[r :``]->() RETURN r AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(13, 1, 14))
      )
    )
  }

  test("Should not allow empty relationship type in variable length pattern") {
    val query = "MATCH ()-[r :``*1..5]->() RETURN r AS invalid"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(emptyTokenErrorMessage, InputPosition(13, 1, 14))
      )
    )
  }

  test("Should not allow to use aggregate functions inside aggregate functions") {
    val query = "WITH 1 AS x RETURN sum(max(x)) AS sumOfMax"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Can't use aggregate functions inside of aggregate functions.", InputPosition(23, 1, 24))
      )
    )
  }

  test("Should not allow to use count(*) inside aggregate functions") {
    val query = "WITH 1 AS x RETURN min(count(*)) AS minOfCount"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Can't use aggregate functions inside of aggregate functions.", InputPosition(23, 1, 24))
      )
    )
  }

  test("Should allow repeating rel variable in pattern") {
    val query = "MATCH ()-[r]-()-[r]-() RETURN r AS r"
    expectNotificationsFrom(
      query,
      Set(
        RepeatedRelationshipReference(
          InputPosition(10, 1, 11),
          "r",
          "()-[r]-()-[r]-()"
        )
      )
    )
  }

  test("Should allow repeating rel variable in comma separated patterns") {
    val query = "MATCH ()-[r]-(), ()-[r]-() RETURN r AS r"
    expectNotificationsFrom(
      query,
      Set(
        RepeatedRelationshipReference(
          InputPosition(10, 1, 11),
          "r",
          "()-[r]-(), ()-[r]-()"
        ),
        CartesianProductNotification(
          InputPosition(0, 1, 1),
          Set.empty,
          "()-[r]-(), ()-[r]-()"
        )
      )
    )
  }

  test("Should allow repeating rel variable in comma separated paths") {
    val query = "MATCH p = ()-[r]-(), q = ()-[r]-() RETURN p, q"
    expectNotificationsFrom(
      query,
      Set(
        RepeatedRelationshipReference(
          InputPosition(14, 1, 15),
          "r",
          "p = ()-[r]-(), q = ()-[r]-()"
        ),
        CartesianProductNotification(
          InputPosition(0, 1, 1),
          Set.empty,
          "p = ()-[r]-(), q = ()-[r]-()"
        )
      )
    )
  }

  test("Should allow repeated rel variable in pattern expression") {
    val query = normalizeNewLines("MATCH ()-[r]-() RETURN size( ()-[r]-()-[r]-() ) AS size")
    expectNotificationsFrom(
      query,
      Set(
        RepeatedRelationshipReference(
          InputPosition(33, 1, 34),
          "r",
          "()-[r]-()-[r]-()"
        )
      )
    )
  }

  test("Should allow repeated rel variable in pattern comprehension") {
    val query = "MATCH ()-[r]-() RETURN [ ()-[r]-()-[r]-() | r ] AS rs"
    expectNotificationsFrom(
      query,
      Set(
        RepeatedRelationshipReference(
          InputPosition(29, 1, 30),
          "r",
          "()-[r]-()-[r]-()"
        )
      )
    )
  }

  test("Should type check predicates in FilteringExpression") {
    val queries = Seq(
      ("RETURN [x IN [1,2,3] WHERE 42 | x + 1] AS foo", InputPosition(27, 1, 28)),
      ("RETURN all(x IN [1,2,3] WHERE 42) AS foo", InputPosition(30, 1, 31)),
      ("RETURN any(x IN [1,2,3] WHERE 42) AS foo", InputPosition(30, 1, 31)),
      ("RETURN none(x IN [1,2,3] WHERE 42) AS foo", InputPosition(31, 1, 32)),
      ("RETURN single(x IN [1,2,3] WHERE 42) AS foo", InputPosition(33, 1, 34))
    )
    queries.foreach {
      case (query, pos) =>
        expectErrorsFrom(query, Set(SemanticError("Type mismatch: expected Boolean but was Integer", pos)))
    }
  }

  test("Should disallow introducing variables in pattern expressions") {
    val query = "MATCH (x) WHERE (x)-[r]-(y) RETURN x"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("PatternExpressions are not allowed to introduce new variables: 'r'.", InputPosition(21, 1, 22)),
        SemanticError("PatternExpressions are not allowed to introduce new variables: 'y'.", InputPosition(25, 1, 26))
      )
    )
  }

  Seq("SKIP", "LIMIT").foreach { phrase =>
    test(s"$phrase with variables should complain") {
      val query = s"MATCH (a) RETURN * $phrase a.prop"
      expectErrorsFrom(
        query,
        Set(
          SemanticError(
            s"It is not allowed to refer to variables in $phrase, so that the value for $phrase can be statically calculated.",
            InputPosition(20 + phrase.length, 1, 21 + phrase.length)
          )
        )
      )
    }
    test(s"$phrase with PatternComprehension should complain") {
      val query = s"RETURN 1 $phrase size([(a)-->(b) | a.prop])"
      expectErrorsFrom(
        query,
        Set(
          SemanticError(
            s"It is not allowed to use patterns in the expression for $phrase, so that the value for $phrase can be statically calculated.",
            InputPosition(10 + phrase.length, 1, 11 + phrase.length)
          )
        )
      )
    }

    test(s"$phrase with PatternExpression should complain") {
      val query = s"RETURN 1 $phrase size(()-->())"
      expectErrorsFrom(
        query,
        Set(
          SemanticError(
            s"It is not allowed to use patterns in the expression for $phrase, so that the value for $phrase can be statically calculated.",
            InputPosition(10 + phrase.length, 1, 11 + phrase.length)
          )
        )
      )
    }

    test(s"$phrase with CountExpression should complain") {
      val query = s"RETURN 1 $phrase COUNT { ()--() }"
      expectErrorsFrom(
        query,
        Set(
          SemanticError(
            s"It is not allowed to use patterns in the expression for $phrase, so that the value for $phrase can be statically calculated.",
            InputPosition(10 + phrase.length, 1, 11 + phrase.length)
          )
        )
      )
    }
  }

  test("UNION with incomplete first part") {
    val query = "MATCH (a) WITH a UNION MATCH (a) RETURN a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Query cannot conclude with WITH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
          InputPosition(10, 1, 11)
        ),
        SemanticError(
          "All sub queries in an UNION must have the same return column names",
          InputPosition(17, 1, 18)
        )
      )
    )
  }

  test("UNION with missing return in first part") {
    val query = "CALL db.labels() YIELD label UNION CALL db.labels() YIELD label RETURN label"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "All sub queries in an UNION must have the same return column names",
          InputPosition(29, 1, 30)
        )
      )
    )
  }

  test("UNION with missing return in second part") {
    val query = "CALL db.labels() YIELD label RETURN label UNION CALL db.labels() YIELD label"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "All sub queries in an UNION must have the same return column names",
          InputPosition(42, 1, 43)
        )
      )
    )
  }

  test("UNION with incomplete second part") {
    val query = "MATCH (a) RETURN a UNION MATCH (a) WITH a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Query cannot conclude with WITH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
          InputPosition(35, 1, 36)
        ),
        SemanticError(
          "All sub queries in an UNION must have the same return column names",
          InputPosition(19, 1, 20)
        )
      )
    )
  }

  test("Query ending in CALL ... YIELD ...") {
    val query = "MATCH (a) CALL proc.foo() YIELD bar"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Query cannot conclude with CALL together with YIELD", InputPosition(10, 1, 11))
      )
    )
  }

  test("Query with only importing WITH") {
    val query = "WITH a"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Variable `a` not defined", InputPosition(5, 1, 6)),
        SemanticError(
          "Query cannot conclude with WITH (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
          InputPosition(0, 1, 1)
        )
      )
    )
  }

  test("Relationship Pattern predicates should be enabled by default") {
    val query = "MATCH ()-[r:Rel WHERE r.prop > 42]->() return *"
    expectNoErrorsFrom(query)
  }

  test("Relationship Pattern predicates should not be allowed with quantification") {
    val query = "MATCH ()-[r:Rel* WHERE r.prop > 42]->() return *"
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Relationship pattern predicates are not supported for variable-length relationships.",
          InputPosition(30, 1, 31)
        )
      )
    )
  }

  test("subquery without RETURN should not declare variables from YIELD in the outer scope") {
    val query =
      """CALL {
        |  CALL dbms.procedures() YIELD name
        |}
        |RETURN name
        |""".stripMargin

    expectErrorMessagesFrom(
      query,
      Set("Variable `name` not defined")
    )
  }

  test("should fail for size(COUNT{...})") {
    val query = "RETURN size(COUNT{ (n) }) AS foo"
    expectErrorsFrom(
      query,
      Set(SemanticError("Type mismatch: expected String or List<T> but was Integer", InputPosition(12, 1, 13)))
    )
  }

  test("should not allow subquery expressions in MERGE ON CREATE") {
    val query = "MERGE (n) ON CREATE SET n.prop = EXISTS { MATCH () } RETURN 1"

    expectErrorsFrom(
      query,
      Set(SemanticError("Subquery expressions are not allowed in a MERGE clause.", InputPosition(33, 1, 34)))
    )
  }

  test("should not allow subquery expressions in MERGE") {
    val query = "MERGE (n {prop: EXISTS {MATCH ()}}) RETURN n.prop"

    expectErrorsFrom(
      query,
      Set(SemanticError("Subquery expressions are not allowed in a MERGE clause.", InputPosition(16, 1, 17)))
    )
  }

  test("should not allow subquery expressions in MERGE ON SET") {
    val query = "MERGE (n) ON CREATE SET n.prop = COUNT { MATCH () } RETURN 1"

    expectErrorsFrom(
      query,
      Set(SemanticError("Subquery expressions are not allowed in a MERGE clause.", InputPosition(33, 1, 34)))
    )
  }

  test("should allow index hint with negated predicate") {
    val query = "MATCH (a:A) USING INDEX a:A(prop) WHERE NOT a.prop > 123 RETURN 1"
    expectNoErrorsFrom(query)
  }

  test("Should check for undefined variables in type predicate expression") {
    val result = runSemanticAnalysis("MATCH (n) WHERE x IS :: BOOL RETURN 1")
    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `x` not defined", 1, 17)
    ))
  }

  test("Should check for undefined variables in negative type predicate expression") {
    val result = runSemanticAnalysis("MATCH (n) WHERE x IS NOT :: BOOL RETURN 1")
    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `x` not defined", 1, 17)
    ))
  }

  test("should fail for normalize() with incorrect arguments") {
    val query = "RETURN normalize(1) AS normalize"
    expectErrorsFrom(
      query,
      Set(SemanticError("Type mismatch: expected String but was Integer", InputPosition(17, 1, 18)))
    )
  }

  test("Should check for undefined variables in normalized predicate expression") {
    val result = runSemanticAnalysis("MATCH (n) WHERE x IS NORMALIZED RETURN 1")
    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `x` not defined", 1, 17)
    ))
  }

  test("Should check for undefined variables in negative normalized predicate expression") {
    val result = runSemanticAnalysis("MATCH (n) WHERE x IS NOT NORMALIZED RETURN 1")
    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("Variable `x` not defined", 1, 17)
    ))
  }

  test("Should not allow too large lower bound in variable length relationship") {
    val query = "MATCH ()-[*9999999999999999999999999999999999999999999..]->() RETURN 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("integer is too large", InputPosition(11, 1, 12))
      )
    )
  }

  test("Should not allow too large upper bound in variable length relationship") {
    val query = "MATCH ()-[*..9999999999999999999999999999999999999999999]->() RETURN 1"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("integer is too large", InputPosition(13, 1, 14))
      )
    )
  }

  override def messageProvider: ErrorMessageProvider = new ErrorMessageProviderAdapter {
    override def createUseClauseUnsupportedError(): String = "A very nice message explaining why USE is not allowed"

    override def createDynamicGraphReferenceUnsupportedError(graphName: String): String =
      "A very nice message explaining why dynamic graph references are not allowed: " + graphName

    override def createMultipleGraphReferencesError(graphName: String, transactioinalDefault: Boolean = false): String =
      "A very nice message explaining why multiple graph references are not allowed: " + graphName
  }
}
