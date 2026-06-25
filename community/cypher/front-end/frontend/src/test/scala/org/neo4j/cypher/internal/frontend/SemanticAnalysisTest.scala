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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersion.Cypher5
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticError.invalidEntityType
import org.neo4j.cypher.internal.ast.semantics.SemanticError.invalidNumberOfProcedureOrFunctionArguments
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTest.gql42N29
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTest.gql42NA5
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.notification.RepeatedRelationshipReference
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.StorableType.storableType
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation.from
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlHelper.getGql22003
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N39
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N71
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlParams.StringParam.variable
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_42001
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_42I40
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_42N29
import org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_42NA5

class SemanticAnalysisTest extends SemanticAnalysisTestSuite with AstConstructionTestSupport {

  private val useAsMulti = Seq(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.UseAsMultipleGraphsSelector
  )

  private val useAsSingle = Seq(
    SemanticFeature.MultipleGraphs,
    SemanticFeature.UseAsSingleGraphSelector
  )

  private val emptyTokenErrorMessage =
    "'' is not a valid token name. Token names cannot be empty or contain any null-bytes."

  test("should fail for max() with no arguments") {
    val msg = "Insufficient parameters for function 'max'"
    run("RETURN max() AS max")
      .hasErrors(invalidNumberOfProcedureOrFunctionArguments(1, 0, "max", "max(input :: ANY) :: ANY", msg, p(7, 1, 8)))
  }

  test("Should allow overriding variable name in RETURN clause with an ORDER BY") {
    run("MATCH (n) RETURN n.prop AS n ORDER BY n + 2").hasNoErrors
  }

  test("Should not allow multiple columns with the same name in WITH") {
    run("MATCH (n) WITH n.prop AS n, n.foo AS n ORDER BY n + 2 RETURN 1 AS one")
      .hasError(
        GqlHelper.getGql42001_42N38(37, 1, 38),
        "Multiple result columns with the same name are not supported",
        p(37, 1, 38)
      )
  }

  test("Should not allow duplicate variable name in CREATE") {
    run("CREATE (n), (n) RETURN 1 as one")
      .hasError(
        GqlHelper.getGql42001_42N59("n", 13, 1, 14),
        "Variable `n` already declared",
        p(13, 1, 14)
      )
  }

  test("Should not allow Distinct in functions that aren't aggregate") {
    val nonAggregateFunctions = Seq(
      ("localdatetime", "'param1'"),
      ("duration", "'param1'"),
      ("left", "'param1', 4"),
      ("right", "'param1', 4"),
      ("reverse", "'param1'"),
      ("ltrim", "'param1'"),
      ("ceil", "0.1"),
      ("floor", "0.1"),
      ("sign", "0.1"),
      ("round", "0.1"),
      ("abs", "0.1"),
      ("asin", "0.1"),
      ("isEmpty", "'param1'"),
      ("toBoolean", "'param1'")
    )
    nonAggregateFunctions.foreach { case (func, params) =>
      run(s"RETURN $func(DISTINCT $params)")
        .hasError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(7, 1, 8)
            .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I27)
              .atPosition(7, 1, 8)
              .withParam(GqlParams.StringParam.fun, func)
              .build())
            .build(),
          s"Invalid use of DISTINCT with function '$func'",
          p(7, 1, 8)
        )
    }
  }

  test("Should not allow parameter maps in node pattern in MATCH") {
    run("MATCH (n $foo) RETURN 1").hasError(
      GqlHelper.getGql42001_42N32("MATCH", 9, 1, 10),
      "Parameter maps cannot be used in `MATCH` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
      p(9, 1, 10)
    )
  }

  test("Should not allow parameter maps in node pattern in MERGE") {
    run("MERGE (n $foo) RETURN 1").hasError(
      GqlHelper.getGql42001_42N32("MERGE", 9, 1, 10),
      "Parameter maps cannot be used in `MERGE` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
      p(9, 1, 10)
    )
  }

  test("Should not allow parameter maps in relationship pattern in MATCH") {
    run("MATCH (n)-[r $foo]->() RETURN 1").hasError(
      GqlHelper.getGql42001_42N32("MATCH", 13, 1, 14),
      "Parameter maps cannot be used in `MATCH` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
      p(13, 1, 14)
    )
  }

  test("Should not allow parameter maps in relationship pattern in MERGE") {
    run("MERGE (n)-[r:R $foo]->() RETURN 1").hasError(
      GqlHelper.getGql42001_42N32("MERGE", 15, 1, 16),
      "Parameter maps cannot be used in `MERGE` patterns (use a literal map instead, e.g. `{id: $foo.id}`)",
      p(15, 1, 16)
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
      analyse(query)
        .withParam("p", CTAny, literal("hello"))
        .run
        .hasNoErrors
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
    queries.foreach(query => run(query).hasNoErrors)
  }

  // Escaped backticks in tokens

  test("Should allow escaped backticks in node property key name") {
    // Property without escaping: `abc123``
    run("CREATE ({prop: 5, ```abc123`````: 1})").hasNoErrors
  }

  test("Should allow escaped backticks in relationship property key name") {
    // Property without escaping: abc`123
    run("MATCH ()-[r]->() RETURN r.`abc``123` as result").hasNoErrors
  }

  test("Should allow escaped backticks in label") {
    // Label without escaping: `abc123
    run("MATCH (n) SET n:```abc123`").hasNoErrors
  }

  test("Should allow escaped backtick in relationship type") {
    // Relationship type without escaping: abc123``
    run("MERGE ()-[r:`abc123`````]->()").hasNoErrors
  }

  test("Should allow escaped backtick in indexes") {
    // Query without proper escaping: CREATE INDEX `abc`123`` FOR (n:`Per`son`) ON (n.first``name`, n.``last`name)
    run("CREATE INDEX ```abc``123````` FOR (n:```Per``son```) ON (n.`first````name```, n.`````last``name`)")
      .hasNoErrors
  }

  test("Should allow escaped backtick in constraints") {
    // Query without proper escaping: CREATE CONSTRAINT abc123` FOR (n:``Label) REQUIRE (n.pr``op) IS NODE KEY
    run("CREATE CONSTRAINT `abc123``` FOR (n:`````Label`) REQUIRE (n.`pr````op`) IS NODE KEY")
      .hasNoErrors
  }

  test("Should register uses in PathExpressions") {
    analyse("MATCH p = (a)-[r]-(b) RETURN p AS p")
      .withPipeline(ProjectNamedPathsPhase andThen SemanticAnalysis(warn = Some(true)))
      .run
      .assert { result =>
        Set("a", "r", "b").foreach { name =>
          result.state.semantics().scopeTree.allSymbols(name).head.uses shouldNot be(empty)
        }
      }
  }

  test("should allow node pattern predicates in MATCH") {
    run(
      "WITH 123 AS minValue MATCH (n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) RETURN n AS result"
    ).hasNoErrors
  }

  test("should allow node pattern predicates in MATCH to refer to other nodes") {
    run("MATCH (start)-->(end:Label WHERE start.prop = 42) RETURN start AS result")
      .hasNoErrors
  }

  test("should allow node pattern predicates in shortest path to refer to other nodes") {
    run("MATCH (a), (b) MATCH shortestPath( (a)-->(b WHERE c.prop = 42) ), (c) RETURN count(*) AS result")
      .hasNoErrors
  }

  test("should allow node property predicates in shortest path to refer to other nodes") {
    run("MATCH (a), (b) MATCH shortestPath( (a)-->(b {prop: c.prop}) ), (c) RETURN count(*) AS result")
      .hasNoErrors
  }

  test("should not allow node pattern predicates in CREATE") {
    run("CREATE (n WHERE n.prop = 123)").hasAllGQLErrorsIn(_ =>
      Seq(
        (
          GqlHelper.getGql42001_42I32("a CREATE clause", 23, 1, 24),
          "Node pattern predicates are not allowed in a CREATE clause, but only in a MATCH clause or inside a pattern comprehension",
          p(23, 1, 24)
        )
      )
    )
  }

  test("should not allow node pattern predicates in MERGE") {
    run("MERGE (n WHERE n.prop = 123)").hasAllGQLErrorsIn(_ =>
      Seq(
        (
          GqlHelper.getGql42001_42I32("a MERGE clause", 22, 1, 23),
          "Node pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension",
          p(22, 1, 23)
        )
      )
    )
  }

  test("should allow node pattern predicates in pattern comprehension") {
    run(
      "WITH 123 AS minValue RETURN [(n {prop: 42} WHERE n.otherProp > minValue)-->(m:Label WHERE m.prop = 42) | n] AS result"
    ).hasNoErrors
  }

  test("should allow node pattern predicates in pattern comprehension to refer to other nodes") {
    run("RETURN [(start)-->(end:Label WHERE start.prop = 42) | start] AS result")
      .hasNoErrors
  }

  test("should not allow node pattern predicates in pattern expression") {
    run("""MATCH (a), (b)
          |RETURN exists((a WHERE a.prop > 123)-->(b)) AS result""".stripMargin)
      .hasError(
        GqlHelper.getGql42001_42I32("an expression", 45, 2, 31),
        "Node pattern predicates are not allowed in an expression, but only in a MATCH clause or inside a pattern comprehension",
        p(45, 2, 31)
      )
  }

  test("should allow node pattern predicates in MATCH with shortestPath") {
    run(
      """
        |WITH 123 AS minValue
        |MATCH p = shortestPath((n {prop: 42} WHERE n.otherProp > minValue)-[:REL*]->(m:Label WHERE m.prop = 42))
        |RETURN n AS result""".stripMargin
    ).hasNoErrors
  }

  test("should allow node pattern predicates in MATCH with shortestPath to refer to other nodes") {
    run("""
          |MATCH p = shortestPath((start)-[:REL*]->(end:Label WHERE start.prop = 42))
          |RETURN start AS result""".stripMargin)
      .hasNoErrors
  }

  test("should not allow node pattern predicates in shortestPath expression") {
    run("""
          |MATCH (a), (b)
          |WITH shortestPath((a WHERE a.prop > 123)-[:REL*]->(b)) AS p
          |RETURN length(p) AS result""".stripMargin)
      .hasError(
        GqlHelper.getGql42001_42I32("an expression", 50, 3, 35),
        "Node pattern predicates are not allowed in an expression, but only in a MATCH clause or inside a pattern comprehension",
        p(50, 3, 35)
      )
  }

  test("should allow relationship pattern predicates in MATCH") {
    run(
      "WITH 123 AS minValue MATCH (n)-[r:Relationship {prop: 42} WHERE r.otherProp > minValue]->(m) RETURN r AS result"
    ).hasNoErrors
  }

  test("should not allow relationship pattern predicates in MATCH when path length is provided") {
    run(
      "WITH 123 AS minValue MATCH (n)-[r:Relationship*1..3 {prop: 42} WHERE r.otherProp > minValue]->(m) RETURN r AS result"
    ).hasAllGQLErrorsIn(_ =>
      Seq((
        GqlHelper.getGql42001_42N37(81, 1, 82),
        "Relationship pattern predicates are not supported for variable-length relationships.",
        p(81, 1, 82)
      ))
    )
  }

  test("should allow relationship pattern predicates in MATCH to refer to nodes") {
    run("MATCH (n)-[r:Relationship WHERE n.prop = 42]->(m:Label) RETURN r AS result")
      .hasNoErrors
  }

  test("should not allow relationship pattern predicates in CREATE") {
    run("CREATE (n)-[r:Relationship WHERE r.prop = 42]->(m)").hasAllGQLErrorsIn(_ =>
      Seq((
        GqlHelper.getGql42001_42I32("a CREATE clause", 40, 1, 41),
        "Relationship pattern predicates are not allowed in a CREATE clause, but only in a MATCH clause or inside a pattern comprehension",
        p(40, 1, 41)
      ))
    )
  }

  test("should not allow relationship pattern predicates in MERGE") {
    run("MERGE (n)-[r:Relationship WHERE r.prop = 42]->(m)").hasAllGQLErrorsIn(_ =>
      Seq((
        GqlHelper.getGql42001_42I32("a MERGE clause", 39, 1, 40),
        "Relationship pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension",
        p(39, 1, 40)
      ))
    )
  }

  test("should allow relationship pattern predicates in pattern comprehension") {
    run("WITH 123 AS minValue RETURN [(n)-[r:Relationship {prop: 42} WHERE r.otherProp > minValue]->(m) | r] AS result")
      .hasNoErrors
  }

  test("should allow relationship pattern predicates in pattern comprehension to refer to nodes") {
    run("RETURN [(n)-[r:Relationship WHERE n.prop = 42]->(m:Label) | r] AS result")
      .hasNoErrors
  }

  test("should not allow relationship pattern predicates in pattern expression") {
    run(
      """MATCH (a)-[r]->(b)
        |RETURN exists((a)-[r WHERE r.prop > 123]->(b)) AS result""".stripMargin
    ).hasError(
      GqlHelper.getGql42001_42I32("an expression", 53, 2, 35),
      "Relationship pattern predicates are not allowed in an expression, but only in a MATCH clause or inside a pattern comprehension",
      p(53, 2, 35)
    )
  }

  test("redundant composite use clause should not be considered as nested use clause") {
    val query = "USE comp CALL { USE comp.c1 RETURN 1 as n } RETURN n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("redundant composite use clause should not be considered as nested use clause and scope clause") {
    val query = "USE comp CALL () { USE comp.c1 RETURN 1 as n } RETURN n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("redundant composite use clause should not be considered as nested use clause (case insensitive)") {
    val query = "USE comp CALL { USE COMP.c1 RETURN 1 as n } RETURN n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("nesting after redundant composite use clause should be considered as nesting") {
    val query =
      """USE comp
        |CALL {
        |  USE comp.c1
        |  CALL {
        |    USE comp.c2
        |    RETURN 1 as n
        |  }
        |  RETURN n
        |}
        |RETURN n""".stripMargin
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasError(
        GqlHelper.getGql42001_42N74(47, 5, 9, "comp.c2", "comp.c1"),
        "Nested subqueries must use the same graph as their parent query",
        p(47, 5, 9)
      )
  }

  test("redundant composite use clause should not be considered as nested use clause with graph function") {
    val query = "USE comp CALL { USE graph.byName('c1') RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test(
    "redundant composite use clause should not be considered as nested use clause with graph function, scope clause"
  ) {
    val query = "USE comp CALL () { USE graph.byName('c1') RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("identical static graph target should not be considered as nested use clause (case insensitive)") {
    val query = "USE comp.c1 CALL { USE COMP.c1 RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("identical dynamic graph target should not be considered as nested use clause") {
    val query = "USE graph.byName('comp.c2') CALL { USE graph.byName('comp.c2') RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("throw nested use clause exception with nested static use clauses") {
    val query = "USE comp.c1 CALL { USE comp.c1.c2 RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasError(
        GqlHelper.getGql42001_42N74(23, 1, 24, "comp.c1.c2", "comp.c1"),
        "Nested subqueries must use the same graph as their parent query",
        p(23, 1, 24)
      )
  }

  test("throw nested use clause exception with nested dynamic use clauses") {
    val query = "USE graph.byName('comp.c1') CALL { USE graph.byName('comp.c2') RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasError(
        GqlHelper.getGql42001_42N74(39, 1, 40, "graph.byName(\"comp.c2\")", "graph.byName(\"comp.c1\")"),
        "Nested subqueries must use the same graph as their parent query",
        p(39, 1, 40)
      )
  }

  test("throw nested use clause exception with nested proper dynamic use clauses") {
    val query =
      """WITH "local" AS i
        | CALL {
        |      WITH i
        |      USE graph.byName("comp." + i)
        |      WITH "neo4j" AS i
        |      CALL {
        |           WITH i
        |           USE graph.byName("comp." + i)
        |           MATCH (n)
        |           RETURN n.prop as prop
        |      }
        |      RETURN prop
        |}
        |RETURN prop""".stripMargin
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasError(
        GqlHelper.getGql42001_42N74(145, 8, 16, "graph.byName(\"comp.\" + i)", "graph.byName(\"comp.\" + i)"),
        "Nested subqueries must use the same graph as their parent query",
        p(145, 8, 16)
      )
  }

  test("throw nested use clause exception with nested proper dynamic use clauses with scope clause") {
    val query =
      """WITH "local" AS i
        | CALL (i) {
        |      USE graph.byName("comp." + i)
        |      WITH "neo4j" AS j
        |      CALL (j) {
        |           USE graph.byName("comp." + j)
        |           MATCH (n)
        |           RETURN n.prop as prop
        |      }
        |      RETURN prop
        |}
        |RETURN prop""".stripMargin
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasError(
        GqlHelper.getGql42001_42N74(122, 6, 16, "graph.byName(\"comp.\" + j)", "graph.byName(\"comp.\" + i)"),
        "Nested subqueries must use the same graph as their parent query",
        p(122, 6, 16)
      )
  }

  test("allow nested use clause exception if inner catalog name has more than a length of 2") {
    val query = "USE comp CALL { USE comp.c1.c2 RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsMulti, isComposite = true, sessionDatabase = "comp")
      .hasNoErrors
  }

  test("throw exception multi database on single graph selector") {
    val query = "USE comp CALL { USE comp.c1 RETURN 1 as n } return n"
    run(query, semanticFeatures = useAsSingle, isComposite = true, sessionDatabase = "neo4j").hasError(
      gql42NA5(p(16, 1, 17)),
      messageProvider.createMultipleGraphReferencesError("comp.c1"),
      p(16, 1, 17)
    )
  }

  test("should not allow USE when semantic feature is not set") {
    run("USE g RETURN 1", semanticFeatures = Seq(SemanticFeature.MultipleGraphs))
      .hasErrorMessages(messageProvider.createUseClauseUnsupportedError())
  }

  test("should allow USE when UseAsMultipleGraphsSelector feature is set") {
    run("USE g RETURN 1", semanticFeatures = useAsMulti).hasNoErrors
  }

  test("should allow USE when UseAsSingleGraphSelector feature is set") {
    run("USE g RETURN 1", semanticFeatures = useAsSingle).hasNoErrors
  }

  test("Allow qualified identifier in USE when UseAsMultipleGraphsSelector feature is set") {
    run("USE x.y.z RETURN 1", semanticFeatures = useAsMulti)
      .hasNoErrors
  }

  test("Allow qualified identifier in USE when UseAsSingleGraphSelector feature is set") {
    run("USE x.y.z RETURN 1", semanticFeatures = useAsSingle)
      .hasNoErrors
  }

  test("Allow view invocation in USE when UseAsMultipleGraphsSelector feature is set") {
    val query =
      """
        |USE graph.byName($g, w($k))
        |RETURN 1
        |""".stripMargin
    run(query, semanticFeatures = useAsMulti, isComposite = true)
      .hasNoErrors
  }

  test("Don't allow view invocation in USE when UseAsSingleGraphSelector feature is set") {
    val query =
      """
        |USE graph.byName($g, w($k))
        |RETURN 1
        |""".stripMargin
    run(query, semanticFeatures = useAsSingle).hasError(
      GqlHelper.getGql42001_42N72(1, 2, 1),
      messageProvider.createDynamicGraphReferenceUnsupportedError("graph.byName($g, w($k))"),
      p(1, 2, 1)
    )
  }

  test("Allow qualified view invocation in USE") {
    val query =
      """
        |USE graph.byName($g, x.g(), x.v($k))
        |RETURN 1
        |""".stripMargin
    run(query, semanticFeatures = useAsMulti, isComposite = true).hasNoErrors
  }

  test("Allow expressions in view invocations (with feature flag)") {
    val query = "USE graph.byName(2, 'x', $x, $x+3) RETURN 1"
    run(query, semanticFeatures = useAsMulti, isComposite = true).hasNoErrors
  }

  test("Expressions in view invocations are checked (with feature flag)") {
    val query = "USE graph.byName(2, 'x', y, $x+3) RETURN 1"
    run(query, semanticFeatures = useAsMulti, isComposite = true).hasError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(25, 1, 26)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
          .atPosition(25, 1, 26)
          .withParam(GqlParams.StringParam.variable, "y")
          .build())
        .build(),
      "Variable `y` not defined",
      p(25, 1, 26)
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
    run(query, semanticFeatures = useAsSingle).hasNoErrors
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

    run(query, semanticFeatures = useAsSingle).hasNoErrors
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

    run(query, semanticFeatures = useAsSingle).hasError(
      gql42NA5(p(22, 5, 1)),
      messageProvider.createMultipleGraphReferencesError("y"),
      p(22, 5, 1)
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

    run(query, semanticFeatures = useAsSingle).hasError(
      gql42NA5(p(29, 5, 5)),
      messageProvider.createMultipleGraphReferencesError("B"),
      p(29, 5, 5)
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

    run(query, semanticFeatures = useAsSingle).hasError(
      gql42NA5(p(27, 5, 5)),
      messageProvider.createMultipleGraphReferencesError("B"),
      p(27, 5, 5)
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

    run(query, semanticFeatures = useAsSingle).hasNoErrors
  }

  test("should allow USE only in leading position") {
    val query =
      """
        |MATCH (n)
        |USE g
        |RETURN n
        |""".stripMargin
    run(query, semanticFeatures = useAsSingle).hasError(
      GqlHelper.getGql42001_42N73(11, 3, 1),
      "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
      p(11, 3, 1)
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

    run(query, semanticFeatures = useAsSingle).hasError(
      GqlHelper.getGql42001_42N73(13, 3, 1),
      "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
      p(13, 3, 1)
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
    run(query, semanticFeatures = useAsSingle).hasError(
      GqlHelper.getGql42001_42N73(11, 3, 1),
      "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
      p(11, 3, 1)
    )
  }

  // Positive tests for 42N44 ("In a WITH/RETURN with DISTINCT or an aggregation,
  // it is not possible to access variables declared before the WITH/RETURN")
  // have moved to GQL_42N44_InaccessibleVariableTest — 42N44 is emitted by
  // AggregationAnalysis, not SemanticAnalysis alone.

  // negative tests that we do not get this error message otherwise

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH DISTINCT") {
    run("MATCH (p) WITH DISTINCT p.email AS mail ORDER BY q.name RETURN mail AS mail")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(49, 1, 50)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(49, 1, 50)
            .withParam(GqlParams.StringParam.variable, "q")
            .build())
          .build(),
        "Variable `q` not defined",
        p(49, 1, 50)
      )
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after WITH with aggregation") {
    run("MATCH (p) WITH collect(p.email) AS mail ORDER BY q.name RETURN mail AS mail")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(49, 1, 50)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(49, 1, 50)
            .withParam(GqlParams.StringParam.variable, "q")
            .build())
          .build(),
        "Variable `q` not defined",
        p(49, 1, 50)
      )
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN DISTINCT") {
    run("MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY q.name")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(51, 1, 52)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(51, 1, 52)
            .withParam(GqlParams.StringParam.variable, "q")
            .build())
          .build(),
        "Variable `q` not defined",
        p(51, 1, 52)
      )
  }

  test("Should not invent helpful error when accessing undefined variable in ORDER BY after RETURN with aggregation") {
    run("MATCH (p) RETURN collect(p.email) AS mail ORDER BY q.name")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(51, 1, 52)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(51, 1, 52)
            .withParam(GqlParams.StringParam.variable, "q")
            .build())
          .build(),
        "Variable `q` not defined",
        p(51, 1, 52)
      )
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH DISTINCT") {
    run("MATCH (p) WITH DISTINCT p.email AS mail WHERE q.name IS NOT NULL RETURN mail AS mail")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(46, 1, 47)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(46, 1, 47)
            .withParam(GqlParams.StringParam.variable, "q")
            .build())
          .build(),
        "Variable `q` not defined",
        p(46, 1, 47)
      )
  }

  test("Should not invent helpful error when accessing undefined variable in WHERE after WITH with aggregation") {
    run("MATCH (p) WITH collect(p.email) AS mail WHERE q.name IS NOT NULL RETURN mail AS mail")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(46, 1, 47)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(46, 1, 47)
            .withParam(GqlParams.StringParam.variable, "q")
            .build())
          .build(),
        "Variable `q` not defined",
        p(46, 1, 47)
      )
  }

  // Empty tokens for node property

  test("Should not allow empty node property key name in CREATE clause") {
    run("CREATE ({prop: 5, ``: 1})")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 18, 1, 19),
        emptyTokenErrorMessage,
        p(18, 1, 19)
      )
  }

  test("Should not allow empty node property key name in MERGE clause") {
    run("MERGE (n {``: 1})")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 10, 1, 11),
        emptyTokenErrorMessage,
        p(10, 1, 11)
      )
  }

  test("Should not allow empty node property key name in ON CREATE SET") {
    run("MERGE (n :Label) ON CREATE SET n.`` = 1")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 33, 1, 34),
        emptyTokenErrorMessage,
        p(33, 1, 34)
      )
  }

  test("Should not allow empty node property key name in ON MATCH SET") {
    run("MERGE (n :Label) ON MATCH SET n.`` = 1")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 32, 1, 33),
        emptyTokenErrorMessage,
        p(32, 1, 33)
      )
  }

  test("Should not allow empty node property key name in MATCH clause") {
    run("MATCH (n {``: 1}) RETURN n AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 10, 1, 11),
        emptyTokenErrorMessage,
        p(10, 1, 11)
      )
  }

  test("Should not allow empty node property key name in SET clause") {
    run("MATCH (n) SET n.``= 1")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 16, 1, 17),
        emptyTokenErrorMessage,
        p(16, 1, 17)
      )
  }

  test("Should not allow empty node property key name in REMOVE clause") {
    run("MATCH (n) REMOVE n.``")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 19, 1, 20),
        emptyTokenErrorMessage,
        p(19, 1, 20)
      )
  }

  test("Should not allow empty node property key name in WHERE clause") {
    run("MATCH (n) WHERE n.``= 1 RETURN n AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 18, 1, 19),
        emptyTokenErrorMessage,
        p(18, 1, 19)
      )
  }

  test("Should not allow empty node property key name in WITH clause") {
    run("MATCH (n) WITH n.`` AS prop RETURN prop AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 17, 1, 18),
        emptyTokenErrorMessage,
        p(17, 1, 18)
      )
  }

  test("Should not allow empty node property key name in ORDER BY in WITH") {
    run("MATCH (n) WITH n AS invalid ORDER BY n.`` RETURN count(*) AS count")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 39, 1, 40),
        emptyTokenErrorMessage,
        p(39, 1, 40)
      )
  }

  test("Should not allow empty node property key name in RETURN clause") {
    run("MATCH (n) RETURN n.`` AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 19, 1, 20),
        emptyTokenErrorMessage,
        p(19, 1, 20)
      )
  }

  test("Should not allow empty node property key name in DISTINCT RETURN clause") {
    run("MATCH (n) RETURN DISTINCT n.`` AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 28, 1, 29),
        emptyTokenErrorMessage,
        p(28, 1, 29)
      )
  }

  test("Should not allow empty node property key name in aggregation in RETURN clause") {
    run("MATCH (n) RETURN count(n.``) AS count")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 25, 1, 26),
        emptyTokenErrorMessage,
        p(25, 1, 26)
      )
  }

  test("Should not allow empty node property key name in ORDER BY in RETURN") {
    run("MATCH (n) RETURN n AS invalid ORDER BY n.`` DESC LIMIT 2")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 41, 1, 42),
        emptyTokenErrorMessage,
        p(41, 1, 42)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 25, 4, 8),
        emptyTokenErrorMessage,
        p(25, 4, 8)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 30, 5, 8),
        emptyTokenErrorMessage,
        p(30, 5, 8)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 51, 6, 8),
        emptyTokenErrorMessage,
        p(51, 6, 8)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 58, 7, 8),
        emptyTokenErrorMessage,
        p(58, 7, 8)
      )
  }

  // Empty tokens for relationship properties

  test("Should not allow empty relationship property key name in CREATE clause") {
    run("CREATE ()-[:REL {``: 1}]->()")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 17, 1, 18),
        emptyTokenErrorMessage,
        p(17, 1, 18)
      )
  }

  test("Should not allow empty relationship property key name in MERGE clause") {
    run("MERGE ()-[r :REL {``: 1, prop: 42}]->()")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 18, 1, 19),
        emptyTokenErrorMessage,
        p(18, 1, 19)
      )
  }

  test("Should not allow empty relationship property key name in ON CREATE SET") {
    run("MERGE ()-[r:REL]->() ON CREATE SET r.`` = 1")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 37, 1, 38),
        emptyTokenErrorMessage,
        p(37, 1, 38)
      )
  }

  test("Should not allow empty relationship property key name in ON MATCH SET") {
    run("MERGE ()-[r:REL]->() ON MATCH SET r.`` = 1")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 36, 1, 37),
        emptyTokenErrorMessage,
        p(36, 1, 37)
      )
  }

  test("Should not allow empty relationship property key name in MATCH clause") {
    run("MATCH ()-[r {prop:1337, ``: 1}]->() RETURN r AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 24, 1, 25),
        emptyTokenErrorMessage,
        p(24, 1, 25)
      )
  }

  test("Should not allow empty relationship property key name in SET clause") {
    run("MATCH ()-[r]->() SET r.``= 1 RETURN r AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 23, 1, 24),
        emptyTokenErrorMessage,
        p(23, 1, 24)
      )
  }

  test("Should not allow empty relationship property key name in REMOVE clause") {
    run("MATCH ()-[r]->() REMOVE r.``")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 26, 1, 27),
        emptyTokenErrorMessage,
        p(26, 1, 27)
      )
  }

  test("Should not allow empty relationship property key name in WHERE clause") {
    run("MATCH (n)-[r]->() WHERE n.prop > r.`` RETURN n AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 35, 1, 36),
        emptyTokenErrorMessage,
        p(35, 1, 36)
      )
  }

  test("Should not allow empty relationship property key name in WITH clause") {
    run("MATCH ()-[r]->() WITH r.`` AS prop, r.prop as prop2 RETURN count(*) AS count")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 24, 1, 25),
        emptyTokenErrorMessage,
        p(24, 1, 25)
      )
  }

  test("Should not allow empty relationship property key name in ORDER BY in WITH") {
    run("MATCH ()-[r]->() WITH r AS invalid ORDER BY r.`` RETURN count(*) AS count")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 46, 1, 47),
        emptyTokenErrorMessage,
        p(46, 1, 47)
      )
  }

  test("Should not allow empty relationship property key name in RETURN clause") {
    run("MATCH ()-[r]->() RETURN r.`` as result")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 26, 1, 27),
        emptyTokenErrorMessage,
        p(26, 1, 27)
      )
  }

  test("Should not allow empty relationship property key name in DISTINCT RETURN clause") {
    run("MATCH ()-[r]->() RETURN DISTINCT r.`` as result")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 35, 1, 36),
        emptyTokenErrorMessage,
        p(35, 1, 36)
      )
  }

  test("Should not allow empty relationship property key name in aggregation in RETURN clause") {
    run("MATCH ()-[r]->() RETURN max(r.``) AS max")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 30, 1, 31),
        emptyTokenErrorMessage,
        p(30, 1, 31)
      )
  }

  test("Should not allow empty relationship property key name in ORDER BY in RETURN") {
    run("MATCH ()-[r]->() RETURN r AS result ORDER BY r.``")
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 47, 1, 48),
        emptyTokenErrorMessage,
        p(47, 1, 48)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 32, 4, 8),
        emptyTokenErrorMessage,
        p(32, 4, 8)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 37, 5, 8),
        emptyTokenErrorMessage,
        p(37, 5, 8)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 58, 6, 8),
        emptyTokenErrorMessage,
        p(58, 6, 8)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I11("property key", "", 65, 7, 8),
        emptyTokenErrorMessage,
        p(65, 7, 8)
      )
  }

  // Empty tokens for labels

  test("Should not allow empty label in CREATE clause") {
    run("CREATE (:Valid:``)")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 14, 1, 15),
        emptyTokenErrorMessage,
        p(14, 1, 15)
      )
  }

  test("Should not allow empty label in MERGE clause") {
    run("MERGE (n:``)")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 9, 1, 10),
        emptyTokenErrorMessage,
        p(9, 1, 10)
      )
  }

  test("Should not allow empty label in MATCH clause") {
    run("MATCH (n:``:Valid) RETURN n AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 11, 1, 12),
        emptyTokenErrorMessage,
        p(11, 1, 12)
      )
  }

  test("Should not allow empty label in label expression") {
    run("MATCH (n:``&Valid) RETURN n AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 11, 1, 12),
        emptyTokenErrorMessage,
        p(11, 1, 12)
      )
  }

  test("should not allow empty label name in label expression predicate") {
    run("MATCH (n) WHERE n:A&`` RETURN *")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 19, 1, 20),
        emptyTokenErrorMessage,
        p(19, 1, 20)
      )
  }

  test("should not allow empty label name in label expression with legacy symbols") {
    run("MATCH (n) WHERE n:A:`` RETURN *")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 19, 1, 20),
        emptyTokenErrorMessage,
        p(19, 1, 20)
      )
  }

  test("Should not allow empty label in SET clause") {
    run("MATCH (n) SET n:``")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 14, 1, 15),
        emptyTokenErrorMessage,
        p(14, 1, 15)
      )
  }

  test("Should not allow empty label in REMOVE clause") {
    run("MATCH (n) REMOVE n:``")
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", 17, 1, 18),
        emptyTokenErrorMessage,
        p(17, 1, 18)
      )
  }

  // Empty tokens for relationship type

  test("Should not allow empty relationship type in CREATE clause") {
    run("CREATE ()-[:``]->()")
      .hasError(
        GqlHelper.getGql42001_42I11("relationship type", "", 12, 1, 13),
        emptyTokenErrorMessage,
        p(12, 1, 13)
      )
  }

  test("Should not allow empty relationship type in MERGE clause") {
    run("MERGE ()-[r :``]->()")
      .hasError(
        GqlHelper.getGql42001_42I11("relationship type", "", 13, 1, 14),
        emptyTokenErrorMessage,
        p(13, 1, 14)
      )
  }

  test("Should not allow empty relationship type in MATCH clause") {
    run("MATCH ()-[r :``]->() RETURN r AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("relationship type", "", 13, 1, 14),
        emptyTokenErrorMessage,
        p(13, 1, 14)
      )
  }

  test("Should not allow empty relationship type in variable length pattern") {
    run("MATCH ()-[r :``*1..5]->() RETURN r AS invalid")
      .hasError(
        GqlHelper.getGql42001_42I11("relationship type", "", 13, 1, 14),
        emptyTokenErrorMessage,
        p(13, 1, 14)
      )
  }

  test("Should not allow to use aggregate functions inside aggregate functions") {
    val position = p(23, 1, 24)
    run("WITH 1 AS x RETURN sum(max(x)) AS sumOfMax")
      .hasErrors(SemanticError(
        GqlHelper.getGql42001_42I24("max(x)", position.offset, position.line, position.column),
        "Can't use aggregate functions inside of aggregate functions.",
        position
      ))
  }

  test("Should not allow to use count(*) inside aggregate functions") {
    val position = p(23, 1, 24)
    run("WITH 1 AS x RETURN min(count(*)) AS minOfCount")
      .hasErrors(SemanticError(
        GqlHelper.getGql42001_42I24("count(*)", position.offset, position.line, position.column),
        "Can't use aggregate functions inside of aggregate functions.",
        position
      ))
  }

  test("Should allow to use aggregate functions inside a scalar subquery inside an aggregate functions") {
    val position = p(23, 1, 24)
    run("WITH 1 AS x RETURN sum(max(x)) AS sumOfMax")
      .hasErrors(SemanticError(
        GqlHelper.getGql42001_42I24("max(x)", position.offset, position.line, position.column),
        "Can't use aggregate functions inside of aggregate functions.",
        position
      ))
  }

  for {
    scalar <- Seq("EXISTS", "COUNT", "COLLECT")
    agg <- Seq("count(*)", "count(x)", "sum(x)", "min(x)", "max(x)")
  } {
    test(s"Should allow repeating rel variable in pattern for $agg in $scalar") {
      run(
        s"""RETURN count($scalar {
           |  UNWIND [1, 2, 3] AS x
           |  RETURN $agg
           |}) AS fine""".stripMargin
      ).hasNoErrors
    }
  }

  test("Should allow repeating rel variable in comma separated patterns") {
    run("MATCH ()-[r]-(), ()-[r]-() RETURN r AS r").hasNotifications(
      RepeatedRelationshipReference(
        p(10, 1, 11),
        "r",
        "()-[r]-(), ()-[r]-()"
      )
    )
  }

  test("Should allow repeating rel variable in comma separated paths") {
    run("MATCH p = ()-[r]-(), q = ()-[r]-() RETURN p, q").hasNotifications(
      RepeatedRelationshipReference(
        p(14, 1, 15),
        "r",
        "p = ()-[r]-(), q = ()-[r]-()"
      )
    )
  }

  test("Should allow repeated rel variable in pattern expression") {
    run("MATCH ()-[r]-() RETURN size( ()-[r]-()-[r]-() ) AS size").hasNotifications(
      RepeatedRelationshipReference(
        p(33, 1, 34),
        "r",
        "()-[r]-()-[r]-()"
      )
    )
  }

  test("Should allow repeated rel variable in pattern comprehension") {
    run("MATCH ()-[r]-() RETURN [ ()-[r]-()-[r]-() | r ] AS rs").hasNotifications(
      RepeatedRelationshipReference(
        p(29, 1, 30),
        "r",
        "()-[r]-()-[r]-()"
      )
    )
  }

  test("Should type check predicates in FilteringExpression") {
    val queries = Seq(
      ("RETURN [x IN [1,2,3] WHERE 42 | x + 1] AS foo", p(27, 1, 28).withInputLength(2)),
      ("RETURN all(x IN [1,2,3] WHERE 42) AS foo", p(30, 1, 31).withInputLength(2)),
      ("RETURN any(x IN [1,2,3] WHERE 42) AS foo", p(30, 1, 31).withInputLength(2)),
      ("RETURN none(x IN [1,2,3] WHERE 42) AS foo", p(31, 1, 32).withInputLength(2)),
      ("RETURN single(x IN [1,2,3] WHERE 42) AS foo", p(33, 1, 34).withInputLength(2))
    )
    queries.foreach { case (query, pos) =>
      val msg = "Type mismatch: expected Boolean but was Integer"
      run(query).hasErrors(
        SemanticError.typeMismatch(
          List("BOOLEAN"),
          "INTEGER",
          msg,
          pos
        )
      )
    }
  }

  test("Should disallow introducing variables in pattern expressions") {
    run("MATCH (x) WHERE (x)-[r]-(y) RETURN x").hasAllGQLErrorsIn(_ =>
      Seq(
        (
          gql42N29(p(21, 1, 22), "r"),
          "PatternExpressions are not allowed to introduce new variables: 'r'.",
          p(21, 1, 22)
        ),
        (
          gql42N29(p(25, 1, 26), "y"),
          "PatternExpressions are not allowed to introduce new variables: 'y'.",
          p(25, 1, 26)
        )
      )
    )
  }

  Seq("SKIP", "LIMIT").foreach { phrase =>
    test(s"$phrase with variables should complain") {
      run(s"MATCH (a) RETURN * $phrase a.prop").hasError(
        GqlHelper.getGql42001_42N28(phrase, 20 + phrase.length, 1, 21 + phrase.length),
        s"It is not allowed to refer to variables in $phrase, so that the value for $phrase can be statically calculated.",
        p(20 + phrase.length, 1, 21 + phrase.length)
      )
    }
    test(s"$phrase with PatternComprehension should complain") {
      run(s"RETURN 1 $phrase size([(a)-->(b) | a.prop])").hasError(
        GqlHelper.getGql42001_42N28(phrase, 10 + phrase.length, 1, 11 + phrase.length),
        s"It is not allowed to use patterns in the expression for $phrase, so that the value for $phrase can be statically calculated.",
        p(10 + phrase.length, 1, 11 + phrase.length)
      )
    }

    test(s"$phrase with PatternExpression should complain") {
      run(s"RETURN 1 $phrase size(()-->())").hasErrors(
        SemanticError(
          GqlHelper.getGql42001_42N28(phrase, 10 + phrase.length, 1, 11 + phrase.length),
          s"It is not allowed to use patterns in the expression for $phrase, so that the value for $phrase can be statically calculated.",
          p(10 + phrase.length, 1, 11 + phrase.length)
        ),
        SemanticError.patternExpressionInSize(p(15 + phrase.length, 1, 16 + phrase.length))
      )
    }

    test(s"$phrase with CountExpression should complain") {
      run(s"RETURN 1 $phrase COUNT { ()--() }").hasError(
        GqlHelper.getGql42001_42N28(phrase, 10 + phrase.length, 1, 11 + phrase.length),
        s"It is not allowed to use patterns in the expression for $phrase, so that the value for $phrase can be statically calculated.",
        p(10 + phrase.length, 1, 11 + phrase.length)
      )
    }
  }

  Seq("", " DISTINCT", " ALL").foreach { setQuantifier =>
    test(s"UNION$setQuantifier with incomplete first part") {
      run(s"MATCH (a) WITH a UNION$setQuantifier MATCH (a) RETURN a").hasErrors(
        getGql42001_42N71(10, 1, 11),
        "Query cannot conclude with WITH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
        p(10, 1, 11),
        getGql42001_42N39(Union.errorParam, 17, 1, 18),
        "All sub queries in an UNION must have the same return column names",
        p(17, 1, 18)
      )
    }

    test(s"UNION$setQuantifier with incomplete second part") {
      val extraLength = setQuantifier.length
      run(s"MATCH (a) RETURN a UNION$setQuantifier MATCH (a) WITH a").hasErrors(
        getGql42001_42N71(35 + extraLength, 1, 36 + extraLength),
        "Query cannot conclude with WITH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
        p(35 + extraLength, 1, 36 + extraLength),
        getGql42001_42N39(Union.errorParam, 19, 1, 20),
        "All sub queries in an UNION must have the same return column names",
        p(19, 1, 20)
      )
    }

    test(s"UNION$setQuantifier with missing return in first part") {
      run(s"CALL db.labels() YIELD label UNION$setQuantifier CALL db.labels() YIELD label RETURN label").hasError(
        getGql42001_42N39(Union.errorParam, 29, 1, 30),
        "All sub queries in an UNION must have the same return column names",
        p(29, 1, 30)
      )
    }

    test(s"UNION$setQuantifier with missing return in second part") {
      run(s"CALL db.labels() YIELD label RETURN label UNION$setQuantifier CALL db.labels() YIELD label").hasError(
        getGql42001_42N39(Union.errorParam, 42, 1, 43),
        "All sub queries in an UNION must have the same return column names",
        p(42, 1, 43)
      )
    }

    test(s"UNION$setQuantifier with finish in first part") {
      run(s"UNWIND [1,2] AS a FINISH UNION$setQuantifier UNWIND [2,3] AS a RETURN a").hasError(
        getGql42001_42N39(Union.errorParam, 25, 1, 26),
        "All sub queries in an UNION must have the same return column names",
        p(25, 1, 26)
      )
    }

    test(s"UNION$setQuantifier with finish in second part") {
      run(s"UNWIND [1,2] AS a RETURN a UNION$setQuantifier UNWIND [2,3] AS a FINISH").hasError(
        getGql42001_42N39(Union.errorParam, 27, 1, 28),
        "All sub queries in an UNION must have the same return column names",
        p(27, 1, 28)
      )
    }
  }

  for {
    qualifierA <- Seq("", " DISTINCT", " ALL")
    qualifierB <- Seq("", " DISTINCT", " ALL")
    if qualifierA != qualifierB && qualifierA + qualifierB != " DISTINCT"
  } {
    test(s"Invalid combination of UNION$qualifierA and UNION$qualifierB") {
      val query =
        s"""RETURN 1 AS a
           |UNION$qualifierA
           |RETURN 2 AS a
           |UNION$qualifierB
           |RETURN 3 AS a""".stripMargin
      val extraLength = qualifierA.length
      val gql = from(STATUS_42001).atPosition(34 + extraLength, 4, 1)
        .withCause(from(STATUS_42I40).atPosition(34 + extraLength, 4, 1).build)
        .build
      run(query).hasError(gql, "Invalid combination of UNION and UNION ALL", p(34 + extraLength, 4, 1))
    }
  }

  test("Query ending in CALL ... YIELD ...") {
    run("MATCH (a) CALL proc.foo() YIELD bar")
      .hasError(getGql42001_42N71(10, 1, 11), "Query cannot conclude with CALL together with YIELD", p(10, 1, 11))
  }

  test("Query with only importing WITH") {
    run("WITH a").hasErrors(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(5, 1, 6)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
          .atPosition(5, 1, 6)
          .withParam(GqlParams.StringParam.variable, "a")
          .build())
        .build(),
      "Variable `a` not defined",
      p(5, 1, 6),
      getGql42001_42N71(0, 1, 1),
      "Query cannot conclude with WITH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(0, 1, 1)
    )
  }

  test("Relationship Pattern predicates should be enabled by default") {
    run("MATCH ()-[r:Rel WHERE r.prop > 42]->() return *")
      .hasNoErrors
  }

  test("Relationship Pattern predicates should not be allowed with quantification") {
    run("MATCH ()-[r:Rel* WHERE r.prop > 42]->() return *")
      .hasError(
        GqlHelper.getGql42001_42N37(30, 1, 31),
        "Relationship pattern predicates are not supported for variable-length relationships.",
        p(30, 1, 31)
      )
  }

  test("subquery without RETURN should not declare variables from YIELD in the outer scope") {
    val query =
      """CALL {
        |  CALL dbms.procedures() YIELD name
        |}
        |RETURN name
        |""".stripMargin
    run(query).hasErrorMessages("Variable `name` not defined")
  }

  test("should fail for size(COUNT{...})") {
    run("RETURN size(COUNT{ (n) }) AS foo").hasSemanticErrorsIn {
      case Cypher5 => Seq(invalidEntityType(
          "INTEGER",
          "argument at index 0 of function size()",
          List("STRING", "LIST"),
          "Type mismatch: expected String or List<T> but was Integer",
          InputPosition(12, 1, 13)
        ))
      case _ => Seq(invalidEntityType(
          "INTEGER",
          "argument at index 0 of function size()",
          List("STRING", "VECTOR", "LIST"),
          "Type mismatch: expected String, Vector or List<T> but was Integer",
          InputPosition(12, 1, 13)
        ))
    }
  }

  test("should fail for percentileCont(0.5, n) where n is a node variable") {
    run("MATCH (n) RETURN percentileCont(0.5, n) AS foo")
      .hasErrors(SemanticError.invalidEntityType(
        "NODE",
        "argument at index 1 of function percentileCont()",
        List("FLOAT"),
        "Type mismatch: expected Float but was Node",
        InputPosition(37, 1, 38)
      ))
  }

  test("should not allow subquery expressions in MERGE ON CREATE") {
    run("MERGE (n) ON CREATE SET n.prop = EXISTS { MATCH () } RETURN 1")
      .hasErrors(
        GqlHelper.getGql42001_42I48(33, 1, 34),
        "Subquery expressions are not allowed in a MERGE clause.",
        p(33, 1, 34),
        GqlHelper.getGql42001_42I48(33, 1, 34),
        "Subquery expressions are not allowed in a MERGE clause.",
        p(33, 1, 34)
      )
  }

  test("should not allow subquery expressions in MERGE") {
    run("MERGE (n {prop: EXISTS {MATCH ()}}) RETURN n.prop")
      .hasError(
        GqlHelper.getGql42001_42I48(16, 1, 17),
        "Subquery expressions are not allowed in a MERGE clause.",
        p(16, 1, 17)
      )
  }

  test("should not allow subquery expressions in MERGE ON SET") {
    run("MERGE (n) ON CREATE SET n.prop = COUNT { MATCH () } RETURN 1")
      .hasErrors(
        GqlHelper.getGql42001_42I48(33, 1, 34),
        "Subquery expressions are not allowed in a MERGE clause.",
        p(33, 1, 34),
        GqlHelper.getGql42001_42I48(33, 1, 34),
        "Subquery expressions are not allowed in a MERGE clause.",
        p(33, 1, 34)
      )
  }

  test("should allow index hint with negated predicate") {
    run("MATCH (a:A) USING INDEX a:A(prop) WHERE NOT a.prop > 123 RETURN 1")
      .hasNoErrors
  }

  test("Should check for undefined variables in type predicate expression") {
    run("MATCH (n) WHERE x IS :: BOOL RETURN 1")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(16, 1, 17)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(16, 1, 17)
            .withParam(GqlParams.StringParam.variable, "x")
            .build())
          .build(),
        "Variable `x` not defined",
        p(16, 1, 17)
      )
  }

  test("Should check for undefined variables in negative type predicate expression") {
    run("MATCH (n) WHERE x IS NOT :: BOOL RETURN 1")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(16, 1, 17)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(16, 1, 17)
            .withParam(GqlParams.StringParam.variable, "x")
            .build())
          .build(),
        "Variable `x` not defined",
        p(16, 1, 17)
      )
  }

  test("should fail for normalize() with incorrect arguments") {
    run("RETURN normalize(1) AS normalize").hasErrors(invalidEntityType(
      "INTEGER",
      "argument at index 0 of function normalize()",
      List("STRING"),
      "Type mismatch: expected String but was Integer",
      InputPosition(17, 1, 18).withInputLength(1)
    ))
  }

  test("Should check for undefined variables in normalized predicate expression") {
    run("MATCH (n) WHERE x IS NORMALIZED RETURN 1")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(16, 1, 17)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(16, 1, 17)
            .withParam(GqlParams.StringParam.variable, "x")
            .build())
          .build(),
        "Variable `x` not defined",
        p(16, 1, 17)
      )
  }

  test("Should check for undefined variables in negative normalized predicate expression") {
    run("MATCH (n) WHERE x IS NOT NORMALIZED RETURN 1")
      .hasError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(16, 1, 17)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
            .atPosition(16, 1, 17)
            .withParam(GqlParams.StringParam.variable, "x")
            .build())
          .build(),
        "Variable `x` not defined",
        p(16, 1, 17)
      )
  }

  test("Should not allow too large lower bound in variable length relationship") {
    val bigNumber = "9999999999999999999999999999999999999999999"
    run(s"MATCH ()-[*$bigNumber..]->() RETURN 1")
      .hasError(getGql22003(bigNumber, 11, 1, 12), "integer is too large", p(11, 1, 12))
  }

  test("Should not allow too large upper bound in variable length relationship") {
    val bigNumber = "9999999999999999999999999999999999999999999"
    run(s"MATCH ()-[*..$bigNumber]->() RETURN 1")
      .hasError(getGql22003(bigNumber, 13, 1, 14), "integer is too large", p(13, 1, 14))
  }

  test("CALL (*) inside a UNION should include variables imported by an outer CALL ()") {
    val query =
      """
        |WITH 1 AS x
        |CALL (x) {
        |  WITH 2 AS y
        |  CALL (*) {
        |    RETURN x + y AS z
        |  }
        |  RETURN z AS z
        |  UNION
        |  RETURN 123 + x AS z
        |}
        |RETURN x AS x, z AS z
        |""".stripMargin

    run(query).hasNoErrors
  }

  test("Without LOAD CSV, properties of a node should be of any storable type") {
    val query =
      """MATCH (row)
        |WITH row.id AS rid
        |CREATE (a: A { id: rid});""".stripMargin
    run(query)
      .assert(_.semanticTable.types(varFor("rid", p(27, 2, 16))).specified shouldBe storableType)
  }

  test("With LOAD CSV WITH HEADERS, properties should be of string type - direct use") {
    val query =
      """LOAD CSV WITH HEADERS FROM 'file:///test.csv' AS row
        |CREATE (a: A { id: row.id});""".stripMargin
    run(query)
      .assert(_.semanticTable.types(prop("row", "id", p(75, 2, 23))).specified shouldBe CTString.covariant)
  }

  test("With LOAD CSV WITH HEADERS, properties should be of string type - one projection") {
    val query =
      """LOAD CSV WITH HEADERS FROM 'file:///test.csv' AS rowWith
        |WITH rowWith.id AS rid
        |CREATE (a: A { id: rid});
      """.stripMargin
    run(query)
      .assert(_.semanticTable.types(varFor("rid", p(76, 2, 20))).specified shouldBe CTString.covariant)
  }

  test("With LOAD CSV WITH HEADERS, properties should be of string type - two projections") {
    val query =
      """LOAD CSV WITH HEADERS FROM 'file:///test.csv' AS row
        |WITH row AS rowAlias
        |WITH rowAlias.id AS rid
        |CREATE (a: A { id: rid});
      """.stripMargin
    run(query)
      .assert(_.semanticTable.types(varFor("rid", p(94, 3, 21))).specified shouldBe CTString.covariant)
  }

  test("Should not find shadowing variable row as the LOAD CSV WITH HEADERS variable") {
    val query =
      """LOAD CSV WITH HEADERS FROM 'file:///test.csv' AS row
        |CREATE (n)
        |WITH n AS row
        |WITH row.id AS rid
        |CREATE (a: A { id: rid});
      """.stripMargin
    run(query)
      .assert(_.semanticTable.types(varFor("rid", p(93, 4, 16))).specified shouldBe storableType)
  }

  test("should forward node type through map creation and access") {
    val query =
      """MATCH (n)
        |WITH {node : n} AS map
        |RETURN map.node AS mapNode
        |""".stripMargin
    run(query)
      .assert(_.semanticTable.types(prop("map", "node", p(43, 3, 11))).specified shouldBe CTNode.invariant)
      .assert(_.semanticTable.types(varFor("mapNode", p(52, 3, 20))).specified shouldBe CTNode.invariant)
  }

  test("should not assert type on unknown properties of map") {
    val query =
      """WITH {key: 'foo'} AS m
        |RETURN m.key
        |ORDER BY m.other.name
        |""".stripMargin
    run(query)
      .assert(_.semanticTable.types(prop("m", "other", p(46, 3, 11))).specified shouldBe CTAny.covariant)
      .assert(
        _.semanticTable
          .types(propExpression(prop("m", "other"), "name", p(52, 3, 17)))
          .specified shouldBe
          CTAny.covariant
      )
  }

  test("Should pass map type from load csv through different Cypher constructs") {
    val query =
      """LOAD CSV WITH HEADERS FROM $from AS r
        |CALL {
        |  WITH r
        |  CREATE (n:N)
        |  SET n = r
        |  SET n = {key: r.key}
        |}
        |CALL (r) {
        |  CREATE (n:N)
        |  SET n = r
        |  SET n = {key: r.key}
        |}
        |""".stripMargin
    run(query)
      .assert { result =>
        // In Cypher 25, we do not allow map assignment with anything but exactly a map anymore
        val expectedExpectedMapType =
          result.context.cypherVersion match {
            case CypherVersion.Cypher5 =>
              CTMap.covariant
            case CypherVersion.Cypher25 =>
              CTMap.invariant
          }

        // first call
        // map assignment
        result.semanticTable.types(varFor("r", p(79, 5, 11))) should equal(
          ExpressionTypeInfo(CTMap.invariant, Some(expectedExpectedMapType))
        )
        // entry access
        result.semanticTable.types(prop("r", "key", p(98, 6, 18))) should equal(
          ExpressionTypeInfo(CTString.covariant, None)
        )

        // second call
        // map assignment
        result.semanticTable.types(varFor("r", p(142, 10, 11))) should equal(
          ExpressionTypeInfo(CTMap.invariant, Some(expectedExpectedMapType))
        )
        // entry access
        result.semanticTable.types(prop("r", "key", p(161, 11, 18))) should equal(
          ExpressionTypeInfo(CTString.covariant, None)
        )
      }
  }

  override def messageProvider: ErrorMessageProvider = new ErrorMessageProviderAdapter {
    override def createUseClauseUnsupportedError(): String = "A very nice message explaining why USE is not allowed"

    override def createDynamicGraphReferenceUnsupportedError(graphName: String): String =
      "A very nice message explaining why dynamic graph references are not allowed: " + graphName

    override def createMultipleGraphReferencesError(graphName: String, transactioinalDefault: Boolean = false): String =
      "A very nice message explaining why multiple graph references are not allowed: " + graphName
  }
}

object SemanticAnalysisTest {

  def gql42N29(pos: InputPosition, n: String): ErrorGqlStatusObject =
    from(STATUS_42001)
      .atPosition(pos.offset, pos.line, pos.column)
      .withCause(from(STATUS_42N29).atPosition(pos.offset, pos.line, pos.column).withParam(variable, n).build())
      .build()

  def gql42NA5(pos: InputPosition): ErrorGqlStatusObject =
    from(STATUS_42001).atPosition(pos.offset, pos.line, pos.column)
      .withCause(from(STATUS_42NA5).atPosition(pos.offset, pos.line, pos.column).build())
      .build()
}
