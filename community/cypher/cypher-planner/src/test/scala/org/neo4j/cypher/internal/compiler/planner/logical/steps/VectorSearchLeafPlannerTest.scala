/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.NodeVectorIndexSearch
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.VectorIndexSearchException

class VectorSearchLeafPlannerTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport2
    with AstConstructionTestSupport with QueryExpressionConstructionTestSupport {

  private val embedding = listOfInt(1, 2, 3)
  private val limit = literalInt(10)

  test("plans nodeVectorIndexSearch when skipIds do not contain the binding variable") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeVectorIndexOn("moviePlots", Seq("Movie"), "plot")
    } withLogicalPlanningContext { (_, context) =>
      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        embedding = embedding,
        where = None,
        limit = limit,
        scoreVariable = None
      )
      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty
      )

      // When skipIds is empty, the planner should plan the vector search
      val planner = VectorSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe a[NodeVectorIndexSearch]
      val nodeVectorSearch = plan.asInstanceOf[NodeVectorIndexSearch]
      nodeVectorSearch.idName should equal(bindingVariable)
    }
  }

  test("plans relationshipVectorIndexSearch when skipIds do not contain the binding variable") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipVectorIndexOn("knowsEmbedding", Seq("KNOWS"), "embedding")
    } withLogicalPlanningContext { (_, context) =>
      val from = v"a"
      val to = v"b"

      val patternRelationship = PatternRelationship(
        variable = bindingVariable,
        (from, to),
        dir = OUTGOING,
        types = Seq(RelTypeName("KNOWS")(pos)),
        length = SimplePatternLength
      )

      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "knowsEmbedding",
        embedding = embedding,
        where = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(from, to),
        patternRelationships = Set(patternRelationship),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty
      )

      // When skipIds is empty, the planner should plan the relationship vector search
      val planner = VectorSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe a[DirectedRelationshipVectorIndexSearch]
      val relVectorSearch = plan.asInstanceOf[DirectedRelationshipVectorIndexSearch]
      relVectorSearch.idName shouldEqual Some(bindingVariable)
    }
  }

  test("produces no candidates if embedding has unresolved dependencies not in argumentIds") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeVectorIndexOn("moviePlots", Seq("Movie"), "plot")
    } withLogicalPlanningContext { (_, context) =>
      val embeddingWithDeps = listOf(v"x", literalInt(2), literalInt(3))

      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        embedding = embeddingWithDeps,
        where = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty // x is not in argumentIds
      )

      val planner = VectorSearchLeafPlanner
      planner(qg, InterestingOrderConfig.empty, context) shouldEqual Set.empty
    }
  }

  test("produces no candidates if limit has unresolved dependencies not in argumentIds") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeVectorIndexOn("moviePlots", Seq("Movie"), "plot")
    } withLogicalPlanningContext { (_, context) =>
      val limitWithDeps = v"y"

      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        embedding = embedding,
        where = None,
        limit = limitWithDeps,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty // y is not in argumentIds
      )

      val planner = VectorSearchLeafPlanner
      planner(qg, InterestingOrderConfig.empty, context) shouldEqual Set.empty
    }
  }

  test("throws error if node vector search binding variable type is not CTNode") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      nodeVectorIndexOn("moviePlots", Seq("Movie"), "plot")
    } withLogicalPlanningContext { (_, context) =>
      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        embedding = embedding,
        where = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set.empty,
        patternRelationships = Set(
          PatternRelationship(
            bindingVariable,
            (v"a", v"b"),
            dir = OUTGOING,
            types = Seq.empty,
            length = SimplePatternLength
          )
        ),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty
      )

      val planner = VectorSearchLeafPlanner

      an[VectorIndexSearchException] should be thrownBy {
        planner(qg, InterestingOrderConfig.empty, context)
      }
    }
  }

  test("throws error if relationship vector search binding variable type is not CTRelationship") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      relationshipVectorIndexOn("knowsEmbedding", Seq("KNOWS"), "embedding")
    } withLogicalPlanningContext { (_, context) =>
      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "knowsEmbedding",
        embedding = embedding,
        where = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty
      )

      val planner = VectorSearchLeafPlanner

      an[VectorIndexSearchException] should be thrownBy {
        planner(qg, InterestingOrderConfig.empty, context)
      }
    }
  }

  test("plans nodeVectorIndexSearch with score variable") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeVectorIndexOn("moviePlots", Seq("Movie"), "plot")
    } withLogicalPlanningContext { (_, context) =>
      val scoreVariable = v"similarity"
      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        embedding = embedding,
        where = None,
        limit = limit,
        scoreVariable = Some(scoreVariable)
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set.empty
      )

      val planner = VectorSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe a[NodeVectorIndexSearch]
      val nodeVectorSearch = plan.asInstanceOf[NodeVectorIndexSearch]
      nodeVectorSearch.score should equal(Some(scoreVariable))
    }
  }

  test("plans nodeVectorIndexSearch with resolved dependencies in argumentIds") {
    val bindingVariable = v"movie"
    val parameter = v"embeddingParam"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      addTypeToSemanticTable(parameter, CTNode)
      nodeVectorIndexOn("moviePlots", Seq("Movie"), "plot")
    } withLogicalPlanningContext { (_, context) =>
      val embeddingWithDeps = v"embeddingParam"

      val vectorSearchClause = VectorSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        embedding = embeddingWithDeps,
        where = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(vectorSearchClause),
        argumentIds = Set(parameter) // Parameter is in argumentIds, so no error should occur
      )

      val planner = VectorSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      plans.head shouldBe a[NodeVectorIndexSearch]
    }
  }

  test("queryExpressionFromFilterExpressions should handle empty case") {
    VectorSearchLeafPlanner.queryExpressionFromWhereClause(
      None,
      Seq.empty,
      ""
    ) should equal(None)
  }

  private val property1 = "property1"
  private val property2 = "property2"

  test("queryExpressionFromFilterExpressions should handle single exact match") {
    val expr = literalInt(42)
    VectorSearchLeafPlanner.queryExpressionFromWhereClause(
      maybeWhere = Some(where(equals(prop(varFor("m"), property1), expr))),
      additionalProperties = Seq(property1),
      indexName = ""
    ) should equal(Some(single(expr)))
  }

  test("queryExpressionFromFilterExpressions should handle single comparison") {
    VectorSearchLeafPlanner.queryExpressionFromWhereClause(
      maybeWhere = Some(where(propGreaterThan("m", property1, 10))),
      additionalProperties = Seq(property1),
      indexName = ""
    ) should equal(Some(rangeExpression(gt(10))))
  }

  test("queryExpressionFromFilterExpressions should handle multiple exact matches") {
    val expr1 = literalInt(42)
    val expr2 = literalInt(43)
    VectorSearchLeafPlanner.queryExpressionFromWhereClause(
      maybeWhere =
        Some(where(ands(equals(prop(varFor("m"), property1), expr1), equals(prop(varFor("m"), property2), expr2)))),
      additionalProperties = Seq(property1, property2),
      indexName = ""
    ) should equal(Some(composite(single(expr1), single(expr2))))
  }

  test("queryExpressionFromFilterExpressions should handle multiple properties") {
    val expr1 = literalInt(42)
    VectorSearchLeafPlanner.queryExpressionFromWhereClause(
      maybeWhere = Some(where(equals(prop(varFor("m"), property2), expr1))),
      additionalProperties = Seq(property1, property2),
      indexName = ""
    ) should equal(Some(composite(AllQueryExpression, single(expr1))))
  }

  test("queryExpressionFromFilterExpressions should handle predicate without property") {
    val expr1 = literalInt(42)
    val expr2 = literalInt(43)
    the[VectorIndexSearchException] thrownBy
      VectorSearchLeafPlanner.queryExpressionFromWhereClause(
        maybeWhere =
          Some(where(ands(equals(prop(varFor("m"), property1), expr1), equals(prop(varFor("m"), property2), expr2)))),
        additionalProperties = Seq(property1),
        indexName = "indexName"
      ) should have message "22ND3: The property `property2` is not an additional property for vector search with filters on the vector index `indexName`."
  }

  test("queryExpressionFromFilterExpressions should combine multiple inequalities on the same property into a range") {
    val lower = literalInt(0)
    val upper = literalInt(42)
    VectorSearchLeafPlanner.queryExpressionFromWhereClause(
      maybeWhere = Some(where(
        ands(
          lessThanOrEqual(prop(varFor("m"), property1), upper),
          greaterThan(prop(varFor("m"), property1), lower)
        )
      )),
      additionalProperties = Seq(property1),
      indexName = "indexName"
    ) should equal(Some(between(gt(lower), lte(upper))))
  }

  test("queryExpressionFromFilterExpressions should handle equality and inequality on same property") {
    val upper = literalInt(42)
    val expr2 = literalInt(43)
    an[InternalException] should be thrownBy
      VectorSearchLeafPlanner.queryExpressionFromWhereClause(
        maybeWhere =
          Some(where(
            ands(
              lessThan(prop(varFor("m"), property1), upper),
              equals(prop(varFor("m"), property1), expr2)
            )
          )),
        additionalProperties = Seq(property1),
        indexName = "indexName"
      )
  }

  test("queryExpressionFromFilterExpressions should handle two less than on same property") {
    val upper1 = literalInt(42)
    val upper2 = literalInt(43)
    an[InternalException] should be thrownBy
      VectorSearchLeafPlanner.queryExpressionFromWhereClause(
        maybeWhere =
          Some(where(ands(
            lessThan(prop(varFor("m"), property1), upper1),
            lessThan(prop(varFor("m"), property1), upper2)
          ))),
        additionalProperties = Seq(property1),
        indexName = "indexName"
      )
  }
}
