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
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.FulltextSearchClause
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeFulltextIndexSearch
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipFulltextIndexSearch
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.exceptions.IndexSearchException

class FulltextSearchLeafPlannerTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport2
    with AstConstructionTestSupport {

  private val queryString = literalString("magic genie")
  private val limit = literalInt(10)

  test("plans nodeFulltextIndexSearch when the binding variable is a pattern node") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )
      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe a[NodeFulltextIndexSearch]
      val nodeFulltextSearch = plan.asInstanceOf[NodeFulltextIndexSearch]
      nodeFulltextSearch.idName should equal(bindingVariable)
    }
  }

  test("plans relationshipFulltextIndexSearch when the binding variable is a pattern relationship") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipFulltextIndexOn("actsInScript", Seq("ACTS_IN"), Seq("script", "notes"))
    } withLogicalPlanningContext { (_, context) =>
      val from = v"a"
      val to = v"b"

      val patternRelationship = PatternRelationship(
        variable = bindingVariable,
        (from, to),
        dir = OUTGOING,
        types = Seq(RelTypeName("ACTS_IN")(pos)),
        length = SimplePatternLength
      )

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "actsInScript",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(from, to),
        patternRelationships = Set(patternRelationship),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe a[DirectedRelationshipFulltextIndexSearch]
      val relFulltextSearch = plan.asInstanceOf[DirectedRelationshipFulltextIndexSearch]
      relFulltextSearch.idName shouldEqual Some(bindingVariable)
    }
  }

  test("plans undirectedRelationshipFulltextIndexSearch when the pattern relationship is undirected") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipFulltextIndexOn("actsInScript", Seq("ACTS_IN"), Seq("script", "notes"))
    } withLogicalPlanningContext { (_, context) =>
      val from = v"a"
      val to = v"b"

      val patternRelationship = PatternRelationship(
        variable = bindingVariable,
        (from, to),
        dir = BOTH,
        types = Seq(RelTypeName("ACTS_IN")(pos)),
        length = SimplePatternLength
      )

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "actsInScript",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(from, to),
        patternRelationships = Set(patternRelationship),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe an[UndirectedRelationshipFulltextIndexSearch]
      val relFulltextSearch = plan.asInstanceOf[UndirectedRelationshipFulltextIndexSearch]
      relFulltextSearch.idName shouldEqual Some(bindingVariable)
      // An undirected pattern relationship keeps its nodes in pattern order; only INCOMING swaps them.
      relFulltextSearch.startNode shouldEqual Some(from)
      relFulltextSearch.endNode shouldEqual Some(to)
      solvedPatternRelationships(plan, context) shouldEqual Set(patternRelationship)
    }
  }

  test("produces no candidates if queryString has unresolved dependencies not in argumentIds") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val queryStringWithDeps = varFor("x")

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryStringWithDeps,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty // x is not in argumentIds
      )

      val planner = FulltextSearchLeafPlanner
      planner(qg, InterestingOrderConfig.empty, context) shouldEqual Set.empty
    }
  }

  test("produces no candidates if limit has unresolved dependencies not in argumentIds") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val limitWithDeps = varFor("y")

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limitWithDeps,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty // y is not in argumentIds
      )

      val planner = FulltextSearchLeafPlanner
      planner(qg, InterestingOrderConfig.empty, context) shouldEqual Set.empty
    }
  }

  test("produces no candidates if analyzer has unresolved dependencies not in argumentIds") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val analyzerWithDeps = varFor("z")

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = Some(analyzerWithDeps),
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty // z is not in argumentIds
      )

      val planner = FulltextSearchLeafPlanner
      planner(qg, InterestingOrderConfig.empty, context) shouldEqual Set.empty
    }
  }

  test("produces no candidates if skip has unresolved dependencies not in argumentIds") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val skipWithDeps = varFor("s")

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = None,
        skip = Some(skipWithDeps),
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty // s is not in argumentIds
      )

      val planner = FulltextSearchLeafPlanner
      planner(qg, InterestingOrderConfig.empty, context) shouldEqual Set.empty
    }
  }

  test("throws error if node fulltext search binding variable type is not CTNode") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = None,
        skip = None,
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
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner

      an[IndexSearchException] should be thrownBy {
        planner(qg, InterestingOrderConfig.empty, context)
      }
    }
  }

  test("throws error if relationship fulltext search binding variable type is not CTRelationship") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      relationshipFulltextIndexOn("actsInScript", Seq("ACTS_IN"), Seq("script", "notes"))
    } withLogicalPlanningContext { (_, context) =>
      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "actsInScript",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner

      an[IndexSearchException] should be thrownBy {
        planner(qg, InterestingOrderConfig.empty, context)
      }
    }
  }

  test("plans nodeFulltextIndexSearch with score variable") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val scoreVariable = v"relevance"
      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = Some(scoreVariable)
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val plan = plans.head
      plan shouldBe a[NodeFulltextIndexSearch]
      val nodeFulltextSearch = plan.asInstanceOf[NodeFulltextIndexSearch]
      nodeFulltextSearch.score should equal(Some(scoreVariable))
    }
  }

  test("plans nodeFulltextIndexSearch with skip and analyzer") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = Some(literalString("english")),
        skip = Some(literalInt(5)),
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      val nodeFulltextSearch = plans.head.asInstanceOf[NodeFulltextIndexSearch]
      nodeFulltextSearch.analyzer should equal(Some(literalString("english")))
      nodeFulltextSearch.skip should equal(Some(literalInt(5)))
    }
  }

  test("plans nodeFulltextIndexSearch with resolved dependencies in argumentIds") {
    val bindingVariable = v"movie"
    val parameter = v"queryParam"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      addTypeToSemanticTable(parameter, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val queryStringWithDeps = v"queryParam"

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryStringWithDeps,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set(parameter) // Parameter is in argumentIds, so no error should occur
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      plans.head shouldBe a[NodeFulltextIndexSearch]
    }
  }

  test("implicitly solves IS NOT NULL on the sole indexed property of a single-property node index") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("movieInfo", Seq("Movie"), Seq("info"))
    } withLogicalPlanningContext { (_, context) =>
      val labelPredicate = hasLabels(bindingVariable, "Movie")
      val isNotNullPredicate = isNotNull(prop(bindingVariable, "info"))

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "movieInfo",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        selections = Selections.from(Seq(labelPredicate, isNotNullPredicate)),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      solvedPredicates(plans.head, context) shouldEqual Set(labelPredicate, isNotNullPredicate)
    }
  }

  test("does not implicitly solve IS NOT NULL on an indexed property of a multi-property node index") {
    val bindingVariable = v"movie"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTNode)
      nodeFulltextIndexOn("moviePlots", Seq("Movie"), Seq("title", "plot"))
    } withLogicalPlanningContext { (_, context) =>
      val labelPredicate = hasLabels(bindingVariable, "Movie")
      val isNotNullPredicate = isNotNull(prop(bindingVariable, "title"))

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "moviePlots",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(bindingVariable),
        selections = Selections.from(Seq(labelPredicate, isNotNullPredicate)),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      // A match on title OR plot does not imply that title in particular is non-null, so the predicate
      // must remain unsolved and be left for a Filter. The label predicate is still solved.
      solvedPredicates(plans.head, context) shouldEqual Set(labelPredicate)
    }
  }

  test("implicitly solves IS NOT NULL on the sole indexed property of a single-property relationship index") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipFulltextIndexOn("contributed", Seq("CONTRIBUTED"), Seq("summary"))
    } withLogicalPlanningContext { (_, context) =>
      val typePredicate = hasTypes(bindingVariable.name, "CONTRIBUTED")
      val isNotNullPredicate = isNotNull(prop(bindingVariable, "summary"))

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "contributed",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships = Set(PatternRelationship(
          variable = bindingVariable,
          (v"a", v"b"),
          dir = OUTGOING,
          types = Seq(RelTypeName("CONTRIBUTED")(pos)),
          length = SimplePatternLength
        )),
        selections = Selections.from(Seq(typePredicate, isNotNullPredicate)),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      solvedPredicates(plans.head, context) shouldEqual Set(typePredicate, isNotNullPredicate)
    }
  }

  test(
    "implicitly solves IS NOT NULL for an undirected relationship on the sole indexed property of a single-property index"
  ) {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipFulltextIndexOn("contributed", Seq("CONTRIBUTED"), Seq("summary"))
    } withLogicalPlanningContext { (_, context) =>
      val typePredicate = hasTypes(bindingVariable.name, "CONTRIBUTED")
      val isNotNullPredicate = isNotNull(prop(bindingVariable, "summary"))

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "contributed",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships = Set(PatternRelationship(
          variable = bindingVariable,
          (v"a", v"b"),
          dir = BOTH,
          types = Seq(RelTypeName("CONTRIBUTED")(pos)),
          length = SimplePatternLength
        )),
        selections = Selections.from(Seq(typePredicate, isNotNullPredicate)),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      plans.head shouldBe an[UndirectedRelationshipFulltextIndexSearch]
      solvedPredicates(plans.head, context) shouldEqual Set(typePredicate, isNotNullPredicate)
    }
  }

  test("does not implicitly solve IS NOT NULL on an indexed property of a multi-property relationship index") {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipFulltextIndexOn("actsInScript", Seq("ACTS_IN"), Seq("script", "notes"))
    } withLogicalPlanningContext { (_, context) =>
      val typePredicate = hasTypes(bindingVariable.name, "ACTS_IN")
      val isNotNullPredicate = isNotNull(prop(bindingVariable, "script"))

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "actsInScript",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships = Set(PatternRelationship(
          variable = bindingVariable,
          (v"a", v"b"),
          dir = OUTGOING,
          types = Seq(RelTypeName("ACTS_IN")(pos)),
          length = SimplePatternLength
        )),
        selections = Selections.from(Seq(typePredicate, isNotNullPredicate)),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      solvedPredicates(plans.head, context) shouldEqual Set(typePredicate)
    }
  }

  test(
    "does not implicitly solve IS NOT NULL for an undirected relationship on an indexed property of a multi-property index"
  ) {
    val bindingVariable = v"knows"
    new givenConfig {
      addTypeToSemanticTable(bindingVariable, CTRelationship)
      relationshipFulltextIndexOn("actsInScript", Seq("ACTS_IN"), Seq("script", "notes"))
    } withLogicalPlanningContext { (_, context) =>
      val typePredicate = hasTypes(bindingVariable.name, "ACTS_IN")
      val isNotNullPredicate = isNotNull(prop(bindingVariable, "script"))

      val fulltextSearchClause = FulltextSearchClause(
        resultVariable = bindingVariable,
        indexName = "actsInScript",
        queryString = queryString,
        analyzer = None,
        skip = None,
        limit = limit,
        scoreVariable = None
      )

      val qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships = Set(PatternRelationship(
          variable = bindingVariable,
          (v"a", v"b"),
          dir = BOTH,
          types = Seq(RelTypeName("ACTS_IN")(pos)),
          length = SimplePatternLength
        )),
        selections = Selections.from(Seq(typePredicate, isNotNullPredicate)),
        searchClause = Some(fulltextSearchClause),
        argumentIds = Set.empty
      )

      val planner = FulltextSearchLeafPlanner
      val plans = planner(qg, InterestingOrderConfig.empty, context)

      plans should have size 1
      plans.head shouldBe an[UndirectedRelationshipFulltextIndexSearch]
      solvedPredicates(plans.head, context) shouldEqual Set(typePredicate)
    }
  }

  private def solvedPredicates(plan: LogicalPlan, context: LogicalPlanningContext): Set[Expression] =
    context.staticComponents.planningAttributes
      .solveds(plan.id)
      .asSinglePlannerQuery
      .queryGraph
      .selections
      .flatPredicatesSet

  private def solvedPatternRelationships(
    plan: LogicalPlan,
    context: LogicalPlanningContext
  ): Set[PatternRelationship] =
    context.staticComponents.planningAttributes
      .solveds(plan.id)
      .asSinglePlannerQuery
      .queryGraph
      .patternRelationships
}
