/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.matchers.Matcher

class VarLengthPlanningIntegrationTest
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  test("should handle LIKES.LIKES*2.LIKES") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:LIKES]->()", 10000)
      .build()
    val query = "MATCH (p { id: 'n0' }) MATCH (p)-[r1:LIKES]->()-[r2:LIKES*2]->()-[r3:LIKES]->(c) RETURN c"
    planner.plan(query) should haveRelNotEqualsFilter("r3", "r1")
  }

  def haveRelNotEqualsFilter(rel1: String, rel2: String): Matcher[LogicalPlan] =
    containSelectionMatching {
      case Not(Equals(Variable(`rel1`), Variable(`rel2`))) =>
    }

  test("should plan the right quantifier on two conflicting ones - in same MATCH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e*1..2]->(), ()-[e*2..3]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projectEndpoints("(anon_0)-[e*2..2]->(anon_1)", startInScope = false, endInScope = false)
      .filter("none(anon_4 IN e WHERE any(anon_5 IN e WHERE anon_4 = anon_5))")
      .expand("(anon_2)-[e*2..2]->(anon_3)")
      .allNodeScan("anon_2")
      .build()
  }

  test("should plan the right quantifier on two conflicting ones - in consecutive MATCHes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 200)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("(:A)-[]->()", 5000)
      .build()

    val plan = planner.plan(
      s"""MATCH (n:A)-[e*1..2]->()
         |MATCH ()-[e*2..3]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projectEndpoints("(anon_1)-[e*2..2]->(anon_2)", startInScope = false, endInScope = false)
      .expand("(n)-[e*2..2]->(anon_0)")
      .nodeByLabelScan("n", "A", IndexOrderNone)
      .build()
  }

  test("should plan the right quantifier on three conflicting ones - in consecutive MATCHes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 200)
      .setLabelCardinality("B", 400)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("(:A)-[]->()", 5000)
      .setRelationshipCardinality("(:B)-[]->()", 6000)
      .build()

    val plan = planner.plan(
      s"""MATCH (n:A)-[e*1..3]->()
         |MATCH (m:B)-[e*2..4]->()
         |MATCH ()-[e*3..5]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projectEndpoints("(anon_2)-[e*3..3]->(anon_3)", startInScope = false, endInScope = false)
      .filter("m:B")
      .projectEndpoints("(m)-[e*3..3]->(anon_1)", startInScope = false, endInScope = false)
      .expand("(n)-[e*3..3]->(anon_0)")
      .nodeByLabelScan("n", "A", IndexOrderNone)
      .build()
  }

  test("should plan the right quantifier on three conflicting ones - in consecutive but separated MATCHes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 200)
      .setLabelCardinality("B", 400)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("(:A)-[]->()", 5000)
      .setRelationshipCardinality("(:B)-[]->()", 6000)
      .build()

    val plan = planner.plan(
      s"""MATCH (n:A)-[e*1..3]->()
         |WITH * SKIP 0
         |MATCH (m:B)-[e*2..4]->()
         |MATCH ()-[e*3..5]->()
         |RETURN e""".stripMargin
    )
    plan shouldEqual planner.planBuilder()
      .produceResults("e")
      .apply()
      .|.projectEndpoints("(anon_2)-[e*3..4]->(anon_3)", startInScope = false, endInScope = false)
      .|.filter("m:B")
      .|.projectEndpoints("(m)-[e*3..4]->(anon_1)", startInScope = false, endInScope = false)
      .|.filter(
        "size(e) >= 3",
        "size(e) <= 4",
        "size(e) <= 5",
        "size(e) >= 2",
              )
      .|.argument("e", "n")
      .skip(0)
      .expand("(n)-[e*1..3]->(anon_0)")
      .nodeByLabelScan("n", "A", IndexOrderNone)
      .build()
  }

  test("should plan the right quantifier on two conflicting ones - in EXISTS clause") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e*1..2]->()
         | WHERE EXISTS { MATCH ()-[e*2..3]->() }
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .semiApply()
      .|.projectEndpoints("(anon_2)-[e*2..3]->(anon_3)", startInScope = false, endInScope = false)
      .|.filter( "size(e) >= 2", "size(e) <= 3")
      .|.argument("e")
      .expand("(anon_0)-[e*1..2]->(anon_1)")
      .allNodeScan("anon_0")
      .build()
  }

  test("should plan the right quantifier on two conflicting ones - in pattern expression") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e*1..2]->()
         | WHERE ()-[e*2..3]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .semiApply()
      .|.projectEndpoints("(anon_4)-[e*2..3]->(anon_5)", startInScope = false, endInScope = false)
      .|.filter("size(e) >= 2", "size(e) <= 3")
      .|.argument("e")
      .expand("(anon_2)-[e*1..2]->(anon_3)")
      .allNodeScan("anon_2")
      .build()
  }

  test("should plan the right quantifier on two conflicting ones - in pattern comprehension") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH (a)-[e*1..2]->(b)
         |RETURN [ (a)-[e*2..3]->(b) | a ]""".stripMargin
    ).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .rollUpApply("[ (a)-[e*2..3]->(b) | a ]", "anon_0")
      .|.projection("a AS anon_0")
      .|.projectEndpoints("(a)-[e*2..3]->(b)", startInScope = true, endInScope = true)
      .|.filter("size(e) >= 2", "size(e) <= 3")
      .|.argument("a", "b", "e")
      .expand("(a)-[e*1..2]->(b)")
      .allNodeScan("a")
      .build()
  }

  test("should plan the right quantifier on two conflicting ones - in OPTIONAL MATCH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e*1..2]->()
         |OPTIONAL MATCH (a)-[e*2..3]->()
         |RETURN a, e""".stripMargin
    )
    plan shouldEqual planner.planBuilder()
      .produceResults("a", "e")
      .apply()
      .|.optional("anon_0", "e", "anon_1")
      .|.projectEndpoints("(a)-[e*2..3]->(anon_2)", startInScope = false, endInScope = false)
      .|.filter( "size(e) >= 2", "size(e) <= 3")
      .|.argument("e", "anon_0", "anon_1")
      .expand("(anon_0)-[e*1..2]->(anon_1)")
      .allNodeScan("anon_0")
      .build()
  }

  test("should plan the right quantifier on two disjoint ones - in consecutive MATCHes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 200)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("(:A)-[]->()", 5000)
      .build()

    val plan = planner.plan(
      s"""MATCH (n:A)-[e*1..2]->()
         |MATCH ()-[e*3..4]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projectEndpoints("(anon_1)-[e*3..2]->(anon_2)", startInScope = false, endInScope = false)
      .expand("(n)-[e*3..2]->(anon_0)")
      .nodeByLabelScan("n", "A", IndexOrderNone)
      .build()
  }

  test("should plan the right quantifier on two conflicting ones - in consecutive separated MATCHes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e*1..2]->()
         |WITH e SKIP 0
         |MATCH ()-[e*2..3]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.projectEndpoints("(anon_2)-[e*2..3]->(anon_3)", startInScope = false, endInScope = false)
      .|.filter( "size(e) >= 2", "size(e) <= 3")
      .|.argument("e")
      .skip(0)
      .expand("(anon_0)-[e*1..2]->(anon_1)")
      .allNodeScan("anon_0")
      .build()
  }

  test("should plan the right type on two overlapping relationships") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e]->()
         |WITH 1 as dummy, e
         |MATCH ()-[e:Type]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.projectEndpoints("(anon_2)-[e:Type]->(anon_3)", startInScope = false, endInScope = false)
      .|.argument("e", "dummy")
      .projection("1 AS dummy")
      .expandAll("(anon_0)-[e]->(anon_1)")
      .allNodeScan("anon_0")
      .build()
  }

  test("should plan an impossible plan when a relationship gets two different types in successive patterns") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:Type1]->()", 100)
      .setRelationshipCardinality("()-[:Type2]->()", 100)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e:Type1]->()
         |MATCH ()-[e:Type2]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projectEndpoints("(anon_0)-[e:Type1]->(anon_1)", startInScope = false, endInScope = false)
      .relationshipTypeScan("(anon_2)-[e:Type2]->(anon_3)", IndexOrderNone)
      .build()
  }

  test("should plan the right type on two overlapping var-length relationships") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val plan = planner.plan(
      s"""MATCH ()-[e*1..2]->()
         |WITH 1 as dummy, e
         |MATCH ()-[e:Type*2..3]->()
         |RETURN e""".stripMargin
    ).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.projectEndpoints("(anon_2)-[e:Type*2..3]->(anon_3)", startInScope = false, endInScope = false)
      .|.filter( "size(e) >= 2", "size(e) <= 3")
      .|.argument("e", "dummy")
      .projection("1 AS dummy")
      .expand("(anon_0)-[e*1..2]->(anon_1)")
      .allNodeScan("anon_0")
      .build()
  }

  test("should extract predicates regardless of function name spelling") {
    val functionNames = Seq(("nodes", "relationships"), ("NODES", "RELATIONSHIPS"))
    for ((nodesF, relationshipsF) <- functionNames) withClue((nodesF, relationshipsF)) {
      val planner = plannerBuilder()
        .setAllNodesCardinality(1000)
        .setRelationshipCardinality("()-[]->()", 500)
        .build()

      val q =
        s"""MATCH p = (a)-[r*]->(b)
           |WHERE
           |  ALL(x IN $nodesF(p) WHERE x.prop > 123) AND
           |  ALL(y IN $relationshipsF(p) WHERE y.prop <> 'hello') AND
           |  NONE(z IN $nodesF(p) WHERE z.otherProp < 321) AND
           |  NONE(t IN $relationshipsF(p) WHERE t.otherProp IS NULL)
           |RETURN count(*) AS result""".stripMargin

      val plan = planner.plan(q).stripProduceResults

      val nodePredicates = Predicate("r_NODES", "r_NODES.prop > 123 AND not r_NODES.otherProp < 321")
      val relPredicates = Predicate("r_RELS", "not r_RELS.prop = 'hello' AND not r_RELS.otherProp IS NULL")
      plan shouldEqual planner.subPlanBuilder()
        .aggregation(Seq(), Seq("count(*) AS result"))
        .expand("(a)-[r*1..]->(b)", nodePredicate = nodePredicates, relationshipPredicate = relPredicates)
        .allNodeScan("a")
        .build()
    }
  }

  test("should inline property predicates of var-length relationships") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    planner.plan("MATCH (a)-[r* {prop: 42}]->(b) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .expand("(a)-[r*1..]->(b)", relationshipPredicate = Predicate("r_RELS", "r_RELS.prop = 42"))
        .allNodeScan("a")
        .build()
    )
  }

  test("should be able to plan var-length relationships with empty property maps") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    planner.plan("MATCH (a {})-[r* {}]->(b) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .expand("(a)-[r*1..]->(b)")
        .allNodeScan("a")
        .build()
    )
  }
}
