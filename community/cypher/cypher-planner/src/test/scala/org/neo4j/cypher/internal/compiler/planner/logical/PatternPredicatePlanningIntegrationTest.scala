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
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

import scala.collection.immutable.ListSet

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private val planner = plannerBuilder()
    .setAllNodesCardinality(100)
    .setLabelCardinality("B", 10)
    .setLabelCardinality("C", 10)
    .setLabelCardinality("D", 1)
    .setLabelCardinality("Foo", 10)
    .setLabelCardinality("Person", 10)
    .setLabelCardinality("ComedyClub", 10)
    .setLabelCardinality("User", 10)
    .setLabelCardinality("Label", 10)
    .setLabelCardinality("UniqueLabel", 100)
    .setLabelCardinality("TextLabel", 10)
    .setLabelCardinality("M", 10)
    .setLabelCardinality("O", 10)
    .setAllRelationshipsCardinality(100)
    .setRelationshipCardinality("()-[:REL]-()", 10)
    .setRelationshipCardinality("()-[:X]-()", 10)
    .setRelationshipCardinality("()-[:X]-(:Foo)", 10)
    .setRelationshipCardinality("()-[:Y]-()", 10)
    .setRelationshipCardinality("()-[]->(:B)", 10)
    .setRelationshipCardinality("()-[]->(:C)", 10)
    .setRelationshipCardinality("()-[]->(:D)", 10)
    .setRelationshipCardinality("()-[:KNOWS]-()", 10)
    .setRelationshipCardinality("(:Person)-[:KNOWS]-()", 10)
    .setRelationshipCardinality("(:Person)-[:KNOWS]-(:Person)", 10)
    .setRelationshipCardinality("()-[:WORKS_AT]-()", 10)
    .setRelationshipCardinality("(:ComedyClub)-[:WORKS_AT]-()", 10)
    .setRelationshipCardinality("()-[:FOLLOWS]-()", 10)
    .setRelationshipCardinality("(:User)-[:FOLLOWS]-()", 10)
    .setRelationshipCardinality("()-[:FOO]->()", 10)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setRelationshipCardinality("()-[:TextRel]->()", 10)
    .setRelationshipCardinality("()-[:R]->(:M)", 10)
    .setRelationshipCardinality("()-[:Q]->()", 10)
    .setRelationshipCardinality("()-[:Q]->(:O)", 10)
    .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
    .addRelationshipIndex("TextRel", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
    .addNodeIndex("Label", Seq("prop"), 1.0, 0.1, indexType = IndexType.RANGE)
    .addNodeIndex("UniqueLabel", Seq("prop"), 1.0, 0.01, isUnique = true, indexType = IndexType.RANGE)
    .addNodeIndex("TextLabel", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
    .addNodeIndex("D", Seq("prop"), 1.0, 0.01)
    .build()

  private val reduceExpr = reduce(
    varFor("sum", pos),
    literalInt(0, pos),
    varFor("x", pos),
    varFor("anon_1", pos),
    add(varFor("sum", pos), varFor("x", pos))
  )

  test("should consider variables introduced by outer list comprehensions when planning pattern predicates") {
    val plan = planner.plan(
      """MATCH (a:Person)-[:KNOWS]->(b:Person) WITH a, collect(b) AS friends RETURN a, [f IN friends WHERE (f)-[:WORKS_AT]->(:ComedyClub)] AS clowns"""
    ).stripProduceResults

    val nestedPlan = planner.subPlanBuilder()
      .filter("anon_4:ComedyClub")
      .expand("(f)-[anon_3:WORKS_AT]->(anon_4)")
      .argument("f")
      .build()

    val projectionExpression = listComprehension(
      varFor("f"),
      varFor("friends"),
      Some(NestedPlanExistsExpression(nestedPlan, "exists((f)-[`anon_3`:WORKS_AT]->(`anon_4`:ComedyClub))")(pos)),
      None
    )

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("clowns" -> projectionExpression))
        .orderedAggregation(Seq("a AS a"), Seq("collect(b) AS friends"), Seq("a"))
        .filter("b:Person")
        .expandAll("(a)-[anon_2:KNOWS]->(b)")
        .nodeByLabelScan("a", "Person", IndexOrderAscending)
        .build()
    )
  }

  test("should get the right scope for pattern comprehensions in ORDER BY") {
    // The important thing for this test is "RETURN u.id" instead of "RETURN u".
    // Like this the scoping is challenged to propagate `u` from the previous scope into the pattern expression
    val logicalPlan = planner.plan("MATCH (u:User) RETURN u.id ORDER BY size([(u)-[r:FOLLOWS]->(u2:User) | u2.id])")

    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("`u.id`")
        .projection("u.id AS `u.id`")
        .sort(Seq(Ascending("size(anon_6)")))
        .projection("size(anon_1) AS `size(anon_6)`")
        .rollUpApply("anon_1", "anon_0")
        .|.projection("u2.id AS anon_0")
        .|.filter("u2:User")
        .|.expandAll("(u)-[r:FOLLOWS]->(u2)")
        .|.argument("u")
        .nodeByLabelScan("u", "User", IndexOrderNone)
        .build()
    )
  }

  // Please look at the SemiApplyVsGetDegree benchmark.
  // GetDegree is slower on sparse nodes, but faster on dense nodes.
  // We heuristically always choose SemiApply, which will do better on average.

  test("should build plans with SemiApply for a single pattern predicate") {
    val logicalPlan = planner.plan("MATCH (a) WHERE (a)-[:X]->() RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate") {
    val logicalPlan = planner.plan("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with exists") {
    val logicalPlan = planner.plan("MATCH (a) WHERE exists((a)-[:X]->()) RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with exists") {
    val logicalPlan = planner.plan("MATCH (a) WHERE NOT exists((a)-[:X]->()) RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with 0 < COUNT") {
    val logicalPlan = planner.plan("MATCH (a) WHERE 0<COUNT{(a)-[:X]->()} RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[anon_3:X]->(anon_4)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single pattern predicate with 0=COUNT") {
    val logicalPlan = planner.plan("MATCH (a) WHERE 0=COUNT{(a)-[:X]->()} RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[anon_3:X]->(anon_4)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with RollUpApply for a pattern predicate with 0=COUNT with WHERE inside") {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE 0=COUNT{(a)-[:X]->() WHERE a.prop = 'c'} RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .filter("0 = size(anon_1)")
        .rollUpApply("anon_1", "anon_0")
        .|.projection(Map("anon_0" -> PathExpression(
          NodePathStep(
            Variable("a")(pos),
            SingleRelationshipPathStep(
              Variable("anon_3")(pos),
              OUTGOING,
              Some(Variable("anon_4")(pos)),
              NilPathStep()(pos)
            )(pos)
          )(pos)
        )(pos)))
        .|.expandAll("(a)-[anon_3:X]->(anon_4)")
        .|.filter("a.prop = 'c'")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with RollUpApply for a pattern predicate with 0<COUNT with WHERE inside") {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE 0<COUNT{(a)-[:X]->(b) WHERE b.prop = 'c'} RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .filter("0 < size(anon_1)")
        .rollUpApply("anon_1", "anon_0")
        .|.projection(Map("anon_0" -> PathExpression(
          NodePathStep(
            Variable("a")(pos),
            SingleRelationshipPathStep(Variable("anon_3")(pos), OUTGOING, Some(Variable("b")(pos)), NilPathStep()(pos))(
              pos
            )
          )(pos)
        )(pos)))
        .|.filter("b.prop = 'c'")
        .|.expandAll("(a)-[anon_3:X]->(b)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with COUNT > 0 with Label on other node") {
    val logicalPlan = planner.plan("MATCH (a) WHERE COUNT{(a)-[:X]->(:Foo)}>0 RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("anon_4:Foo")
        .|.expandAll("(a)-[anon_3:X]->(anon_4)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should build plans with AntiSemiApply for a single pattern predicate with COUNT = 0 with Label on other node"
  ) {
    val logicalPlan = planner.plan("MATCH (a) WHERE COUNT{(a)-[:X]->(:Foo)}=0 RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("anon_4:Foo")
        .|.expandAll("(a)-[anon_3:X]->(anon_4)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern in a pattern comprehension, with 0 < size(pt)") {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE 0 < size([pt = (a)-[:X]->() | pt]) RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern in a pattern comprehension, 0 < size(pt)") {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE 0=size([pt = (a)-[:X]->() | pt]) RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should build plans with SemiApply for a single pattern in a pattern comprehension, size(pt) > 0, with Label on other node"
  ) {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE size([pt = (a)-[:X]->(:Foo) | pt]) > 0 RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("anon_3:Foo")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should build plans with AntiSemiApply for a single negated pattern in a pattern comprehension, size(pt) = 0, with Label on other node"
  ) {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE size([pt = (a)-[:X]->(:Foo) | pt]) = 0 RETURN a")
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("anon_3:Foo")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with 2 SemiApplies for two pattern predicates") {
    val logicalPlan = planner.plan("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[anon_6:Y]->(anon_7)")
        .|.argument("a")
        .semiApply()
        .|.expandAll("(a)-[anon_4:X]->(anon_5)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SelectOrSemiApply for a pattern predicate and an expression") {
    val logicalPlan = planner.plan("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .selectOrSemiApply("a.prop > 4")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SelectOrSemiApply for a pattern predicate and multiple expressions") {
    val logicalPlan =
      planner.plan("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .selectOrSemiApply("a.prop > 4 OR a.prop2 = 9")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SelectOrAntiSemiApply for a single negated pattern predicate and an expression") {
    val logicalPlan = planner.plan("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("a.prop = 9")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should build plans with LetSelectOrSemiApply and SelectOrAntiSemiApply for two pattern predicates and expressions"
  ) {
    val logicalPlan = planner.plan("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")

    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("anon_9")
        .|.expandAll("(a)-[anon_6:X]->(anon_7)")
        .|.argument("a")
        .letSelectOrSemiApply(
          "anon_9",
          "a.prop = 9"
        ) // anon_9 is used to not go into the RHS of SelectOrAntiSemiApply if not needed
        .|.expandAll("(a)-[anon_4:Y]->(anon_5)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with LetSemiApply and SelectOrAntiSemiApply for two pattern predicates with one negation") {
    val logicalPlan = planner.plan("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")

    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("anon_9")
        .|.expandAll("(a)-[anon_6:X]->(anon_7)")
        .|.argument("a")
        .letSemiApply("anon_9") // anon_9 is used to not go into the RHS of SelectOrAntiSemiApply if not needed
        .|.expandAll("(a)-[anon_4:Y]->(anon_5)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with LetAntiSemiApply and SelectOrAntiSemiApply for two negated pattern predicates") {
    val query = "MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a"
    val logicalPlan = planner.plan(query).stripProduceResults

    logicalPlan should equal(
      planner.subPlanBuilder()
        .selectOrAntiSemiApply("anon_9")
        .|.expandAll("(a)-[anon_6:X]->(anon_7)")
        .|.argument("a")
        .letAntiSemiApply("anon_9") // anon_9 is used to not go into the RHS of SelectOrAntiSemiApply if not needed
        .|.expandAll("(a)-[anon_4:Y]->(anon_5)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with Label on other node") {
    val logicalPlan = planner.plan("MATCH (a) WHERE (a)-[:X]->(:Foo) RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("anon_3:Foo")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with Label on other node") {
    val logicalPlan = planner.plan("MATCH (a) WHERE NOT (a)-[:X]->(:Foo) RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("anon_3:Foo")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with exists with Label on other node") {
    val logicalPlan = planner.plan("MATCH (a) WHERE exists((a)-[:X]->(:Foo)) RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("anon_3:Foo")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should build plans with AntiSemiApply for a single negated pattern predicate with exists with Label on other node"
  ) {
    val logicalPlan = planner.plan("MATCH (a) WHERE NOT exists((a)-[:X]->(:Foo)) RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("anon_3:Foo")
        .|.expandAll("(a)-[anon_2:X]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should plan all predicates along with named varlength pattern") {
    val plan =
      planner.plan("MATCH p=(a)-[r*]->(b) WHERE all(n in nodes(p) WHERE n.prop = 1337) RETURN p").stripProduceResults

    val pathExpression = PathExpression(NodePathStep(
      varFor("a"),
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("p" -> pathExpression))
        .expand(
          "(a)-[r*1..]->(b)",
          expandMode = ExpandAll,
          projectedDir = OUTGOING,
          nodePredicates = Seq(Predicate("n", "n.prop = 1337"))
        )
        .allNodeScan("a")
        .build()
    )
  }

  test("should plan none predicates along with named varlength pattern") {
    val plan =
      planner.plan("MATCH p=(a)-[r*]->(b) WHERE none(n in nodes(p) WHERE n.prop = 1337) RETURN p").stripProduceResults

    val pathExpression = PathExpression(NodePathStep(
      varFor("a"),
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("p" -> pathExpression))
        .expand(
          "(a)-[r*1..]->(b)",
          expandMode = ExpandAll,
          projectedDir = OUTGOING,
          nodePredicates = Seq(Predicate("n", "NOT(n.prop = 1337)"))
        )
        .allNodeScan("a")
        .build()
    )
  }

  test("should solve pattern comprehensions as part of VarExpand") {
    val q =
      """
        |MATCH p= ( (b) -[:REL*0..]- (c) )
        |WHERE
        | ALL(n in nodes(p) where
        |   n.prop <= 1 < n.prop2
        |   AND coalesce(
        |     head( [ (n)<--(d) WHERE d.prop3 <= 1 < d.prop2 | d.prop4 = true ] ),
        |     head( [ (n)<--(e) WHERE e.prop3 <= 1 < e.prop2 | e.prop5 = '0'] ),
        |     true)
        | )
        | AND ALL(r in relationships(p) WHERE r.prop <= 1 < r.prop2)
        |RETURN c
      """.stripMargin

    planner.plan(q) // Should not fail
    // The plan that solves the predicates as part of VarExpand is not chosen, but considered, thus we cannot assert on _that_ plan here.
    // Nevertheless the assertion LogicalPlanProducer.assertNoBadExpressionsExists should never fail, even for plans that do not get chosen.
  }

  test("should not use RollupApply for PatternComprehensions in coalesce") {
    val q =
      """
        |MATCH (a)
        |WHERE coalesce(
        |     head( [ (a)<--(b) | b.prop4 = true ] ),
        |     head( [ (a)<--(c) | c.prop5 = '0'] ),
        |     true)
        |RETURN a
      """.stripMargin

    val plan = planner.plan(q)
    plan.folder.treeExists({
      case _: RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in coalesce v2") {
    val q =
      """
        |MATCH (a)
        |WHERE NOT isEmpty(
        |     coalesce(
        |       [1, 2, 3],
        |       [(a)<--(c) | c.prop5 = '0'],
        |       [true]
        |     ))
        |RETURN a
      """.stripMargin

    val plan = planner.plan(q)
    plan.folder.treeExists({
      case _: RollUpApply => true
    }) should be(false)
  }

  test("should solve pattern comprehension for NodeByIdSeek") {
    val q =
      """
        |MATCH (n)
        |WHERE id(n) = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.nodeByIdSeek("n", Set("anon_1"), "reduce(sum = 0, x IN anon_1 | sum + x)")
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for DirectedRelationshipByIdSeek") {
    val q =
      """
        |MATCH ()-[r]->()
        |WHERE id(r) = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.directedRelationshipByIdSeekExpr("r", "anon_2", "anon_3", Set("anon_1"), reduceExpr)
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_4]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for UndirectedRelationshipByIdSeek") {
    val q =
      """
        |MATCH ()-[r]-()
        |WHERE id(r) = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.undirectedRelationshipByIdSeekExpr("r", "anon_2", "anon_3", Set("anon_1"), reduceExpr)
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_4]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for NodeIndexSeek") { // change labels so that one is unique and the other one nonunique
    val q =
      """
        |MATCH (n:Label)
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator("n:Label(prop = ???)", argumentIds = Set("anon_1"), paramExpr = Seq(reduceExpr))
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for relationship index seek") {
    val q =
      """
        |MATCH ()-[r:REL]->()
        |WHERE r.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(anon_2)-[r:REL(prop = ???)]->(anon_3)",
          argumentIds = Set("anon_1"),
          paramExpr = Seq(reduceExpr)
        )
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_4]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for NodeUniqueIndexSeek") {
    val q =
      """
        |MATCH (n:UniqueLabel)
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator(
          "n:UniqueLabel(prop = ???)",
          unique = true,
          argumentIds = Set("anon_1"),
          paramExpr = Seq(reduceExpr)
        )
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for NodeIndexContainsScan") {
    val q =
      """
        |MATCH (n:TextLabel)
        |WHERE n.prop CONTAINS toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    val toStrings = function("toString", reduceExpr)

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator(
          "n:TextLabel(prop CONTAINS ???)",
          indexType = IndexType.TEXT,
          paramExpr = Seq(toStrings),
          argumentIds = Set("anon_1")
        )
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for DirectedRelationshipIndexContainsScan") {
    val q =
      """
        |MATCH ()-[r:TextRel]->()
        |WHERE r.prop CONTAINS toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))
        |RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    val toStrings = function("toString", reduceExpr)

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(anon_2)-[r:TextRel(prop CONTAINS ???)]->(anon_3)",
          argumentIds = Set("anon_1"),
          indexType = IndexType.TEXT,
          paramExpr = Seq(toStrings)
        )
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_4]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for UndirectedRelationshipIndexEndsWithScan") {
    val q =
      """
        |MATCH ()-[r:TextRel]-()
        |WHERE r.prop ENDS WITH toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))
        |RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    val toStrings = function("toString", reduceExpr)

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(anon_2)-[r:TextRel(prop ENDS WITH ???)]-(anon_3)",
          argumentIds = Set("anon_1"),
          indexType = IndexType.TEXT,
          paramExpr = Seq(toStrings)
        )
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_4]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for NodeIndexEndsWithScan") {
    val q =
      """
        |MATCH (n:TextLabel)
        |WHERE n.prop ENDS WITH toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    val toStrings = function("toString", reduceExpr)

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator(
          "n:TextLabel(prop ENDS WITH ???)",
          indexType = IndexType.TEXT,
          paramExpr = Seq(toStrings),
          argumentIds = Set("anon_1")
        )
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .argument()
        .build()
    )
  }

  test("should solve pattern comprehension for Selection") {
    val q =
      """
        |MATCH (n)
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .filter("n.prop = reduce(sum = 0, x IN anon_1 | sum + x)")
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve pattern comprehension for Horizon Selection") {
    val q =
      """
        |MATCH (n)
        |WITH n, 1 AS one
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .filter("n.prop = reduce(sum = 0, x IN anon_1 | sum + x)")
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.allNodeScan("a")
        .projection("1 AS one")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve pattern comprehension for SelectOrAntiSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE NOT (n)-[:R]->(:M) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .selectOrAntiSemiApply("n.prop = reduce(sum = 0, x IN anon_3 | sum + x)")
        .|.filter("anon_5:M")
        .|.expandAll("(n)-[anon_4:R]->(anon_5)")
        .|.argument("n")
        .rollUpApply("anon_3", "anon_2")
        .|.projection("b.age AS anon_2")
        .|.expandAll("(a)-[anon_6]->(b)")
        .|.allNodeScan("a")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve pattern comprehension for LetSelectOrAntiSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE NOT (n)-[:R]->(:M) OR NOT (n)-[:Q]->(:O) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .selectOrAntiSemiApply("anon_12")
        .|.filter("anon_9:O")
        .|.expandAll("(n)-[anon_8:Q]->(anon_9)")
        .|.argument("n")
        .letSelectOrAntiSemiApply("anon_12", "n.prop = reduce(sum = 0, x IN anon_5 | sum + x)")
        .|.filter("anon_7:M")
        .|.expandAll("(n)-[anon_6:R]->(anon_7)")
        .|.argument("n")
        .rollUpApply("anon_5", "anon_4")
        .|.projection("b.age AS anon_4")
        .|.expandAll("(a)-[anon_10]->(b)")
        .|.allNodeScan("a")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve pattern comprehension for SelectOrSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE (n)-[:R]->(:M) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .selectOrSemiApply("n.prop = reduce(sum = 0, x IN anon_3 | sum + x)")
        .|.filter("anon_5:M")
        .|.expandAll("(n)-[anon_4:R]->(anon_5)")
        .|.argument("n")
        .rollUpApply("anon_3", "anon_2")
        .|.projection("b.age AS anon_2")
        .|.expandAll("(a)-[anon_6]->(b)")
        .|.allNodeScan("a")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve pattern comprehension for LetSelectOrSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE (n)-[:R]->(:M) OR (n)-[:Q]->(:O) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .selectOrSemiApply("anon_12")
        .|.filter("anon_9:O")
        .|.expandAll("(n)-[anon_8:Q]->(anon_9)")
        .|.argument("n")
        .letSelectOrSemiApply("anon_12", "n.prop = reduce(sum = 0, x IN anon_5 | sum + x)")
        .|.filter("anon_7:M")
        .|.expandAll("(n)-[anon_6:R]->(anon_7)")
        .|.argument("n")
        .rollUpApply("anon_5", "anon_4")
        .|.projection("b.age AS anon_4")
        .|.expandAll("(a)-[anon_10]->(b)")
        .|.allNodeScan("a")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name pattern comprehensions for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN [(n)-->(b) | b.age] AS ages
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .rollUpApply("ages", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(n)-[anon_2]->(b)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name nested pattern comprehensions for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN [1, [(n)-->(b) | b.age], 2] AS ages
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .projection("[1, anon_1, 2] AS ages")
        .rollUpApply("anon_1", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(n)-[anon_2]->(b)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name pattern comprehensions nested in pattern comprehensions for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN [(n)-->(b) | [(b)-->(c) | c.age]] AS ages
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .rollUpApply("ages", "anon_2")
        .|.rollUpApply("anon_2", "anon_0")
        .|.|.projection("c.age AS anon_0")
        .|.|.expandAll("(b)-[anon_5]->(c)")
        .|.|.argument("b")
        .|.expandAll("(n)-[anon_4]->(b)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name pattern comprehensions with named paths for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN [p = (n)-->(b) | p] AS ages
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .rollUpApply("ages", "anon_0")
        .|.projection(Map("anon_0" -> PathExpression(
          NodePathStep(
            varFor("n"),
            SingleRelationshipPathStep(varFor("anon_2"), OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
          )(pos)
        )(pos)))
        .|.expandAll("(n)-[anon_2]->(b)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name pattern comprehensions for Aggregation, grouping expression") {
    val q =
      """
        |MATCH (n)
        |RETURN [(n)-->(b) | b.age] AS ages, sum(n.foo)
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .aggregation(Seq("ages AS ages"), Seq("sum(n.foo) AS `sum(n.foo)`"))
        .rollUpApply("ages", "anon_0")
        .|.projection("b.age AS anon_0")
        .|.expandAll("(n)-[anon_2]->(b)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve pattern comprehensions for Aggregation, aggregation expression") {
    val q =
      """
        |MATCH (n)
        |RETURN collect([(n)-->(b) | b.age]) AS ages, n.foo
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq("n.foo AS `n.foo`"), Seq("collect(anon_1) AS ages"))
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(n)-[anon_2]->(b)")
      .|.argument("n")
      .allNodeScan("n")
      .build())
  }

  test("should solve and name pattern comprehensions for Distinct") {
    val q =
      """
        |MATCH (n)
        |RETURN DISTINCT [(n)-->(b) | b.age] AS ages
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .distinct("ages AS ages")
      .rollUpApply("ages", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(n)-[anon_2]->(b)")
      .|.argument("n")
      .allNodeScan("n")
      .build())
  }

  test("should solve pattern comprehensions for LoadCSV") {
    val q =
      """
        |LOAD CSV FROM toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)) AS foo RETURN foo
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .loadCSV("toString(reduce(sum = 0, x IN anon_1 | sum + x))", "foo", NoHeaders, None)
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .argument()
      .build())
  }

  test("should solve pattern comprehensions for Unwind") {
    val q =
      """
        |UNWIND [reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)] AS foo RETURN foo
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .unwind("[reduce(sum = 0, x IN anon_1 | sum + x)] AS foo")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .argument()
      .build())
  }

  test("should solve pattern comprehensions for ShortestPath") {
    val q =
      """
        |MATCH p=shortestPath((n)-[r*..6]-(n2)) WHERE NONE(n in nodes(p) WHERE n.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)) RETURN n, n2
      """.stripMargin

    val nestedPlan = planner.subPlanBuilder()
      .projection("b.age AS anon_0")
      .expand("(a)-[anon_2]->(b)")
      .allNodeScan("a")
      .build()
    val nestedCollection =
      NestedPlanCollectExpression(nestedPlan, varFor("anon_0"), "[(a)-[`anon_2`]->(b) | b.age]")(pos)
    val reduceExprWithNestedPlan = reduce(
      varFor("sum", pos),
      literalInt(0, pos),
      varFor("x", pos),
      nestedCollection,
      add(varFor("sum", pos), varFor("x", pos))
    )
    val expr = NoneIterablePredicate(
      FilterScope(varFor("n", pos), Some(equals(prop("n", "foo", pos), reduceExprWithNestedPlan)))(pos),
      function("nodes", varFor("p", pos))
    )(pos)
    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .shortestPathExpr("(n)-[r*1..6]-(n2)", pathName = Some("p"), predicates = Seq(expr))
      .cartesianProduct()
      .|.allNodeScan("n2")
      .allNodeScan("n")
      .build())
  }

  test("should solve pattern comprehensions for Create") {
    val q =
      """
        |CREATE (n {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)}) RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .create(createNodeWithProperties("n", Seq(), "{foo: reduce(sum = 0, x IN anon_1 | sum + x)}"))
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .argument()
      .build())
  }

  test("should solve pattern comprehensions for MERGE node") {
    val q =
      """
        |MERGE (n {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)}) RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .merge(Seq(createNodeWithProperties("n", Seq(), "{foo: reduce(sum = 0, x IN anon_1 | sum + x)}")))
      .filter("n.foo = reduce(sum = 0, x IN anon_1 | sum + x)")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .allNodeScan("n")
      .build())
  }

  test("should solve pattern comprehensions for MERGE relationship") {
    val q =
      """
        |MERGE ()-[r:R {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)}]->() RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .merge(
        Seq(createNode("anon_2"), createNode("anon_4")),
        Seq(createRelationship(
          "r",
          "anon_2",
          "R",
          "anon_4",
          OUTGOING,
          Some("{foo: reduce(sum = 0, x IN anon_1 | sum + x)}")
        ))
      )
      .filter("r.foo = reduce(sum = 0, x IN anon_1 | sum + x)")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_3]->(b)")
      .|.allNodeScan("a")
      .relationshipTypeScan("(anon_2)-[r:R]->(anon_4)", IndexOrderNone)
      .build())
  }

  test("should solve pattern comprehensions for DeleteNode") {
    val q =
      """
        |MATCH (n)
        |WITH [n] AS nodes
        |DELETE nodes[reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)]
      """.stripMargin

    val nestedPlan = planner.subPlanBuilder()
      .projection("b.age AS anon_0")
      .expand("(a)-[anon_2]->(b)")
      .allNodeScan("a")
      .build()
    val nestedCollection =
      NestedPlanCollectExpression(nestedPlan, varFor("anon_0"), "[(a)-[`anon_2`]->(b) | b.age]")(pos)
    val reduceExprWithNestedPlan = reduce(
      varFor("sum", pos),
      literalInt(0, pos),
      varFor("x", pos),
      nestedCollection,
      add(varFor("sum", pos), varFor("x", pos))
    )
    val expr = ContainerIndex(Variable("nodes")(pos), reduceExprWithNestedPlan)(pos)
    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .deleteNode(expr)
      .eager(ListSet(EagernessReason.ReadDeleteConflict("nodes")))
      .projection("[n] AS nodes")
      .allNodeScan("n")
      .build())
  }

  test("should solve pattern comprehensions for DeleteRelationship") {
    val q =
      """
        |MATCH ()-[r]->()
        |WITH [r] AS rels
        |DELETE rels[reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)]
      """.stripMargin

    val nestedPlan = planner.subPlanBuilder()
      .projection("b.age AS anon_0")
      .expand("(a)-[anon_4]->(b)")
      .allNodeScan("a")
      .build()
    val nestedCollection =
      NestedPlanCollectExpression(nestedPlan, varFor("anon_0"), "[(a)-[`anon_4`]->(b) | b.age]")(pos)
    val reduceExprWithNestedPlan = reduce(
      varFor("sum", pos),
      literalInt(0, pos),
      varFor("x", pos),
      nestedCollection,
      add(varFor("sum", pos), varFor("x", pos))
    )
    val expr = ContainerIndex(Variable("rels")(pos), reduceExprWithNestedPlan)(pos)
    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .deleteRelationship(expr)
      .eager(ListSet(EagernessReason.ReadDeleteConflict("rels")))
      .projection("[r] AS rels")
      .expandAll("(anon_2)-[r]->(anon_3)")
      .allNodeScan("anon_2")
      .build())
  }

  test("should solve pattern comprehensions for DeleteExpression") {
    val q =
      """
        |MATCH ()-[r]->()
        |WITH {rel: r} AS rels
        |DELETE rels[toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))]
      """.stripMargin

    val nestedPlan = planner.subPlanBuilder()
      .projection("b.age AS anon_0")
      .expand("(a)-[anon_4]->(b)")
      .allNodeScan("a")
      .build()
    val nestedCollection =
      NestedPlanCollectExpression(nestedPlan, varFor("anon_0"), "[(a)-[`anon_4`]->(b) | b.age]")(pos)
    val reduceExprWithNestedPlan = reduce(
      varFor("sum", pos),
      literalInt(0, pos),
      varFor("x", pos),
      nestedCollection,
      add(varFor("sum", pos), varFor("x", pos))
    )
    val toStrings = function("toString", reduceExprWithNestedPlan)
    val expr = containerIndex(varFor("rels", pos), toStrings)
    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .deleteExpression(expr)
      .eager(ListSet(EagernessReason.ReadDeleteConflict("rels")))
      .projection("{rel: r} AS rels")
      .expandAll("(anon_2)-[r]->(anon_3)")
      .allNodeScan("anon_2")
      .build())
  }

  test("should solve pattern comprehensions for SetNodeProperty") {
    val q =
      """
        |MATCH (n) SET n.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x) RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .setNodeProperty("n", "foo", "reduce(sum = 0, x IN anon_1 | sum + x)")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .allNodeScan("n")
      .build())
  }

  test("should solve pattern comprehensions for SetNodePropertiesFromMap") {
    val q =
      """
        |MATCH (n) SET n = {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)} RETURN n
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .setNodePropertiesFromMap("n", "{foo: reduce(sum = 0, x IN anon_1 | sum + x)}", true)
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .allNodeScan("n")
      .build())
  }

  test("should solve pattern comprehensions for SetRelationshipProperty") {
    val q =
      """
        |MATCH ()-[r]->() SET r.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x) RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .setRelationshipProperty("r", "foo", "reduce(sum = 0, x IN anon_1 | sum + x)")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_4]->(b)")
      .|.allNodeScan("a")
      .expandAll("(anon_2)-[r]->(anon_3)")
      .allNodeScan("anon_2")
      .build())
  }

  test("should solve pattern comprehensions for SetRelationshipPropertiesFromMap") {
    val q =
      """
        |MATCH ()-[r]->() SET r = {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)} RETURN r
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .setRelationshipPropertiesFromMap("r", "{foo: reduce(sum = 0, x IN anon_1 | sum + x)}", true)
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_4]->(b)")
      .|.allNodeScan("a")
      .expandAll("(anon_2)-[r]->(anon_3)")
      .allNodeScan("anon_2")
      .build())
  }

  test("should solve pattern comprehensions for SetProperty") {
    val q =
      """
        |SET $param.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .setProperty("$param", "foo", "reduce(sum = 0, x IN anon_1 | sum + x)")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .argument()
      .build())
  }

  test("should solve pattern comprehensions for ForeachApply") {
    val q =
      """
        |FOREACH (num IN [reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)] |
        |  MERGE ({foo: num}) )
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .foreachApply("num", "[reduce(sum = 0, x IN anon_1 | sum + x)]")
      .|.merge(Seq(createNodeWithProperties("anon_3", Seq(), "{foo: num}")))
      .|.filter("anon_3.foo = num")
      .|.allNodeScan("anon_3", "num")
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .argument()
      .build())
  }

  test("should solve pattern comprehensions for Foreach") {
    val q =
      """
        |FOREACH (num IN [reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)] |
        |  CREATE ({foo: num}) )
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults
    println(plan.printLogicalPlanBuilderString())

    plan should equal(planner.subPlanBuilder()
      .emptyResult()
      .foreach(
        "num",
        "[reduce(sum = 0, x IN anon_1 | sum + x)]",
        Seq(createPattern(Seq(createNodeWithProperties("anon_3", Seq(), "{foo: num}")), Seq()))
      )
      .rollUpApply("anon_1", "anon_0")
      .|.projection("b.age AS anon_0")
      .|.expandAll("(a)-[anon_2]->(b)")
      .|.allNodeScan("a")
      .eager(ListSet(EagernessReason.Unknown))
      .argument()
      .build())
  }

  test("should not use RollupApply for PatternComprehensions in head") {
    val q =
      """
        |MATCH (a)
        |WHERE head( [(a)<--(b) | b.prop4 = true] ) = true
        |RETURN a
      """.stripMargin

    val plan = planner.plan(q)
    plan.folder.treeExists({
      case _: RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in container index") {
    val q =
      """
        |MATCH (a)
        |WHERE [(a)<--(b) | b.prop4 = true][2]
        |RETURN a
      """.stripMargin

    val plan = planner.plan(q)
    plan.folder.treeExists({
      case _: RollUpApply => true
    }) should be(false)
  }

  test("should solve and name pattern comprehensions with NestedPlanExpression for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN [(n)-->(b) | b.age][1] AS age
        """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .limit(add(literalInt(1), literalInt(1)))
      .projection("b.age AS anon_0")
      .expand("(n)-[anon_2]->(b)")
      .argument("n")
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("age" -> containerIndex(
          NestedPlanCollectExpression(expectedNestedPlan, varFor("anon_0"), "[(n)-[`anon_2`]->(b) | b.age]")(pos),
          literalInt(1)
        )))
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name pattern expressions with NestedPlanExpression for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN exists( (n)-->()-->() ) AS foo
        """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .filter("not anon_2 = anon_4")
      .expand("(anon_3)-[anon_4]->(anon_5)")
      .expand("(n)-[anon_2]->(anon_3)")
      .argument("n")
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("foo" ->
          NestedPlanExistsExpression(expectedNestedPlan, "exists((n)-[`anon_2`]->(`anon_3`)-[`anon_4`]->(`anon_5`))")(
            pos
          )))
        .allNodeScan("n")
        .build()
    )
  }

  test("should solve and name pattern expressions with inlined Label filter with NestedPlanExpression for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN exists( (n)-->(:B) ) AS foo
        """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .filter("anon_3:B")
      .expand("(n)-[anon_2]->(anon_3)")
      .argument("n")
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("foo" ->
          NestedPlanExistsExpression(expectedNestedPlan, "exists((n)-[`anon_2`]->(`anon_3`:B))")(pos)))
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "should solve and name pattern expressions with two disjoint inlined Label filters with NestedPlanExpression for Projection"
  ) {
    val q =
      """
        |MATCH (n)
        |RETURN exists( (n)-->(:B|C) ) AS foo
        """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .filterExpression(hasAnyLabel("anon_3", "B", "C"))
      .expand("(n)-[anon_2]->(anon_3)")
      .argument("n")
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("foo" ->
          NestedPlanExistsExpression(expectedNestedPlan, "exists((n)-[`anon_2`]->(`anon_3`:B|C))")(pos)))
        .allNodeScan("n")
        .build()
    )
  }

  test("should use index seeks for subqueries if suitable") {
    val q =
      """
        |MATCH (n)
        |RETURN exists( (n)-->(:D {prop: 5}) ) AS foo
        """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .expandInto("(n)-[anon_2]->(anon_3)")
      .nodeIndexOperator("anon_3:D(prop=5)", argumentIds = Set("n"))
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("foo" ->
          NestedPlanExistsExpression(expectedNestedPlan, "exists((n)-[`anon_2`]->(`anon_3`:D {prop: 5}))")(pos)))
        .allNodeScan("n")
        .build()
    )
  }

  test("should not use RollupApply for PatternComprehensions in list slice to") {
    val q =
      """
        |MATCH (a)
        |WHERE NOT isEmpty([(a)<--(b) | b.prop4 = true][..5])
        |RETURN a
      """.stripMargin

    val plan = planner.plan(q)
    plan.folder.treeExists({
      case _: RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in list slice from/to") {
    val q =
      """
        |MATCH (a)
        |WHERE NOT isEmpty([(a)<--(b) | b.prop4 = true ][2..5])
        |RETURN a
      """.stripMargin

    val plan = planner.plan(q)
    plan.folder.treeExists({
      case _: RollUpApply => true
    }) should be(false)
  }

  test("should solve pattern comprehension in size function with GetDegree") {
    val q =
      """
        |MATCH (a)
        |RETURN size([p=(a)-[:FOO]->() | p]) AS size
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection(Map("size" -> GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos)))
      .allNodeScan("a")
      .build())
  }

  test("should solve pattern expression in exists with GetDegree") {
    val q =
      """
        |MATCH (a)
        |RETURN exists((a)-[:FOO]->()) AS exists
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection(Map("exists" -> HasDegreeGreaterThan(
        varFor("a"),
        Some(RelTypeName("FOO")(pos)),
        SemanticDirection.OUTGOING,
        literalInt(0)
      )(pos)))
      .allNodeScan("a")
      .build())
  }

  test("should honor relationship uniqueness for pattern expression") {
    val logicalPlan = planner.plan("MATCH (a) WHERE (a)-->()-->() RETURN a")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("not anon_2 = anon_4") // This filter upholds rel uniqueness
        .|.expandAll("(anon_3)-[anon_4]->(anon_5)")
        .|.expandAll("(a)-[anon_2]->(anon_3)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should honor relationship uniqueness for pattern comprehension") {
    val logicalPlan = planner.plan("MATCH (a) RETURN [(a)-->(b)-->(c) | c.prop] AS props")
    logicalPlan should equal(
      planner.planBuilder()
        .produceResults("props")
        .rollUpApply("props", "anon_0")
        .|.projection("c.prop AS anon_0")
        .|.filter("not anon_2 = anon_3") // This filter upholds rel uniqueness
        .|.expandAll("(b)-[anon_3]->(c)")
        .|.expandAll("(a)-[anon_2]->(b)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }
}
