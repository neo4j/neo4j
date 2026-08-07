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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.UsingMatcher.using
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipWithDynamicType
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny

class MergeRelationshipPlanningIntegrationTest
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private def plannerConfigForSimpleExpandTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("()-[:R]->()", 100)
      .setRelationshipCardinality("()-[:R]->(:A)", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", 100)
      .build()

  test("should plan simple expand") {
    val cfg = plannerConfigForSimpleExpandTests()

    val plan = cfg.plan("MERGE (a:A)-[r:R]->(b)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(
        nodes = Seq(createNode("a", "A"), createNode("b")),
        relationships = Seq(createRelationship("r", "a", "R", "b", OUTGOING))
      )
      .expandAll("(a)-[r:R]->(b)")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("should plan simple expand with argument dependency") {
    val cfg = plannerConfigForSimpleExpandTests()

    val plan = cfg.plan("WITH 42 AS arg MERGE (a:A {p: arg})-[r:R]->(b)").stripProduceResults

    val mergeNodes = Seq(createNodeWithProperties("a", Seq("A"), "{p: arg}"), createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "a", "R", "b", OUTGOING))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(mergeNodes, mergeRelationships)
      .|.expandAll("(a)-[r:R]->(b)")
      .|.filter("a.p = arg")
      .|.nodeByLabelScan("a", "A", "arg")
      .projection("42 AS arg")
      .argument()
      .build()
  }

  test("should use AssertSameNode when multiple unique index matches") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 50)
      .setLabelCardinality("Y", 50)
      .addNodeIndex("X", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .addNodeIndex("Y", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .setRelationshipCardinality("(:X)-[:T]->()", 100)
      .setRelationshipCardinality("(:Y)-[:T]->()", 100)
      .setRelationshipCardinality("()-[:T]->()", 100)
      .setRelationshipCardinality("()-[:T]->(:X)", 0)
      .setRelationshipCardinality("()-[:T]->(:Y)", 0)
      .build()

    val plan = cfg.plan("MERGE (a:X:Y {prop: 42})-[:T]->(b)").stripProduceResults

    plan shouldBe using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should not use AssertSameNode when one unique index matches") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 50)
      .setLabelCardinality("Y", 50)
      .addNodeIndex("X", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .setRelationshipCardinality("(:X)-[:T]->()", 100)
      .setRelationshipCardinality("(:Y)-[:T]->()", 100)
      .build()

    val plan = cfg.plan("MERGE (a:X:Y {prop: 42})").stripProduceResults

    plan should not be using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 100)
      .build()

    val plan = cfg.plan("MATCH (n) MERGE (n)-[r:T]->(b)").stripProduceResults

    val mergeNodes = Seq(createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "n", "T", "b", OUTGOING))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(mergeNodes, mergeRelationships, lockNodes = Set("n"))
      .|.expandAll("(n)-[r:T]->(b)")
      .|.argument("n")
      .allNodeScan("n")
      .build()
  }

  private def plannerConfigForMergeOnExistingVariableTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 10000)
      .build()

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val cfg = plannerConfigForMergeOnExistingVariableTests()

    val plan = cfg.plan("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.mergeInto("(n)-[r:T]->(m)")
      .|.argument("n", "m")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    val cfg = plannerConfigForMergeOnExistingVariableTests()

    val plan = cfg.plan("MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[r:T]->(b)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.mergeInto("(a)-[r:T]->(b)")
      .|.argument("a", "b")
      .projection("n AS a", "m AS b")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test(
    "should plan only one create node when the other node is already in scope and aliased when creating a relationship"
  ) {
    val cfg = plannerConfigForMergeOnExistingVariableTests()

    val plan = cfg.plan("MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b)").stripProduceResults

    val mergeNodes = Seq(createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "a", "T", "b", OUTGOING))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(mergeNodes, mergeRelationships, lockNodes = Set("a"))
      .|.expandAll("(a)-[r:T]->(b)")
      .|.argument("a")
      .projection("n AS a")
      .allNodeScan("n")
      .build()
  }

  test("should plan relationship unique index seek under MERGE") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 1, uniqueSelectivity = 0.01, isUnique = true)
      .build()

    val plan = cfg.plan("MERGE (a)-[r:REL {prop: 123}]->(b)").stripProduceResults

    val mergeNodes = Seq(createNode("a"), createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "a", "REL", "b", OUTGOING, Some("{prop: 123}")))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(mergeNodes, mergeRelationships)
      .relationshipIndexOperator("(a)-[r:REL(prop = 123)]->(b)", unique = true)
      .build()
  }

  test("should plan assert same relationship on top of multiple unique index seeks under MERGE") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 1, uniqueSelectivity = 0.01, isUnique = true)
      .addRelationshipIndex(
        "REL",
        Seq("prop2", "prop3"),
        existsSelectivity = 1,
        uniqueSelectivity = 0.01,
        isUnique = true
      )
      .build()

    val plan = cfg.plan("MERGE (a)-[r:REL {prop: 123, prop2: 42, prop3: 'welp'}]->(b)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(
        nodes = Seq(
          createNode("a"),
          createNode("b")
        ),
        relationships = Seq(
          createRelationship("r", "a", "REL", "b", OUTGOING, Some("{prop: 123, prop2: 42, prop3: 'welp'}"))
        )
      )
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(a)-[r:REL(prop2 = 42, prop3 = 'welp')]->(b)", unique = true)
      .relationshipIndexOperator("(a)-[r:REL(prop = 123)]->(b)", unique = true)
      .build()
  }

  test("should plan merge multiple names relationships and reused variable") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .build()
    val plan = cfg.plan("WITH [b IN [1]] AS ignored MERGE (a)-[b:REL]->(c)<-[d:REL]-(e)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(
        Seq(createNode("a"), createNode("c"), createNode("e")),
        Seq(createRelationship("b", "a", "REL", "c", OUTGOING), createRelationship("d", "c", "REL", "e", INCOMING)),
        Seq(),
        Seq(),
        Set()
      )
      .|.filter("not d = b")
      .|.expandAll("(c)<-[b:REL]-(a)")
      .|.relationshipTypeScan("(e)-[d:REL]->(c)", IndexOrderNone)
      .projection("[b IN [1]] AS ignored")
      .argument()
      .build()
  }

  test("should not cache inaccessible variable on the RHS of a MERGE apply plan") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("Role", 100)
      .setLabelCardinality("Privilege", 100)
      .setRelationshipCardinality("()-[:GRANTED]->()", 100)
      .setRelationshipCardinality("(:Role)-[:GRANTED]->(:Privilege)", 100)
      .setRelationshipCardinality("(:Role)-[:GRANTED]->()", 100)
      .setRelationshipCardinality("()-[:GRANTED]->(:Privilege)", 100)
      .setRelationshipCardinality("(:Privilege)-[:GRANTED]->()", 0)
      .setRelationshipCardinality("()-[:GRANTED]->(:Role)", 0)
      .build()

    val query =
      """
        |MATCH (to:Role {name: $`__internal_toRole`})
        |MATCH (from:Role {name: $`__internal_fromRole`})-[gFrom:GRANTED]->(p:Privilege)
        |MERGE (to)-[gTo:GRANTED {immutable: coalesce(gFrom.immutable, false)}]->(p)
        |RETURN from.name, to.name, count(gTo)
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .aggregation(
        Seq("cacheN[from.name] AS `from.name`", "cacheN[to.name] AS `to.name`"),
        Seq("count(gTo) AS `count(gTo)`")
      )
      .apply()
      .|.merge(
        relationships = Seq(createRelationship(
          "gTo",
          "to",
          "GRANTED",
          "p",
          OUTGOING,
          Some("{immutable: coalesce(cacheRFromStore[gFrom.immutable], false)}")
        )),
        lockNodes = Set("to", "p")
      )
      .|.cacheProperties("cacheN[to.name]")
      .|.filter("gTo.immutable = coalesce(cacheRFromStore[gFrom.immutable], false)")
      .|.expandInto("(to)-[gTo:GRANTED]->(p)")
      .|.argument("to", "p", "gFrom")
      .eager(ListSet(TypeReadSetConflict(relTypeName("GRANTED")).withConflict(Conflict(Id(3), Id(11)))))
      .cartesianProduct()
      .|.filter("p:Privilege")
      .|.expandAll("(from)-[gFrom:GRANTED]->(p)")
      .|.filter("cacheNFromStore[from.name] = $__internal_fromRole")
      .|.nodeByLabelScan("from", "Role")
      .filter("cacheNFromStore[to.name] = $__internal_toRole")
      .nodeByLabelScan("to", "Role")
      .build()
  }

  test("merge a copy of each existing node, copying its labels dynamically, with a relationship to the original") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setLabelCardinality("Copy", 1)
        .setRelationshipCardinality("()-[:COPY_OF]->()", 1)
        .setRelationshipCardinality("(:Copy)-[:COPY_OF]->()", 1)
        .setRelationshipCardinality("()-[:COPY_OF]->(:Copy)", 0)
        .build()

    val query =
      """MATCH (n)
        |MERGE (copy:Copy:$(labels(n)))-[copy_of:COPY_OF]->(n)""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.merge(
        nodes = Seq(createNodeFull("copy", labels = Seq("Copy"), dynamicLabels = Seq("labels(n)"))),
        relationships = Seq(createRelationship("copy_of", "copy", "COPY_OF", "n", OUTGOING)),
        lockNodes = Set("n")
      )
      .|.filter(
        hasDynamicLabels(varFor("copy"), function("labels", varFor("n"))),
        hasLabels(varFor("copy"), "Copy")
      )
      .|.expandAll("(n)<-[copy_of:COPY_OF]-(copy)")
      .|.argument("n")
      .allNodeScan("n")
      .build()
  }

  test("merge a relationship with a dynamic type and then try to read") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(1000)
        .setLabelCardinality("A", 100)
        .setLabelCardinality("B", 100)
        .setRelationshipCardinality("()-[]->()", 1000)
        .setRelationshipCardinality("(:A)-[]->()", 250)
        .setRelationshipCardinality("()-[]->(:A)", 250)
        .setRelationshipCardinality("(:A)-[]->(:A)", 100)
        .setRelationshipCardinality("(:B)-[]->()", 250)
        .setRelationshipCardinality("()-[]->(:B)", 250)
        .setRelationshipCardinality("(:B)-[]->(:B)", 100)
        .build()

    val query =
      """MERGE (:A)-[:$("Foo")]->(:A)
        |WITH *
        |MATCH (:B)-[r:!Foo]->(:B)
        |RETURN r
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("r")
      .filter(not(hasTypes("r", "Foo")), hasLabels("anon_4", "B"))
      .expandAll("(anon_3)-[r]->(anon_4)")
      .apply()
      .|.nodeByLabelScan("anon_3", "B", IndexOrderNone, "anon_0", "anon_2", "anon_1")
      .eager(ListSet(EagernessReason.ReadCreateConflict.withConflict(EagernessReason.Conflict(Id(6), Id(2)))))
      .merge(
        nodes = Seq(createNodeFull("anon_0", labels = Seq("A")), createNodeFull("anon_2", labels = Seq("A"))),
        relationships = Seq(createRelationshipWithDynamicType("anon_1", "anon_0", "'Foo'", "anon_2", OUTGOING))
      )
      .filter(
        hasDynamicType(varFor("anon_1"), literal("Foo")),
        hasLabels("anon_2", "A")
      )
      .expandAll("(anon_0)-[anon_1]->(anon_2)")
      .nodeByLabelScan("anon_0", "A", IndexOrderNone)
      .build()
  }

  test("should not plan mergeInto if optimization disabled") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 10000)
      .withSetting(GraphDatabaseInternalSettings.merge_optimization_enabled, Boolean.box(false))
      .build()

    val plan = cfg.plan("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(Seq(), Seq(createRelationship("r", "n", "T", "m", OUTGOING)), Seq(), Seq(), Set("n", "m"))
      .|.expandInto("(n)-[r:T]->(m)")
      .|.argument("n", "m")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test("should plan multi-hop MERGE when only some endpoint nodes have a unique index") {
    // Previously, we would fail any merge pattern with more than one hop and a unique index on only one of the end nodes:
    // PriorityLeafPlannerList suppresses other leaf plans on that pattern.
    // However, for initialising the IDP table we assume a leaf plan per symbol, which is why this failed in SingleComponentPlanner.doInitTable.
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Organization", 10)
      .setLabelCardinality("Tasks", 10)
      .setLabelCardinality("Task", 500)
      .setRelationshipCardinality("()-[:HAS_TASKS]->()", 10)
      .setRelationshipCardinality("(:Organization)-[:HAS_TASKS]->()", 10)
      .setRelationshipCardinality("()-[:HAS_TASKS]->(:Tasks)", 10)
      .setRelationshipCardinality("(:Organization)-[:HAS_TASKS]->(:Tasks)", 10)
      .setRelationshipCardinality("()-[:HAS_TASK]->()", 500)
      .setRelationshipCardinality("(:Tasks)-[:HAS_TASK]->()", 500)
      .setRelationshipCardinality("()-[:HAS_TASK]->(:Task)", 500)
      .setRelationshipCardinality("(:Tasks)-[:HAS_TASK]->(:Task)", 500)
      .addNodeIndex("Organization", Seq("id"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, isUnique = true)
      .build()

    planner.plan(
      """MERGE (o:Organization {id: $organization_id})-[:HAS_TASKS]->(:Tasks {organization_id: $organization_id})-[:HAS_TASK]->(t:Task {id: $task_id})"""
    ) should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .merge(
          Seq(
            createNodeFull("o", labels = Seq("Organization"), properties = Some("{id: $organization_id}")),
            createNodeFull("anon_1", labels = Seq("Tasks"), properties = Some("{organization_id: $organization_id}")),
            createNodeFull("t", labels = Seq("Task"), properties = Some("{id: $task_id}"))
          ),
          Seq(
            createRelationship("anon_0", "o", "HAS_TASKS", "anon_1", OUTGOING),
            createRelationship("anon_2", "anon_1", "HAS_TASK", "t", OUTGOING)
          )
        )
        .filter("t.id = $task_id", "t:Task")
        .expandAll("(anon_1)-[anon_2:HAS_TASK]->(t)")
        .filter("anon_1.organization_id = $organization_id", "anon_1:Tasks")
        .expandAll("(o)-[anon_0:HAS_TASKS]->(anon_1)")
        .nodeIndexOperator(
          "o:Organization(id = ???)",
          paramExpr = Seq(parameter("organization_id", CTAny)),
          unique = true
        )
        .build()
    )
  }

  test("should plan assertSameRelationship on top of multiple unique index seeks under MERGE with bound nodes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 1, uniqueSelectivity = 0.01, isUnique = true)
      .addRelationshipIndex(
        "REL",
        Seq("prop2", "prop3"),
        existsSelectivity = 1,
        uniqueSelectivity = 0.01,
        isUnique = true
      )
      .build()

    val plan = planner.plan("MATCH (a), (b) MERGE (a)-[r:REL {prop: 123, prop2: 42, prop3: 'welp'}]->(b)")

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .apply()
        .|.merge(
          relationships =
            Seq(createRelationship("r", "a", "REL", "b", OUTGOING, Some("{prop: 123, prop2: 42, prop3: 'welp'}"))),
          lockNodes = Set("a", "b")
        )
        // Two unique indexes cover different properties of the same relationship type.
        // AssertSameRelationship is required to detect constraint conflicts: if the two index scans
        // return different relationships, a suitable error should be raised.
        // Usually, this would be planned by one relationship leaf plan and a filter,
        // which would simply return no results instead of failing.
        .|.assertSameRelationship("r")
        .|.|.filter("a = anon_2", "b = anon_3")
        .|.|.relationshipIndexOperator(
          "(anon_2)-[r:REL(prop2 = 42, prop3 = 'welp')]->(anon_3)",
          argumentIds = Set("a", "b"),
          unique = true
        )
        .|.filter("a = anon_0", "b = anon_1")
        .|.relationshipIndexOperator(
          "(anon_0)-[r:REL(prop = 123)]->(anon_1)",
          argumentIds = Set("a", "b"),
          unique = true
        )
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
        .build()
    )
  }
}
