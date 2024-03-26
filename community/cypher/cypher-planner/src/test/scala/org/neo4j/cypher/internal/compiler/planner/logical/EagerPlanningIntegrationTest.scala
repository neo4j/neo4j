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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Unknown
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class EagerPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      // This makes it deterministic which plans ends up on what side of a CartesianProduct.
      .setExecutionModel(Volcano)

  // These are simply kept here to ease backporting code from 5.x to 4.4
  def lpReasons(reasons: EagernessReason.Reason*): Seq[EagernessReason.Reason] = Seq(Unknown)

  implicit class OptionallyEagerPlannerBuilder(b: LogicalPlanBuilder) {

    def irEager(reasons: Seq[EagernessReason.Reason] = Seq(Unknown)): LogicalPlanBuilder = b.eager(reasons.toSeq)

    def lpEager(reasons: Seq[EagernessReason.Reason] = Seq(Unknown)): LogicalPlanBuilder = b.resetIndent()
  }

  test(
    "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[:BAR]->() RETURN count(*)"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("C", 60)
      .setLabelCardinality("B", 50)
      .setRelationshipCardinality("()-[:BAR]->()", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->()", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->(:B)", 10)
      .setRelationshipCardinality("(:C)-[:BAR]->()", 10)
      .build()

    val query =
      "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[:BAR]->() RETURN count(*)"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filterExpression(assertIsNode("c"))
        .semiApply()
        .|.expandAll("(c)-[anon_3:BAR]->(anon_4)")
        .|.argument("c")
        .eager()
        .apply()
        .|.merge(
          Seq(createNode("b", "B")),
          Seq(createRelationship("anon_2", "a", "BAR", "b", OUTGOING)),
          lockNodes = Set("a")
        )
        .|.filter("b:B")
        .|.expandAll("(a)-[anon_2:BAR]->(b)")
        .|.argument("a")
        .cartesianProduct()
        .|.nodeByLabelScan("c", "C")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test(
    "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[]->() RETURN count(*)"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("C", 60)
      .setLabelCardinality("B", 50)
      .setRelationshipCardinality("()-[:BAR]->()", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->()", 10)
      .setRelationshipCardinality("(:A)-[:BAR]->(:B)", 10)
      .setRelationshipCardinality("(:C)-[:BAR]->()", 10)
      .setRelationshipCardinality("(:C)-[]->()", 10)
      .build()

    val query =
      "MATCH (a:A), (c:C) MERGE (a)-[:BAR]->(b:B) WITH c MATCH (c) WHERE (c)-[]->() RETURN count(*)"

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .filterExpression(assertIsNode("c"))
        .semiApply()
        .|.expandAll("(c)-[anon_3]->(anon_4)")
        .|.argument("c")
        .eager()
        .apply()
        .|.merge(
          Seq(createNode("b", "B")),
          Seq(createRelationship("anon_2", "a", "BAR", "b", OUTGOING)),
          lockNodes = Set("a")
        )
        .|.filter("b:B")
        .|.expandAll("(a)-[anon_2:BAR]->(b)")
        .|.argument("a")
        .cartesianProduct()
        .|.nodeByLabelScan("c", "C")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("Relationship in get degree requires an eager operator before detach delete") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Resource", 100)
      .setRelationshipCardinality("()-[:DEPENDS_ON]->()", 50)
      .setRelationshipCardinality("(:Resource)-[:DEPENDS_ON]->()", 50)
      .build()

    val query = """MATCH (resource:Resource)
                  |  WHERE exists((resource)-[:DEPENDS_ON]->())
                  |DETACH DELETE resource""".stripMargin

    val plan = planner.plan(query)

    plan should equal(
      planner.planBuilder()
        .produceResults()
        .emptyResult()
        .detachDeleteNode("resource")
        .eager(Seq(EagernessReason.DeleteOverlap(Seq("resource"))))
        .semiApply()
        .|.expandAll("(resource)-[anon_2:DEPENDS_ON]->(anon_3)")
        .|.argument("resource")
        .nodeByLabelScan("resource", "Resource", IndexOrderNone)
        .build()
    )
  }

  test("should eagerize complex case of write-read-conflict with returned complete entity in UNION") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L0", 10)
      .build()

    val query =
      """OPTIONAL MATCH (var0:L0)
        |  WHERE id(var0) = 0
        |WITH var0
        |  WHERE var0 IS NULL
        |CREATE (var1:L0 {P0: 0})
        |WITH var1
        |SET var1 += $param1
        |RETURN var1
        |UNION
        |MATCH (var1:L0)
        |  WHERE id(var1) = 0 AND var1.P0 = $param2
        |SET var1.P0 = var1.P0 + 1
        |WITH var1
        |  WHERE var1.P0 = coalesce($param2, 0) + 1
        |SET var1 += $param1
        |RETURN var1""".stripMargin

    val plan = planner.plan(query)
    val expectedPlan = planner.planBuilder()
      .produceResults("var1")
      .distinct("var1 AS var1")
      .union()
      .|.projection("var1 AS var1")
      // IR eagerness interprets the conflict to be with the last projection
      .|.eager()
      .|.setNodePropertiesFromMap("var1", "$param1", removeOtherProps = false)
      .|.eager()
      .|.filter("cacheN[var1.P0] = coalesce($param2, 0) + 1")
      .|.eager()
      .|.setNodeProperty("var1", "P0", "var1.P0 + 1")
      .|.eager()
      .|.filter("var1:L0", "cacheNFromStore[var1.P0] = $param2")
      .|.nodeByIdSeek("var1", Set(), 0)
      .projection("var1 AS var1")
      // IR eagerness interprets the conflict to be with the last projection
      .eager()
      .setNodePropertiesFromMap("var1", "$param1", removeOtherProps = false)
      .create(createNodeWithProperties("var1", Seq("L0"), "{P0: 0}"))
      .filter("var0 IS NULL")
      .eager()
      .optional()
      .filter("var0:L0")
      .nodeByIdSeek("var0", Set(), 0)
      .build()
    plan should equal(expectedPlan)
  }

  test("should eagerize simple case of write-read-conflict with returned complete entity in UNION") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 20)
      .setRelationshipCardinality("(:L0)-[]->()", 8)
      .setRelationshipCardinality("()-[]->(:L0)", 8)
      .build()

    val query =
      """  CREATE (var1:L0 {P0: 0})-[:REL]->(:L0)
        |  RETURN var1
        |UNION
        |  MATCH (var1:L0)--(var0)
        |  SET var0 += $param2
        |  RETURN var1;""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("var1")
        .distinct("var1 AS var1")
        .union()
        .|.projection("var1 AS var1")
        .|.eager(Seq(Unknown))
        .|.setNodePropertiesFromMap("var0", "$param2", removeOtherProps = false)
        .|.expandAll("(var1)-[anon_2]-(var0)")
        .|.nodeByLabelScan("var1", "L0", IndexOrderNone)
        .projection("var1 AS var1")
        .create(
          Seq(
            createNodeWithProperties("var1", Seq("L0"), "{P0: 0}"),
            createNode("anon_1", "L0")
          ),
          Seq(
            createRelationship("anon_0", "var1", "REL", "anon_1", OUTGOING)
          )
        )
        .argument()
        .build()
    )
  }

  test("should eagerize subquery case of write-read-conflict with returned complete entity in UNION") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 20)
      .setRelationshipCardinality("(:L0)-[]->()", 8)
      .setRelationshipCardinality("()-[]->(:L0)", 8)
      .setRelationshipCardinality("(:L0)-[]->(:L0)", 8)
      .build()

    val query =
      """CALL {
        |    CREATE (var1:L0 {P0: 0})-[:REL]->(:L0)
        |    RETURN var1
        |  UNION
        |    MATCH (var1:L0)--(var0)
        |    SET var0 += $param2
        |    RETURN var1
        |}
        |RETURN var1""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("var1")
        .irEager()
        .apply(true)
        .|.distinct("var1 AS var1")
        .|.union()
        .|.|.projection("var1 AS var1")
        .|.|.setNodePropertiesFromMap("var0", "$param2", removeOtherProps = false)
        .|.|.expandAll("(var1)-[anon_2]-(var0)")
        .|.|.nodeByLabelScan("var1", "L0", IndexOrderNone)
        .|.projection("var1 AS var1")
        .|.create(
          Seq(
            createNodeWithProperties("var1", Seq("L0"), "{P0: 0}"),
            createNode("anon_1", "L0")
          ),
          Seq(
            createRelationship("anon_0", "var1", "REL", "anon_1", OUTGOING)
          )
        )
        .|.argument()
        .argument()
        .build()
    )
  }
}
