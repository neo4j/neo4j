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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

import scala.collection.immutable.ListSet

class EagerWhereNeededRewriterTest extends CypherFunSuite with LogicalPlanTestOps with AstConstructionTestSupport {

  // Negative tests

  test("inserts no eager in linear read query") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .limit(5)
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager in branched read query") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .limit(5)
      .apply()
      .|.filter("n.prop > 5")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager if a unary EagerLogicalPlan is already present where needed") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .sort(Seq(Ascending("n")))
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager if a binary EagerLogicalPlan is already present where needed. Conflict LHS with RHS.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .nodeHashJoin("n")
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager if a binary EagerLogicalPlan is already present where needed. Conflict LHS with Top.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .nodeHashJoin("n")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager if there is a conflict between LHS and RHS of a Union.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .union()
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("does not support if there is a conflict between LHS and RHS of an OrderedUnion.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .orderedUnion(Seq(Ascending("n")))
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    // Currently OrderedUnion is only planned for read-plans
    an[IllegalStateException] should be thrownBy {
      EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    }
  }

  test("does not support if there is a conflict between LHS and RHS of an AssertSameNode.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .assertSameNode("n")
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    // Currently AssertSameNode is only planned for read-plans
    an[IllegalStateException] should be thrownBy {
      EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    }
  }

  // Property Read/Set conflicts

  test("inserts eager between property set and property read of same property on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no eager between property set and property read of different property on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop2 AS foo")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between property set and property read of same property when read appears in second set property"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .setNodeProperty("n", "prop", "n.prop + 1")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test("insert eager for multiple properties set") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("nv1", "nv2")
      .projection("n.v1 AS nv1", "n.v2 AS nv2")
      .setNodeProperties("n", ("v1", "n.v1 + 1"), ("v2", "n.v2 + 1"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("nv1", "nv2")
        .projection("n.v1 AS nv1", "n.v2 AS nv2")
        .eager(ListSet(
          EagernessReason.PropertyReadSetConflict(propName("v2")),
          EagernessReason.PropertyReadSetConflict(propName("v1"))
        ))
        .setNodeProperties("n", ("v1", "n.v1 + 1"), ("v2", "n.v2 + 1"))
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts no eager between property set and property read (NodeIndexScan) if property read through stable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.allNodeScan("m")
      .nodeIndexOperator("n:N(prop)")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between property set and property read (NodeIndexScan) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop)")
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.nodeIndexOperator("n:N(prop)")
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between property set and all properties read") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("properties")
      .projection("properties(n) AS properties")
      .setNodeProperty("m", "prop", "42")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("properties")
        .projection("properties(n) AS properties")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .setNodeProperty("m", "prop", "42")
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager when setting concrete properties from map") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("p")
      .projection("m.prop AS p")
      .setNodePropertiesFromMap("m", "{prop: 42}", removeOtherProps = false)
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection("m.prop AS p")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .setNodePropertiesFromMap("m", "{prop: 42}", removeOtherProps = false)
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no eager when setting concrete different properties from map") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("p")
      .projection("m.prop AS p")
      .setNodePropertiesFromMap("m", "{prop2: 42}", removeOtherProps = false)
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager when setting concrete different properties from map but removing previous properties") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("p")
      .projection("m.prop AS p")
      .setNodePropertiesFromMap("m", "{prop2: 42}", removeOtherProps = true)
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection("m.prop AS p")
        .eager(ListSet(EagernessReason.UnknownPropertyReadSetConflict))
        .setNodePropertiesFromMap("m", "{prop2: 42}", removeOtherProps = true)
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager when setting unknown properties from map") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("p")
      .projection("m.prop AS p")
      .setNodePropertiesFromMap("m", "$param", removeOtherProps = false)
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection("m.prop AS p")
        .eager(ListSet(EagernessReason.UnknownPropertyReadSetConflict))
        .setNodePropertiesFromMap("m", "$param", removeOtherProps = false)
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  // Label Read/Set conflict

  test("inserts no eager between label set and label read if label read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setLabels("m", "N")
      .apply()
      .|.allNodeScan("m")
      .nodeByLabelScan("n", "N")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager between label set and label read (NodeByLabelScan) if label read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setLabels("m", "N")
      .apply()
      .|.nodeByLabelScan("n", "N")
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setLabels("m", "N")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("N"))))
        .apply()
        .|.nodeByLabelScan("n", "N")
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between label set and label read (NodeIndexScan) if label read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setLabels("m", "N")
      .apply()
      .|.nodeIndexOperator("n:N(prop)")
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setLabels("m", "N")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("N"))))
        .apply()
        .|.nodeIndexOperator("n:N(prop)")
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between label set and all labels read") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .setLabels("m", "A")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("labels")
        .projection("labels(n) AS labels")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("A"))))
        .setLabels("m", "A")
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between label set and negated label read") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("labels")
      .setLabels("m", "A")
      .expand("(n)-[r]->(m)")
      .cartesianProduct()
      .|.filter("n:!A")
      .|.allNodeScan("n")
      .argument()
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("labels")
        .setLabels("m", "A")
        .expand("(n)-[r]->(m)")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("A"))))
        .cartesianProduct()
        .|.filter("n:!A")
        .|.allNodeScan("n")
        .argument()
        .build()
    )
  }

  // Read vs Create conflicts

  test("inserts no eager between Create and AllNodeScan if read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager between Create and AllNodeScan if read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o"))
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o"))
        .eager(ListSet(EagernessReason.ReadCreateConflict))
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Create with label and AllNodeScan if read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O"))
        .eager(ListSet(EagernessReason.ReadCreateConflict))
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no eager between bare Create and NodeByLabelScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o"))
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager between Create and NodeByLabelScan if no label overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager between Create and NodeByLabelScan if label overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .cartesianProduct()
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts no eager between Create and NodeByLabelScan if no label overlap with ANDed labels, same label in Filter"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.filter("m:O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with ANDed labels"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O", "M"))
      .cartesianProduct()
      .|.filter("m:O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O", "M"))
        .eager(ListSet(
          EagernessReason.LabelReadSetConflict(labelName("O")),
          EagernessReason.LabelReadSetConflict(labelName("M"))
        ))
        .cartesianProduct()
        .|.filter("m:O")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts no eager between Create and NodeByLabelScan if no label overlap with ORed labels in union"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "P"))
      .cartesianProduct()
      .|.distinct("m AS m")
      .|.union()
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with ORed labels in union"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.distinct("m AS m")
      .|.union()
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("O"))))
        .cartesianProduct()
        .|.distinct("m AS m")
        .|.union()
        .|.|.nodeByLabelScan("m", "O")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts no eager between Create and NodeByLabelScan if no label overlap with ANDed labels in join"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.nodeHashJoin("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with ANDed labels in join"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O", "M"))
      .cartesianProduct()
      .|.nodeHashJoin("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O", "M"))
        .eager(ListSet(
          EagernessReason.LabelReadSetConflict(labelName("O")),
          EagernessReason.LabelReadSetConflict(labelName("M"))
        ))
        .cartesianProduct()
        .|.nodeHashJoin("m")
        .|.|.nodeByLabelScan("m", "O")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with ANDed labels in AssertSameNode"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O", "M"))
      .cartesianProduct()
      .|.assertSameNode("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O", "M"))
        .eager(ListSet(
          EagernessReason.LabelReadSetConflict(labelName("O")),
          EagernessReason.LabelReadSetConflict(labelName("M"))
        ))
        .cartesianProduct()
        .|.assertSameNode("m")
        .|.|.nodeByLabelScan("m", "O")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels of LHS of left outer join"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.leftOuterHashJoin("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .cartesianProduct()
        .|.leftOuterHashJoin("m")
        .|.|.nodeByLabelScan("m", "O")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels of RHS of right outer join"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.rightOuterHashJoin("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("O"))))
        .cartesianProduct()
        .|.rightOuterHashJoin("m")
        .|.|.nodeByLabelScan("m", "O")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels of RHS of left outer join"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.leftOuterHashJoin("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels of LHS of right outer join"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.rightOuterHashJoin("m")
      .|.|.nodeByLabelScan("m", "O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with optional labels"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.apply()
      .|.|.optional()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with non-optional labels"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.apply()
      .|.|.optional()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .cartesianProduct()
        .|.apply()
        .|.|.optional()
        .|.|.filter("m:O")
        .|.|.argument("m")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels from SemiApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.semiApply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels outside of SemiApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.semiApply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels from AntiSemiApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.antiSemiApply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
      .cartesianProduct()
      .|.antiSemiApply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
      .build())
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels outside of SelectOrSemiApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.selectOrSemiApply("true")
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .cartesianProduct()
        .|.selectOrSemiApply("true")
        .|.|.filter("m:O")
        .|.|.argument("m")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels for new node in SelectOrSemiApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .selectOrSemiApply("n.prop > 0")
      .|.nodeByLabelScan("m", "M", "n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .selectOrSemiApply("n.prop > 0")
        .|.nodeByLabelScan("m", "M", "n")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels from RollUpApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.rollUpApply("m", "ms")
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels outside of RollUpApply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.rollUpApply("m", "ms")
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
      .cartesianProduct()
      .|.rollUpApply("m", "ms")
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
      .build())
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels outside of SubqueryForeach"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.subqueryForeach()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
      .cartesianProduct()
      .|.subqueryForeach()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
      .build())
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap with labels in and outside of Apply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M", "O"))
      .cartesianProduct()
      .|.apply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M", "O"))
        .eager(ListSet(
          EagernessReason.LabelReadSetConflict(labelName("M")),
          EagernessReason.LabelReadSetConflict(labelName("O"))
        ))
        .cartesianProduct()
        .|.apply()
        .|.|.filter("m:O")
        .|.|.argument("m")
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels only outside of Apply"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.apply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts no eager between Create and NodeByLabelScan if no label overlap, but conflicting label in Filter after create"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .filter("m:O")
      .create(createNode("o", "O"))
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts no eager between Create and NodeByLabelScan if no label overlap with ANDed labels, same label in NodeByLabelScan"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.filter("m:O")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "inserts eager between Create and NodeByLabelScan if label overlap, and other label in Filter after create"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .filter("m:O")
      .create(createNode("o", "M"))
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .filter("m:O")
        .create(createNode("o", "M"))
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .cartesianProduct()
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Create and later NodeByLabelScan"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .apply()
      .|.nodeByLabelScan("m", "M")
      .create(createNode("o", "M"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .apply()
        .|.nodeByLabelScan("m", "M")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("M"))))
        .create(createNode("o", "M"))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no eager between bare Create and IndexScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts no eager between Create and IndexScan if no property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq.empty, "{foo: 5}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager between Create and IndexScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq.empty, "{prop: 5}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq.empty, "{prop: 5}"))
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Create and IndexScan if unknown property created") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq.empty, "$foo"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq.empty, "$foo"))
        .eager(ListSet(EagernessReason.UnknownPropertyReadSetConflict))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  // Read vs Merge conflicts

  test("inserts no eager between Merge and AllNodeScan if read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .merge(nodes = Seq(createNode("m")))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager between Merge and AllNodeScan if read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(nodes = Seq(createNode("m")))
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .merge(nodes = Seq(createNode("m")))
        .eager(ListSet(EagernessReason.ReadCreateConflict))
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no eager between Merge with ON MATCH and IndexScan if no property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(nodes = Seq(createNode("m")), onMatch = Seq(setNodeProperty("m", "foo", "42")))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test("inserts eager between Merge with ON MATCH and IndexScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(nodes = Seq(createNode("m")), onMatch = Seq(setNodeProperty("m", "prop", "42")))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .merge(nodes = Seq(createNode("m")), onMatch = Seq(setNodeProperty("m", "prop", "42")))
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Merge with ON MATCH and IndexScan if property overlap (setting multiple properties)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(nodes = Seq(createNode("m")), onMatch = Seq(setNodeProperties("m", ("prop", "42"), ("foo", "42"))))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .merge(nodes = Seq(createNode("m")), onMatch = Seq(setNodeProperties("m", ("prop", "42"), ("foo", "42"))))
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Merge with ON MATCH and IndexScan if property overlap (setting properties from map)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(
        nodes = Seq(createNode("m")),
        onMatch = Seq(setNodePropertiesFromMap("m", "{prop: 42}", removeOtherProps = false))
      )
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .merge(
          nodes = Seq(createNode("m")),
          onMatch = Seq(setNodePropertiesFromMap("m", "{prop: 42}", removeOtherProps = false))
        )
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Merge with ON MATCH and IndexScan if all property removed (setting properties from map)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(
        nodes = Seq(createNode("m")),
        onMatch = Seq(setNodePropertiesFromMap("m", "{foo: 42}", removeOtherProps = true))
      )
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .merge(
          nodes = Seq(createNode("m")),
          onMatch = Seq(setNodePropertiesFromMap("m", "{foo: 42}", removeOtherProps = true))
        )
        .eager(ListSet(EagernessReason.UnknownPropertyReadSetConflict))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Merge with ON CREATE and IndexScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .merge(nodes = Seq(createNode("m")), onCreate = Seq(setNodeProperty("m", "prop", "42")))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .merge(nodes = Seq(createNode("m")), onCreate = Seq(setNodeProperty("m", "prop", "42")))
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  // Insert Eager at best position

  test("inserts eager between conflicting plans at the cardinality minimum between the two plans") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(50)
      .projection("n.prop AS foo").withCardinality(50)
      .expand("(n)-->(m)").withCardinality(50)
      .filter("5 > 3").withCardinality(10) // Minimum of prop conflict
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .expand("(n)-->(m)")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (non-intersecting candidate lists)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(20)
      .projection("n.foo AS foo").withCardinality(20)
      .expand("(n)-->(o)").withCardinality(20) // Minimum of foo conflict
      .filter("5 > 3").withCardinality(40)
      .setNodeProperty("n", "foo", "5").withCardinality(50)
      .projection("n.prop AS prop").withCardinality(50)
      .expand("(n)-->(m)").withCardinality(50)
      .filter("5 > 3").withCardinality(10) // Minimum of prop conflict
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("foo"))))
        .expand("(n)-->(o)")
        .filter("5 > 3")
        .setNodeProperty("n", "foo", "5")
        .projection("n.prop AS prop")
        .expand("(n)-->(m)")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (one candidate list subset of the other, same minimum)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(25)
      .projection("n.prop AS prop").withCardinality(25)
      .projection("n.foo AS foo").withCardinality(25)
      .expand("(n)-->(o)").withCardinality(20) // Minimum of both conflicts
      .expand("(n)-->(m)").withCardinality(50)
      .setNodeProperty("n", "foo", "5").withCardinality(75)
      .filter("5 > 3").withCardinality(75)
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS prop")
        .projection("n.foo AS foo")
        .eager(ListSet(
          EagernessReason.PropertyReadSetConflict(propName("prop")),
          EagernessReason.PropertyReadSetConflict(propName("foo"))
        ))
        .expand("(n)-->(o)")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "foo", "5")
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (one candidate list subset of the other, different minima)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(25)
      .projection("n.prop AS prop").withCardinality(25)
      .projection("n.foo AS foo").withCardinality(25)
      .expand("(n)-->(o)").withCardinality(20) // Minimum of foo conflict
      .expand("(n)-->(m)").withCardinality(50)
      .setNodeProperty("n", "foo", "5").withCardinality(75)
      .filter("5 > 3").withCardinality(10) // Mininum of prop conflict
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS prop")
        .projection("n.foo AS foo")
        .eager(ListSet(
          EagernessReason.PropertyReadSetConflict(propName("prop")),
          EagernessReason.PropertyReadSetConflict(propName("foo"))
        ))
        .expand("(n)-->(o)")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "foo", "5")
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (overlapping candidate lists, same minimum)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(25)
      .projection("n.foo AS foo").withCardinality(100)
      .expand("(n)-->(p)").withCardinality(50)
      .projection("n.prop AS prop").withCardinality(50)
      .expand("(n)-->(o)").withCardinality(10) // Minimum of both conflicts
      .setNodeProperty("n", "foo", "5").withCardinality(50)
      .expand("(n)-->(m)").withCardinality(50)
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .eager(ListSet(
          EagernessReason.PropertyReadSetConflict(propName("prop")),
          EagernessReason.PropertyReadSetConflict(propName("foo"))
        ))
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (overlapping candidate lists, first minimum in intersection)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(25)
      .projection("n.foo AS foo").withCardinality(100)
      .expand("(n)-->(p)").withCardinality(5) // Minimum of foo conflict
      .projection("n.prop AS prop").withCardinality(50)
      .expand("(n)-->(o)").withCardinality(10) // Minimum of prop conflict (in intersection)
      .setNodeProperty("n", "foo", "5").withCardinality(50)
      .expand("(n)-->(m)").withCardinality(50)
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .eager(ListSet(
          EagernessReason.PropertyReadSetConflict(propName("prop")),
          EagernessReason.PropertyReadSetConflict(propName("foo"))
        ))
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (overlapping candidate lists, second minimum in intersection)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(25)
      .projection("n.foo AS foo").withCardinality(100)
      .expand("(n)-->(p)").withCardinality(15)
      .projection("n.prop AS prop").withCardinality(50)
      .expand("(n)-->(o)").withCardinality(10) // Minimum of foo conflict (in intersection)
      .setNodeProperty("n", "foo", "5").withCardinality(50)
      .expand("(n)-->(m)").withCardinality(5) // Minimum of prop conflict
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .eager(ListSet(
          EagernessReason.PropertyReadSetConflict(propName("prop")),
          EagernessReason.PropertyReadSetConflict(propName("foo"))
        ))
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between conflicting plans at the cardinality minima of two separate conflicts (overlapping candidate lists, different minima, none in intersection)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(100)
      .projection("n.foo AS foo").withCardinality(100)
      .expand("(n)-->(p)").withCardinality(5) // Minimum  of foo conflict
      .projection("n.prop AS prop").withCardinality(100)
      .expand("(n)-->(o)").withCardinality(8) // Minimum of intersection
      .setNodeProperty("n", "foo", "5").withCardinality(100)
      .expand("(n)-->(m)").withCardinality(5) // Minimum  of prop conflict
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("foo"))))
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  // Apply-Plans

  test(
    "inserts eager between property read on the LHS and property write on top of an Apply (cardinality lower after Apply)"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .apply().withCardinality(5)
      .|.argument("n")
      .projection("n.prop AS foo").withCardinality(10)
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.argument("n")
        .projection("n.prop AS foo")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property read on the LHS and property write on the RHS of an Apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .apply()
      .|.setNodeProperty("n", "prop", "5")
      .|.argument("n")
      .projection("n.prop AS foo")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .apply()
        .|.setNodeProperty("n", "prop", "5")
        .|.argument("n")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .projection("n.prop AS foo")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property read on the RHS and property write on top of an Apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .apply()
      .|.projection("n.prop AS foo")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.projection("n.prop AS foo")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property read on the RHS and property write on the RHS of an Apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .apply()
      .|.setNodeProperty("n", "prop", "5")
      .|.projection("n.prop AS foo")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .apply()
        .|.setNodeProperty("n", "prop", "5")
        .|.eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .|.projection("n.prop AS foo")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property read on the RHS of an Apply and property write on the RHS of another Apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .apply()
      .|.setNodeProperty("n", "prop", "5")
      .|.argument("n")
      .apply()
      .|.projection("n.prop AS foo")
      .|.argument("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .apply()
        .|.setNodeProperty("n", "prop", "5")
        .|.argument("n")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.projection("n.prop AS foo")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager for transactional apply for otherwise stable iterators") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .transactionApply()
      .|.setLabels("m", "A")
      .|.expand("(n)--(m)")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .transactionApply()
        .|.setLabels("m", "A")
        .|.expand("(n)--(m)")
        .|.argument("n")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("A"))))
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("insert eager when apply plan is conflicting with the outside") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .setLabels("n", "A")
      .selectOrSemiApply("a:A")
      .|.expand("(a)-[r]->(n)")
      .|.argument("a")
      .allNodeScan("a")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .setLabels("n", "A")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("A"))))
        .selectOrSemiApply("a:A")
        .|.expand("(a)-[r]->(n)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("insert eager when apply plan is conflicting with the LHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .selectOrSemiApply("a:A")
      .|.expand("(a)-[r]->(n)")
      .|.argument("a")
      .setLabels("a", "A")
      .allNodeScan("a")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .selectOrSemiApply("a:A")
        .|.expand("(a)-[r]->(n)")
        .|.argument("a")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("A"))))
        .setLabels("a", "A")
        .allNodeScan("a")
        .build()
    )
  }

  test("does not support when apply plan is conflicting with the RHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .selectOrSemiApply("a:A")
      .|.setLabels("n", "A")
      .|.expand("(a)-[r]->(n)")
      .|.argument("a")
      .allNodeScan("a")
    val plan = planBuilder.build()

    // Currently the RHS of any SemiApply variant must be read-only
    an[IllegalStateException] should be thrownBy {
      EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    }
  }

  // Non-apply binary plans

  test("inserts eager in LHS in a conflict between LHS and Top of a CartesianProduct") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("`count(*)`")
      .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
      .setLabels("a", "B")
      .cartesianProduct().withCardinality(2)
      .|.allNodeScan("b")
      .filter("a:B").withCardinality(1)
      .nodeByLabelScan("a", "A")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .setLabels("a", "B")
        .cartesianProduct()
        .|.allNodeScan("b")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("B"))))
        .filter("a:B")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("inserts eager on Top in a conflict between LHS and Top of a CartesianProduct") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("`count(*)`")
      .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
      .setLabels("a", "B")
      .cartesianProduct().withCardinality(1)
      .|.allNodeScan("b")
      .filter("a:B").withCardinality(2)
      .nodeByLabelScan("a", "A")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .setLabels("a", "B")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("B"))))
        .cartesianProduct()
        .|.allNodeScan("b")
        .filter("a:B")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("inserts eager on top in a conflict between RHS and Top of a CartesianProduct") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .cartesianProduct().withCardinality(10)
      .|.setNodeProperty("n", "prop", "5").withCardinality(1) // Eager must be on Top anyway
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .cartesianProduct()
        .|.setNodeProperty("n", "prop", "5")
        .|.allNodeScan("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager in a conflict between LHS and RHS of a CartesianProduct") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .cartesianProduct()
      .|.projection("m.prop AS foo")
      .|.allNodeScan("m")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .cartesianProduct()
        .|.projection("m.prop AS foo")
        .|.allNodeScan("m")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test("eagerize nested cartesian products") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .setLabels("n", "Two")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("m2", "Two")
      .|.nodeByLabelScan("m1", "Two")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .setLabels("n", "Two")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("Two"))))
        .cartesianProduct()
        .|.cartesianProduct()
        .|.|.nodeByLabelScan("m2", "Two")
        .|.nodeByLabelScan("m1", "Two")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager in RHS in a conflict between RHS and Top of a join") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .nodeHashJoin("n").withCardinality(2)
      .|.setNodeProperty("n", "prop", "5").withCardinality(1)
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .nodeHashJoin("n")
        .|.eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .|.setNodeProperty("n", "prop", "5")
        .|.allNodeScan("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager on top in a conflict between RHS and Top of a join") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .nodeHashJoin("n").withCardinality(1)
      .|.setNodeProperty("n", "prop", "5").withCardinality(2)
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .nodeHashJoin("n")
        .|.setNodeProperty("n", "prop", "5")
        .|.allNodeScan("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts Eager if there is a conflict between LHS and Top of an AssertSameNode.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .assertSameNode("n")
      .|.allNodeScan("n")
      .projection("n.prop AS foo")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .assertSameNode("n")
        .|.allNodeScan("n")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .projection("n.prop AS foo")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts Eager on Top if there is a conflict between RHS and Top of an AssertSameNode.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .assertSameNode("n").withCardinality(10)
      .|.projection("n.prop AS foo").withCardinality(1)
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .assertSameNode("n")
        .|.projection("n.prop AS foo")
        .|.allNodeScan("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts Eager if there is a conflict between LHS and Top of a Union.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .union()
      .|.allNodeScan("n")
      .projection("n.prop AS foo")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.allNodeScan("n")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .projection("n.prop AS foo")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts Eager if there is a conflict between RHS and Top of a Union.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .union()
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .|.projection("n.prop AS foo")
        .|.allNodeScan("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts Eager if there are two conflict in a Union plan: LHS vs Top and RHS vs Top.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .union().withCardinality(10)
      .|.projection("n.prop AS foo2").withCardinality(5)
      .|.allNodeScan("n").withCardinality(5)
      .projection("n.prop AS foo").withCardinality(5)
      .allNodeScan("n").withCardinality(5)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .|.projection("n.prop AS foo2")
        .|.allNodeScan("n")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .projection("n.prop AS foo")
        .allNodeScan("n")
        .build()
    )
  }

test(
    "should not insert Eager if two different created nodes in the same operator have together the labels from a NodeByLabelScan"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.filter("c:B")
      .|.nodeByLabelScan("c", "A")
      .create(createNode("a", "A"), createNode("b", "B"))
      .argument()

    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  test(
    "should insert Eager if two different created nodes in the same operator overlap with a FilterExpression"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.filter("c:!B")
      .|.nodeByLabelScan("c", "A")
      .create(createNode("a", "A"), createNode("b", "B"))
      .argument()

    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.filter("c:!B")
      .|.nodeByLabelScan("c", "A")
      .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("A"))))
      .create(createNode("a", "A"), createNode("b", "B"))
      .argument()
      .build()
    )
  }

  test(
    "inserts Eager if there are two conflict in a Union plan: LHS vs Top and RHS vs Top (LHS and RHS are identical plans)."
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .union().withCardinality(10)
      .|.projection("n.prop AS foo").withCardinality(5)
      .|.allNodeScan("n").withCardinality(5)
      .projection("n.prop AS foo").withCardinality(5)
      .allNodeScan("n").withCardinality(5)
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .|.projection("n.prop AS foo")
        .|.allNodeScan("n")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .projection("n.prop AS foo")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Should only reference the conflicting label when there is an eagerness conflict between a write within a forEach and a read after"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.nodeByLabelScan("d", "Event")
      .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event"), createNode("p", "Place")), Seq(createRelationship("i", "e", "IN", "p"))), setNodeProperty("e", "foo", "'e_bar'")))
      .argument()

    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("Event"))))
        .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event"), createNode("p", "Place")), Seq(createRelationship("i", "e", "IN", "p"))), setNodeProperty("e", "foo", "'e_bar'")))
        .argument()
        .build()
    )
  }

  test(
    "Should insert an eager when there is a conflict between a write within a forEach and a read after"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.nodeByLabelScan("d", "Event")
      .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event")))))
      .argument()

    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("Event"))))
        .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event")))))
        .argument()
        .build()
    )
  }


  test(
    "inserts eager between property set and property read (NodeIndexSeekByRange) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop>5)")
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.nodeIndexOperator("n:N(prop > 5)")
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts eager between property set and property read (NodeIndexSeek) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop=5)")
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.nodeIndexOperator("n:N(prop = 5)")
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts eager between property set and property read (NodeIndexContainsScan) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop CONTAINS '1')", indexType = IndexType.TEXT)
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.nodeIndexOperator("n:N(prop CONTAINS '1')", indexType = IndexType.TEXT)
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts eager between property set and property read (NodeIndexEndsWithScan) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop ENDS WITH '1')", indexType = IndexType.TEXT)
      .allNodeScan("m")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(EagernessReason.PropertyReadSetConflict(propName("prop"))))
        .apply()
        .|.nodeIndexOperator("n:N(prop ENDS WITH '1')", indexType = IndexType.TEXT)
        .allNodeScan("m")
        .build()
    )
  }


  // Ignored tests

  //Todo: Update LabelExpressionEvaluator to return a boolean or a set of the conflicting Labels
  ignore(
    "Should only reference the conflicting labels when there is a write of multiple labels in the same create pattern"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.nodeByLabelScan("d", "Event")
      .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event", "Place")))))
      .argument()

    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(EagernessReason.LabelReadSetConflict(labelName("Event"))))
        .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event", "Place")))))
        .argument()
        .build()
    )
  }

  // No analysis for possible overlaps of node variables based on predicates yet.
  ignore(
    "does not insert eager between label set and all labels read if no overlap possible by property predicate means"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .setLabels("m", "A")
      .filter("m.prop = 4")
      .expand("(n)-[r]->(m)")
      .filter("n.prop = 5")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }

  // This is not very important to fix, the kind of query that yields this plan is quite unrealistic.
  ignore(
    "does not insert eager between Create and NodeByLabelScan if label overlap with labels from AntiSemiApply and from NodeByLabelScan"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M", "O"))
      .cartesianProduct()
      .|.antiSemiApply()
      .|.|.filter("m:O")
      .|.|.argument("m")
      .|.nodeByLabelScan("m", "M")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = EagerWhereNeededRewriter(planBuilder.cardinalities, Attributes(planBuilder.idGen)).eagerize(plan)
    result should equal(plan)
  }
}
