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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Head
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.Summarized
import org.neo4j.cypher.internal.ir.EagernessReason.SummaryEntry
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownPropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UpdateStrategyEager
import org.neo4j.cypher.internal.ir.EagernessReason.WriteAfterCallInTransactions
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setPropertyFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.StorableType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class EagerWhereNeededRewriterTest extends CypherFunSuite with LogicalPlanTestOps with AstConstructionTestSupport {

  /**
   * Get a builder for incomplete logical plans with an ID offset.
   * This can be used to build plans for nested plan expressions.
   *
   * The ID offset helps to avoid having ID conflicts in the outer plan that
   * contains the nested plan.
   */
  private def subPlanBuilderWithIdOffset(): LogicalPlanBuilder =
    new LogicalPlanBuilder(wholePlan = false, initialId = 100)

  private def eagerizePlan(
    planBuilder: LogicalPlanBuilder,
    plan: LogicalPlan,
    shouldCompressReasons: Boolean = false
  ): LogicalPlan =
    EagerWhereNeededRewriter(
      planBuilder.cardinalities,
      Attributes(planBuilder.idGen),
      shouldCompressReasons
    ).eagerize(
      plan,
      planBuilder.getSemanticTable,
      new AnonymousVariableNameGenerator
    )

  // Negative tests

  test("inserts no eager in linear read query") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .limit(5)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager if a unary EagerLogicalPlan is already present where needed") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .sort("n ASC")
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("does not support if there is a conflict between LHS and RHS of an OrderedUnion.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .orderedUnion("n ASC")
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .unwind("[1,2] AS x")
      .allNodeScan("n")
    val plan = planBuilder.build()

    // Currently OrderedUnion is only planned for read-plans
    an[IllegalStateException] should be thrownBy {
      EagerWhereNeededRewriter(
        planBuilder.cardinalities,
        Attributes(planBuilder.idGen),
        shouldCompressReasons = false
      ).eagerize(
        plan,
        planBuilder.getSemanticTable,
        new AnonymousVariableNameGenerator
      )
    }
  }

  test("does not support if there is a conflict between LHS and RHS of an AssertSameNode.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .assertSameNode("n")
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .setNodeProperty("n", "prop", "5")
      .unwind("[1,2] AS x")
      .allNodeScan("n")
    val plan = planBuilder.build()

    // Currently AssertSameNode is only planned for read-plans
    an[IllegalStateException] should be thrownBy {
      EagerWhereNeededRewriter(
        planBuilder.cardinalities,
        Attributes(planBuilder.idGen),
        shouldCompressReasons = false
      ).eagerize(
        plan,
        planBuilder.getSemanticTable,
        new AnonymousVariableNameGenerator
      )
    }
  }

  // Property Read/Set conflicts

  test("inserts eager between property set and property read of same property on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .projection("n.prop AS foo")
      .setNodeProperty("n", "prop", "5")
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(1)))))
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager between property set and property read of same property when read appears in second set property"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .setNodeProperty("n", "prop", "5")
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("n", "prop", "n.prop + 1")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(2)))))
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
        .allNodeScan("n")
        .build()
    )
  }

  test("insert eager for multiple properties set") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("nv1", "nv2")
      .projection("n.v1 AS nv1", "n.v2 AS nv2")
      .setNodeProperties("n", ("v1", "n.v1 + 1"), ("v2", "n.v2 + 1"))
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("nv1", "nv2")
        .projection("n.v1 AS nv1", "n.v2 AS nv2")
        .eager(ListSet(
          PropertyReadSetConflict(propName("v2")).withConflict(Conflict(Id(2), Id(1))),
          PropertyReadSetConflict(propName("v1")).withConflict(Conflict(Id(2), Id(1)))
        ))
        .setNodeProperties("n", ("v1", "n.v1 + 1"), ("v2", "n.v2 + 1"))
        .unwind("[1,2] AS x")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts no eager between property set and property read (NodeIndexScan) if property read through stable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.allNodeScan("m")
      .nodeIndexOperator("n:N(prop)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager between property set and property read (NodeIndexScan) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop)")
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("properties")
        .projection("properties(n) AS properties")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(1)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection("m.prop AS p")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(1)))))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection("m.prop AS p")
        .eager(ListSet(UnknownPropertyReadSetConflict.withConflict(Conflict(Id(2), Id(1)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("p")
        .projection("m.prop AS p")
        .eager(ListSet(UnknownPropertyReadSetConflict.withConflict(Conflict(Id(2), Id(1)))))
        .setNodePropertiesFromMap("m", "$param", removeOtherProps = false)
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property set and property read in nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .filter("m.prop > 0")
      .expand("(n)-[r]->(m)")
      .argument("n")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"EXISTS { MATCH (n)-[r]->(m) WHERE m.prop > 0 }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.foo AS x")
      .filterExpression(nestedPlanExpression)
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.foo AS x")
        .filterExpression(nestedPlanExpression)
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(2)))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property set and all properties read in nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .projection("properties(m) AS properties")
      .expand("(n)-[r]->(m)")
      .argument("n")
      .build()

    val nestedPlanExpression = NestedPlanCollectExpression(
      nestedPlan,
      v"properties",
      s"COLLECT { MATCH (n)-[r]->(m) RETURN properties(m) AS properties }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("ps")
      .projection(Map("ps" -> nestedPlanExpression))
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("ps")
        .projection(Map("ps" -> nestedPlanExpression))
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(1)))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between property set and property read in projection of nested plan collect expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .expand("(n)-[r]->(m)")
      .argument("n")
      .build()

    val nestedPlanExpression = NestedPlanCollectExpression(
      nestedPlan,
      prop("m", "prop"),
      s"[(n)-[r]->(m) | m.prop]"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("mProps")
      .projection(Map("mProps" -> nestedPlanExpression))
      .setNodeProperty("n", "prop", "5")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("mProps")
        .projection(Map("mProps" -> nestedPlanExpression))
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(1)))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  // Label Read/Set conflict

  test("inserts no eager between label set and label read if label read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.allNodeScan("m")
      .nodeByLabelScan("n", "N")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between label set and NodeByIdSeek with label filter if read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply().withCardinality(100)
      .|.allNodeScan("m")
      .filter("n:N").withCardinality(10)
      .nodeByIdSeek("n", Set.empty, 1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("m", "N")
        .apply()
        .|.allNodeScan("m")
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(5)))))
        .filter("n:N")
        .nodeByIdSeek("n", Set.empty, 1)
        .build()
    )
  }

  test("inserts eager between label remove and NodeByIdSeek with label filter if read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .removeLabels("m", "N")
      .apply().withCardinality(100)
      .|.allNodeScan("m")
      .filter("n:N").withCardinality(10)
      .nodeByIdSeek("n", Set.empty, 1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .removeLabels("m", "N")
        .apply()
        .|.allNodeScan("m")
        .eager(ListSet(LabelReadRemoveConflict(labelName("N")).withConflict(Conflict(Id(2), Id(5)))))
        .filter("n:N")
        .nodeByIdSeek("n", Set.empty, 1)
        .build()
    )
  }

  test("inserts eager between label set and label read (NodeByLabelScan) if label read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.nodeByLabelScan("n", "N")
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("m", "N")
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.nodeByLabelScan("n", "N")
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts eager between label set and label read (UnionNodeByLabelScan) if label read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.unionNodeByLabelsScan("n", Seq("N", "M"))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("m", "N")
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.unionNodeByLabelsScan("n", Seq("N", "M"))
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between label set and label read (NodeIndexScan) if label read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.nodeIndexOperator("n:N(prop)")
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("m", "N")
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(4)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("labels")
        .projection("labels(n) AS labels")
        .eager(ListSet(LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(2), Id(1)))))
        .setLabels("m", "A")
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between label set and negated label read") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .setLabels("m", "A")
      .expand("(n)-[r]->(m)").withCardinality(100)
      .cartesianProduct().withCardinality(10)
      .|.filter("n:!A")
      .|.allNodeScan("n")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .setLabels("m", "A")
        .expand("(n)-[r]->(m)")
        .eager(ListSet(LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(1), Id(4)))))
        .cartesianProduct()
        .|.filter("n:!A")
        .|.allNodeScan("n")
        .argument()
        .build()
    )
  }

  test(
    "inserts eager between label set and label read (NodeCountFromCountStore) if label read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.nodeCountFromCountStore("count", Seq(Some("N")))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("m", "N")
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.nodeCountFromCountStore("count", Seq(Some("N")))
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts eager between label set and label read (RelationshipCountFromCountStore) if label read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.relationshipCountFromCountStore("count", Some("N"), Seq("REL"), None)
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("m", "N")
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.relationshipCountFromCountStore("count", Some("N"), Seq("REL"), None)
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts no eager between label set and label read (NodeCountFromCountStore) if label read through stable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("m", "N")
      .apply()
      .|.allNodeScan("m")
      .nodeCountFromCountStore("count", Seq(Some("N")))
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between label set and label read in nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .filter("m:N")
      .expand("(n)-[r]->(m)")
      .argument("n")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"EXISTS { MATCH (n)-[r]->(m:N) }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.foo AS x")
      .filterExpression(nestedPlanExpression)
      .setLabels("n", "N")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.foo AS x")
        .filterExpression(nestedPlanExpression)
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(3), Id(2)))))
        .setLabels("n", "N")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between label set and all labels read in nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .projection("labels(m) AS labels")
      .expand("(n)-[r]->(m)")
      .argument("n")
      .build()

    val nestedPlanExpression = NestedPlanCollectExpression(
      nestedPlan,
      v"labels",
      s"COLLECT { MATCH (n)-[r]->(m) RETURN labels(m) AS labels }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("lbs")
      .projection(Map("lbs" -> nestedPlanExpression))
      .setLabels("n", "N")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("lbs")
        .projection(Map("lbs" -> nestedPlanExpression))
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(2), Id(1)))))
        .setLabels("n", "N")
        .allNodeScan("n")
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

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between Create and Create") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o"))
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between Create and Create if there is nested plan expression somewhere else") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .aggregation(Seq(), Seq("count(*) AS count"))
      .allNodeScan("m")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"COUNT { MATCH (m) }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o"))
      .create(createNode("m"))
      .projection(Map("c" -> nestedPlanExpression))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createNode("o"))
        .create(createNode("m"))
        .eager(ListSet(
          ReadCreateConflict.withConflict(Conflict(Id(1), Id(3))),
          ReadCreateConflict.withConflict(Conflict(Id(2), Id(3)))
        ))
        .projection(Map("c" -> nestedPlanExpression))
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no Eager between Create and Create with subsequent filters") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .filter("m.prop > 0")
      .filter("o.prop > 0")
      .create(createNode("o"))
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between Create and Create in Foreach") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .foreach("i", "[1]", Seq(createPattern(Seq(createNode("o")))))
      .create(createNode("m"))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager between label create and label read (Filter) after stable AllNodeScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N", "O"))
      .filter("n:N")
      .filter("n:O")
      .unwind("n.prop AS prop")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager between create and stable AllNodeScan + Projection") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m"))
      .projection("n AS n2")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o"))
        .eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(1), Id(3)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O"))
        .eager(ListSet(LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(3)))))
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between Create and All nodes read (NodeCountFromCountStore) if read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "O"))
      .apply()
      .|.nodeCountFromCountStore("count", Seq(None))
      .nodeByLabelScan("m", "M")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createNode("o", "O"))
        .eager(ListSet(LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.nodeCountFromCountStore("count", Seq(None))
        .nodeByLabelScan("m", "M")
        .build()
    )
  }

  test(
    "inserts eager between Create and All relationships read (RelationshipCountFromCountStore) if read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createRelationship("r", "m", "REL", "m"))
      .apply()
      .|.relationshipCountFromCountStore("count", Some("N"), Seq("REL"), None)
      .nodeByLabelScan("m", "M")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createRelationship("r", "m", "REL", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("REL")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.relationshipCountFromCountStore("count", Some("N"), Seq("REL"), None)
        .nodeByLabelScan("m", "M")
        .build()
    )
  }

  test(
    "inserts no eager between Create and All nodes read (NodeCountFromCountStore) if read through stable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "O"))
      .apply()
      .|.nodeByLabelScan("m", "M")
      .nodeCountFromCountStore("count", Seq(None))
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager between Create and label read (NodeCountFromCountStore) if label read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "N"))
      .apply()
      .|.nodeCountFromCountStore("count", Seq(Some("N")))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createNode("o", "N"))
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.nodeCountFromCountStore("count", Seq(Some("N")))
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts eager between create and type read (RelationshipCountFromCountStore) if label read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .create(createRelationship("r", "m", "REL", "m"))
      .apply()
      .|.relationshipCountFromCountStore("count", None, Seq("REL"), None)
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .create(createRelationship("r", "m", "REL", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("REL")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.relationshipCountFromCountStore("count", None, Seq("REL"), None)
        .allNodeScan("m")
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))))
        .cartesianProduct()
        .|.nodeByLabelScan("m", "M")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts no eager between Create and UnionNodeByLabelScan if no label overlap"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "O"))
      .apply()
      .|.unionNodeByLabelsScan("n", Seq("N", "M"))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager between Create and UnionNodeByLabelScan if label overlap"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "M"))
      .apply()
      .|.unionNodeByLabelsScan("n", Seq("N", "M"))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createNode("o", "M"))
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.unionNodeByLabelsScan("n", Seq("N", "M"))
        .allNodeScan("m")
        .build()
    )
  }

  test(
    "inserts no eager between Create and IntersectionNodeByLabelScan if no label overlap"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "O"))
      .apply()
      .|.unionNodeByLabelsScan("n", Seq("N", "M"))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager between Create and IntersectionNodeByLabelScan if label overlap"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o", "M", "N"))
      .apply()
      .|.intersectionNodeByLabelsScan("n", Seq("N", "M"))
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createNode("o", "M", "N"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.intersectionNodeByLabelsScan("n", Seq("N", "M"))
        .allNodeScan("m")
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O", "M"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(4)))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(5))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(6)))
        ))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O", "M"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(5))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(5)))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O", "M"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(5))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(5)))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(5)))
        ))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "O"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(4))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(5)))
        ))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(7)))))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(6)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(6)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M"))
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(6)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M"))
      .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(6)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M", "O"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(6))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(6)))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .filter("m:O")
        .create(createNode("o", "M"))
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(2), Id(4)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .apply()
        .|.nodeByLabelScan("m", "M")
        .eager(ListSet(LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(3), Id(2)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager between Create and IndexScan if no property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "{foo: 5}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between Create and IndexScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "{prop: 5}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq("M"), "{prop: 5}"))
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Create and IndexScan if unknown property created") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "$foo"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq("M"), "$foo"))
        .eager(ListSet(
          UnknownPropertyReadSetConflict.withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Create and NodeByIdSeek (single ID) with label filter if read through stable iterator") {
    // This plan does actually not need to be Eager.
    // But since we only eagerize a single row, we accept that the analysis is imperfect here.
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .filter("n:N")
      .nodeByIdSeek("n", Set.empty, 1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(3)))))
      .filter("n:N")
      .nodeByIdSeek("n", Set.empty, 1)
      .build())
  }

  test(
    "inserts eager between Create and NodeByIdSeek (multiple IDs) with label filter if read through stable iterator"
  ) {
    // This plan looks like we would not need Eagerness, but actually the IDs 2 and 3 do not need to exist yet
    // and the newly created node could get one of these IDs.
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .filter("n:N")
      .nodeByIdSeek("n", Set.empty, 1, 2, 3)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(3)))))
      .filter("n:N")
      .nodeByIdSeek("n", Set.empty, 1, 2, 3)
      .build())
  }

  test(
    "inserts eager between Create and NodeByElementIdSeek (single ID) with label filter if read through stable iterator"
  ) {
    // This plan does actually not need to be Eager.
    // But since we only eagerize a single row, we accept that the analysis is imperfect here.
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .filter("n:N")
      .nodeByElementIdSeek("n", Set.empty, 1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(3)))))
      .filter("n:N")
      .nodeByElementIdSeek("n", Set.empty, 1)
      .build())
  }

  test(
    "inserts eager between Create and NodeByElementIdSeek (multiple IDs) with label filter if read through stable iterator"
  ) {
    // This plan looks like we would not need Eagerness, but actually the IDs 2 and 3 do not need to exist yet
    // and the newly created node could get one of these IDs.
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .filter("n:N")
      .nodeByElementIdSeek("n", Set.empty, 1, 2, 3)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N"))
      .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(3)))))
      .filter("n:N")
      .nodeByElementIdSeek("n", Set.empty, 1, 2, 3)
      .build())
  }

  test(
    "inserts no eager between Create and MATCH with a different label but same property, when other operand is a variable"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .filter("m.prop = x")
      .apply()
      .|.nodeByLabelScan("m", "M")
      .create(createNodeWithProperties("n", Seq("N"), "{prop: 5}"))
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between create and label read in nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .filter("m:N")
      .expand("(n)-[r]->(m)")
      .argument("n")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"EXISTS { MATCH (n)-[r]->(m:N) }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.foo AS x")
      .filterExpression(nestedPlanExpression)
      .create(createNode("n", "N"))
      .unwind("[1,2] AS x")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.foo AS x")
        .filterExpression(nestedPlanExpression)
        .eager(ListSet(LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(3), Id(2)))))
        .create(createNode("n", "N"))
        .unwind("[1,2] AS x")
        .argument()
        .build()
    )
  }

  test("inserts eager between create and all nodes read in nested plan expression") {
    // UNWIND [1,2] AS x
    // CREATE (n)
    // SET n.p = COUNT { MATCH (m) }
    // RETURN n.p AS x

    val nestedPlan = subPlanBuilderWithIdOffset()
      .aggregation(Seq(), Seq("count(*) AS count"))
      .allNodeScan("m")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"COUNT { MATCH (m) }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.p AS x")
      .setNodeProperty("n", "p", nestedPlanExpression)
      .create(createNode("n"))
      .unwind("[1,2] AS x")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.p AS x")
        .eager(ListSet(PropertyReadSetConflict(propName("p")).withConflict(Conflict(Id(2), Id(1)))))
        .setNodeProperty("n", "p", nestedPlanExpression)
        .eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(3), Id(2)))))
        .create(createNode("n"))
        .unwind("[1,2] AS x")
        .argument()
        .build()
    )
  }

  // Read vs Merge conflicts

  test("inserts no eager between Merge and AllNodeScan if read through stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .merge(nodes = Seq(createNode("n")))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager between Merge and and its child plans") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .merge(nodes = Seq(createNode("n", "N")))
      .filter("n", "N")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between Merge and AllNodeScan if read through unstable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .apply()
      .|.merge(nodes = Seq(createNode("o")))
      .|.allNodeScan("o")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .apply()
        .|.merge(nodes = Seq(createNode("o")))
        .|.allNodeScan("o")
        .eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(2), Id(5)))))
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts no eager between Merge with ON MATCH and IndexScan if no property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .merge(nodes = Seq(createNode("m")), onMatch = Seq(setNodeProperty("m", "foo", "42")))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager between Merge with ON MATCH and IndexScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.merge(nodes = Seq(createNode("o")), onMatch = Seq(setNodeProperty("o", "prop", "42")))
      .|.allNodeScan("o")
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.merge(nodes = Seq(createNode("o")), onMatch = Seq(setNodeProperty("o", "prop", "42")))
        .|.allNodeScan("o")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(6)))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Merge with ON MATCH and IndexScan if property overlap (setting multiple properties)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.merge(nodes = Seq(createNode("o")), onMatch = Seq(setNodeProperties("o", ("prop", "42"), ("foo", "42"))))
      .|.allNodeScan("o")
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.merge(nodes = Seq(createNode("o")), onMatch = Seq(setNodeProperties("o", ("prop", "42"), ("foo", "42"))))
        .|.allNodeScan("o")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(6)))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Merge with ON MATCH and IndexScan if property overlap (setting properties from map)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.merge(
        nodes = Seq(createNode("o")),
        onMatch = Seq(setNodePropertiesFromMap("o", "{prop: 42}", removeOtherProps = false))
      )
      .|.allNodeScan("o")
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.merge(
          nodes = Seq(createNode("o")),
          onMatch = Seq(setNodePropertiesFromMap("o", "{prop: 42}", removeOtherProps = false))
        )
        .|.allNodeScan("o")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(6)))))
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
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.merge(
        nodes = Seq(createNode("o")),
        onMatch = Seq(setNodePropertiesFromMap("o", "{foo: 42}", removeOtherProps = true))
      )
      .|.allNodeScan("o")
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.merge(
          nodes = Seq(createNode("o")),
          onMatch = Seq(setNodePropertiesFromMap("o", "{foo: 42}", removeOtherProps = true))
        )
        .|.allNodeScan("o")
        .eager(ListSet(UnknownPropertyReadSetConflict.withConflict(Conflict(Id(3), Id(6)))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between Merge with ON CREATE and IndexScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.merge(nodes = Seq(createNode("o", "M")), onCreate = Seq(setNodeProperty("o", "prop", "42")))
      .|.nodeByLabelScan("o", "M")
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.merge(nodes = Seq(createNode("o", "M")), onCreate = Seq(setNodeProperty("o", "prop", "42")))
        .|.nodeByLabelScan("o", "M")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(6)))))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should not insert an eager when the property conflict of a merge is on a stable iterator") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .merge(Seq(createNodeWithProperties("n", Seq(), "{foo: 5}")))
      .filter("n.foo = 5")
      .allNodeScan("n")

    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  // Insert Eager at best position

  test("inserts eager between conflicting plans at the cardinality minimum between the two plans") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo").withCardinality(50)
      .projection("n.prop AS foo").withCardinality(50)
      .expand("(n)-->(m)").withCardinality(50)
      .filter("5 > 3").withCardinality(10) // Minimum of prop conflict
      .setNodeProperty("n", "prop", "5").withCardinality(100)
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .expand("(n)-->(m)")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(4), Id(1)))))
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .eager(ListSet(PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(4), Id(1)))))
        .expand("(n)-->(o)")
        .filter("5 > 3")
        .setNodeProperty("n", "foo", "5")
        .projection("n.prop AS prop")
        .expand("(n)-->(m)")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(8), Id(5)))))
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS prop")
        .projection("n.foo AS foo")
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(1))),
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(5), Id(2)))
        ))
        .expand("(n)-->(o)")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "foo", "5")
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS prop")
        .projection("n.foo AS foo")
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(1))),
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(5), Id(2)))
        ))
        .expand("(n)-->(o)")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "foo", "5")
        .filter("5 > 3")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .eager(ListSet(
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(5), Id(1))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(3)))
        ))
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .eager(ListSet(
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(5), Id(1))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(3)))
        ))
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .eager(ListSet(
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(5), Id(1))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(3)))
        ))
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
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
      .unwind("[1,2] AS x").newVar("x", CTInteger).withCardinality(100)
      .allNodeScan("n").withCardinality(50)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.foo AS foo")
        .eager(ListSet(PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(5), Id(1)))))
        .expand("(n)-->(p)")
        .projection("n.prop AS prop")
        .expand("(n)-->(o)")
        .setNodeProperty("n", "foo", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(3)))))
        .expand("(n)-->(m)")
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2] AS x")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts eager between label create and label read (Filter) directly after AllNodeScan if cheapest") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("m", "N", "O"))
      .filter("n:N").withCardinality(800)
      .filter("n:O").withCardinality(900)
      .unwind("n.prop AS prop").withCardinality(1000)
      .apply().withCardinality(10)
      .|.allNodeScan("n").withCardinality(10)
      .argument().withCardinality(1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .create(createNode("m", "N", "O"))
        .filter("n:N")
        .filter("n:O")
        .unwind("n.prop AS prop")
        .eager(ListSet(
          LabelReadSetConflict(labelName("N")).withConflict(Conflict(Id(1), Id(6))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(6)))
        ))
        .apply()
        .|.allNodeScan("n")
        .argument()
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
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(4)))))
        .apply()
        .|.argument("n")
        .projection("n.prop AS foo")
        .unwind("[1,2,3] AS x")
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
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .apply()
        .|.setNodeProperty("n", "prop", "5")
        .|.argument("n")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
        .projection("n.prop AS foo")
        .unwind("[1,2,3] AS x")
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .apply()
        .|.setNodeProperty("n", "prop", "5")
        .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .apply()
        .|.setNodeProperty("n", "prop", "5")
        .|.argument("n")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(5)))))
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .transactionApply()
        .|.setLabels("m", "A")
        .|.expand("(n)--(m)")
        .|.argument("n")
        .eager(ListSet(LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(3), Id(6)))))
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("inserts no eager for NodeByLabelScan -> DETACH DELETE same node in transactions") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.detachDeleteNode("n")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager for NodeByLabelScan -> Remove label on same node in transactions") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.removeLabels("n", "A")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager for NodeIndexScan -> Update property on same node in transactions") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      // Incrementing n and committing the transaction would mean that n gets a new position in the n:A(prop) index.
      // The index cursor scanning the index could now potentially find n again. That must not happen.
      // Thus we need eager.
      .|.setProperty("n", "prop", "n.prop + 1000")
      .|.argument("n")
      .nodeIndexOperator("n:A(prop)", indexOrder = IndexOrderAscending)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.setProperty("n", "prop", "n.prop + 1000")
      .|.argument("n")
      .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(5)))))
      .nodeIndexOperator("n:A(prop)", indexOrder = IndexOrderAscending)
      .build())
  }

  test(
    "inserts no eager for NodeByLabelScan -> Distinct Property Filter -> Update property on same node in transactions"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.setProperty("n", "prop", "n.prop - 1000")
      .|.argument("n")
      .filter("n.prop > 0")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts no eager for NodeByLabelScan -> Distinct Property Filter -> Expand -> Update property on same node in transactions"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.setProperty("n", "prop", "n.prop - m.prop2")
      .|.argument("n", "m")
      .expand("(n)-[r]->(m)")
      .filter("n.prop > 0")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager for NodeByLabelScan -> Expand -> Property Filter -> Update property on same node in transactions"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.setProperty("n", "prop", "n.prop - m.prop2")
      .|.argument("n", "m")
      .filter("n.prop > 0")
      .expand("(n)-[r]->(m)")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.setProperty("n", "prop", "n.prop - m.prop2")
      .|.argument("n", "m")
      .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(5)))))
      .filter("n.prop > 0")
      .expand("(n)-[r]->(m)")
      .nodeByLabelScan("n", "A")
      .build())
  }

  test("inserts no eager for Distinct Read -> DETACH DELETE same node in transactions") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.detachDeleteNode("n")
      .|.argument("n")
      .distinct("n AS n")
      .sort("one ASC") // Eager boundary to solve conflicts with plans below
      .projection("1 AS one").newVar("one", CTInteger)
      .expand("(n)-[r]->(m)")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager for NodeByLabelScan -> Unwind -> Filter -> DETACH DELETE same node in transactions") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("one")
      .transactionApply()
      .|.projection("1 as one")
      .|.detachDeleteNode("n")
      .|.argument("n")
      .filter("n.prop > 0")
      .unwind("[1,2,3] AS i").newVar("i", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("one")
        .transactionApply()
        .|.projection("1 as one")
        .|.detachDeleteNode("n")
        .|.argument("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(5)))))
        .filter("n.prop > 0")
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("inserts no eager for Distinct Property Read -> Set property on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("np")
      .setNodeProperty("n", "p", "1")
      .projection("n.p AS np")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager for Distinct Label Read -> Set Label on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .setLabels("n", "N")
      .filter("n:!N")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts no eager for Set Label -> Distinct Label Read on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n") // reads n's labels
      .setLabels("n", "N")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("inserts eager for Set Property -> Distinct property Read on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n") // reads n's properties
      .distinct("n AS n")
      // mp is ordered, last one should "win" and get written to n.p.
      // Not being Eager would mean we would return n nodes with a wrong n.p property.
      .setNodeProperty("n", "p", "mp")
      .sort("mp ASC")
      .projection("m.p AS mp")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("n")
      .eager(ListSet(PropertyReadSetConflict(propName("p")).withConflict(Conflict(Id(2), Id(0)))))
      .distinct("n AS n")
      .setNodeProperty("n", "p", "mp")
      .sort("mp ASC")
      .projection("m.p AS mp")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
      .build())
  }

  test("inserts no eager for Distinct Set Property -> property Read on same node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n") // reads n's properties
      // n is distinct when setting the property.
      // Each n gets only written once. So when reading later in the plan,
      // we must be reading the correct updated value.
      .setNodeProperty("n", "p", "4")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .setLabels("n", "A")
        .eager(ListSet(LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(1), Id(2)))))
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
      .unwind("[1,2,3] AS i")
      .setLabels("a", "A")
      .allNodeScan("a")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .selectOrSemiApply("a:A")
        .|.expand("(a)-[r]->(n)")
        .|.argument("a")
        .eager(ListSet(LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(5), Id(1)))))
        .unwind("[1,2,3] AS i")
        .setLabels("a", "A")
        .allNodeScan("a")
        .build()
    )
  }

  test("does not support when SingleFromRightLogicalPlan is conflicting with the RHS") {
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
      EagerWhereNeededRewriter(
        planBuilder.cardinalities,
        Attributes(planBuilder.idGen),
        shouldCompressReasons = false
      ).eagerize(
        plan,
        planBuilder.getSemanticTable,
        new AnonymousVariableNameGenerator
      )
    }
  }

  test("does not support when RollUpApply has writes on the RHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .rollUpApply("as", "a")
      .|.setLabels("n", "A")
      .|.expand("(a)-[r]->(n)")
      .|.argument("a")
      .allNodeScan("a")
    val plan = planBuilder.build()

    // Currently the RHS of any RollUpApply variant must be read-only
    an[IllegalStateException] should be thrownBy {
      EagerWhereNeededRewriter(
        planBuilder.cardinalities,
        Attributes(planBuilder.idGen),
        shouldCompressReasons = false
      ).eagerize(
        plan,
        planBuilder.getSemanticTable,
        new AnonymousVariableNameGenerator
      )
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
      .unwind("[1,2,3] AS i")
      .nodeByLabelScan("a", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .setLabels("a", "B")
        .cartesianProduct()
        .|.allNodeScan("b")
        .eager(ListSet(LabelReadSetConflict(labelName("B")).withConflict(Conflict(Id(2), Id(5)))))
        .filter("a:B")
        .unwind("[1,2,3] AS i")
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
      .unwind("[1,2,3] AS i")
      .nodeByLabelScan("a", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .setLabels("a", "B")
        .eager(ListSet(LabelReadSetConflict(labelName("B")).withConflict(Conflict(Id(2), Id(5)))))
        .cartesianProduct()
        .|.allNodeScan("b")
        .filter("a:B")
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("inserts eager on top in a conflict between RHS and Top of a CartesianProduct") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .cartesianProduct().withCardinality(10)
      .|.projection("n.prop AS foo").withCardinality(1) // Eager must be on Top anyway
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))))
        .cartesianProduct()
        .|.projection("n.prop AS foo")
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .cartesianProduct()
        .|.projection("m.prop AS foo")
        .|.allNodeScan("m")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(4), Id(2)))))
        .setNodeProperty("n", "prop", "5")
        .allNodeScan("n")
        .build()
    )
  }

  test("eagerize nested cartesian products") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("n", "Two")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("m2", "Two")
      .|.nodeByLabelScan("m1", "Two")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("n", "Two")
        .eager(ListSet(
          LabelReadSetConflict(labelName("Two")).withConflict(Conflict(Id(2), Id(5))),
          LabelReadSetConflict(labelName("Two")).withConflict(Conflict(Id(2), Id(6)))
        ))
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
      .|.unwind("[1,2] AS x")
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .nodeHashJoin("n")
        .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(1)))))
        .|.setNodeProperty("n", "prop", "5")
        .|.unwind("[1,2] AS x")
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
      .|.unwind("[1,2] AS x").newVar("x", CTInteger)
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .projection("n.prop AS foo")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(1)))))
        .nodeHashJoin("n")
        .|.setNodeProperty("n", "prop", "5")
        .|.unwind("[1,2] AS x")
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
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(4)))))
        .assertSameNode("n")
        .|.allNodeScan("n")
        .projection("n.prop AS foo")
        .unwind("[1,2,3] AS x")
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))))
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
      .union().withCardinality(20)
      .|.allNodeScan("n")
      .projection("n.prop AS foo").withCardinality(10)
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.allNodeScan("n")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(4)))))
        .projection("n.prop AS foo")
        .unwind("[1,2,3] AS x")
        .allNodeScan("n")
        .build()
    )
  }

  test("inserts Eager if there is a conflict between RHS and Top of a Union.") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .union().withCardinality(20)
      .|.projection("n.prop AS foo").withCardinality(10)
      .|.allNodeScan("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))))
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
      .|.unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .|.allNodeScan("n").withCardinality(5)
      .projection("n.prop AS foo").withCardinality(5)
      .unwind("[1,2,3] AS x")
      .allNodeScan("n").withCardinality(5)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))))
        .|.projection("n.prop AS foo2")
        .|.unwind("[1,2,3] AS x")
        .|.allNodeScan("n")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(6)))))
        .projection("n.prop AS foo")
        .unwind("[1,2,3] AS x")
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.filter("c:!B")
      .|.nodeByLabelScan("c", "A")
      .eager(ListSet(LabelReadSetConflict(labelName("A")).withConflict(Conflict(Id(4), Id(3)))))
      .create(createNode("a", "A"), createNode("b", "B"))
      .argument()
      .build())
  }

  test(
    "inserts Eager if there are two conflicts in a Union plan: LHS vs Top and RHS vs Top (LHS and RHS are identical plans)."
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .setNodeProperty("n", "prop", "5")
      .union().withCardinality(10)
      .|.projection("n.prop AS foo").withCardinality(5)
      .|.unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .|.allNodeScan("n").withCardinality(5)
      .projection("n.prop AS foo").withCardinality(5)
      .unwind("[1,2,3] AS x")
      .allNodeScan("n").withCardinality(5)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .setNodeProperty("n", "prop", "5")
        .union()
        .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))))
        .|.projection("n.prop AS foo")
        .|.unwind("[1,2,3] AS x")
        .|.allNodeScan("n")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(6)))))
        .projection("n.prop AS foo")
        .unwind("[1,2,3] AS x")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts Eager if there is a conflict in a Union plan: RHS vs Top (LHS and RHS are identical plans with a filter)."
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("foo")
      .create(createNodeWithProperties("m", Seq.empty, "{prop: 5}"))
      .union().withCardinality(8)
      .|.filter("n.prop > 0").withCardinality(4)
      .|.allNodeScan("n").withCardinality(5)
      .filter("n.prop > 0").withCardinality(4)
      .allNodeScan("n").withCardinality(5)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("foo")
        .create(createNodeWithProperties("m", Seq(), "{prop: 5}"))
        .union()
        .|.eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(1), Id(4)))))
        .|.filter("n.prop > 0")
        .|.allNodeScan("n")
        .filter("n.prop > 0")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Should only reference the conflicting label when there is an eagerness conflict between a write within a forEach and a read after"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.nodeByLabelScan("d", "Event")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setNodeProperty("e", "foo", "'e_bar'")
        )
      )
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3)))))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setNodeProperty("e", "foo", "'e_bar'")
          )
        )
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(3), Id(2)))))
        .foreach("x", "[1]", Seq(createPattern(Seq(createNode("e", "Event")))))
        .argument()
        .build()
    )
  }

  test(
    "Should insert an eager when there is a conflict between a Remove Label within a forEach and a read after"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.nodeByLabelScan("d", "Event")
      .foreach("x", "[1]", Seq(removeLabel("e", "Event")))
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(
          LabelReadRemoveConflict(labelName("Event")).withConflict(Conflict(Id(3), Id(0))),
          LabelReadRemoveConflict(labelName("Event")).withConflict(Conflict(Id(3), Id(2)))
        ))
        .foreach("x", "[1]", Seq(removeLabel("e", "Event")))
        .argument()
        .build()
    )
  }

  test(
    "Should insert an eager when there is a conflict between a write within a forEachApply and a read after"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.nodeIndexOperator("d:D(foo>0)")
      .foreachApply("num", "[1]")
      .|.create(createNodeWithProperties("n", Seq("D"), "{foo: num}"))
      .|.argument("num")
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeIndexOperator("d:D(foo>0)")
        .eager(ListSet(
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(4), Id(2))),
          LabelReadSetConflict(labelName("D")).withConflict(Conflict(Id(4), Id(2)))
        ))
        .foreachApply("num", "[1]")
        .|.create(createNodeWithProperties("n", Seq("D"), "{foo: num}"))
        .|.argument("num")
        .argument()
        .build()
    )
  }

  test(
    "Should not insert an eager when there is no conflict between a write within a forEachApply and a read after"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("d")
      .apply()
      .|.nodeIndexOperator("d:D(bar>0)")
      .foreachApply("num", "[1]")
      .|.create(createNodeWithProperties("n", Seq(), "{foo: num}"))
      .|.argument("num")
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "inserts eager between property set and property read (NodeIndexSeekByRange) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop>5)")
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
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
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop=5)")
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.nodeIndexOperator("n:N(prop = 5)")
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between Create and NodeIndexSeek if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "{prop: 5}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop>0)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq("M"), "{prop: 5}"))
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop>0)")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between property set and property read (NodeUniqueIndexSeek) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop=5)", unique = true)
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.nodeIndexOperator("n:N(prop = 5)", unique = true)
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between Create and NodeUniqueIndexSeek if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "{prop: 5}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop>0)", unique = true)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq("M"), "{prop: 5}"))
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop>0)", unique = true)
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between property set and property read (NodeIndexContainsScan) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop CONTAINS '1')", indexType = IndexType.TEXT)
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.nodeIndexOperator("n:N(prop CONTAINS '1')", indexType = IndexType.TEXT)
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between Create and NodeIndexContainsScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "{prop: '1'}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop CONTAINS '1')")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq("M"), "{prop: '1'}"))
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop CONTAINS '1')")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "inserts eager between property set and property read (NodeIndexEndsWithScan) if property read through unstable iterator"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setNodeProperty("m", "prop", "5")
      .apply()
      .|.nodeIndexOperator("n:N(prop ENDS WITH '1')", indexType = IndexType.TEXT)
      .allNodeScan("m")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setNodeProperty("m", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.nodeIndexOperator("n:N(prop ENDS WITH '1')", indexType = IndexType.TEXT)
        .allNodeScan("m")
        .build()
    )
  }

  test("inserts eager between Create and NodeIndexEndsWithScan if property overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("o", Seq("M"), "{prop: '1'}"))
      .cartesianProduct()
      .|.nodeIndexOperator("m:M(prop ENDS WITH '1')")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNodeWithProperties("o", Seq("M"), "{prop: '1'}"))
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3))),
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .cartesianProduct()
        .|.nodeIndexOperator("m:M(prop ENDS WITH '1')")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Conflicts in when traversing the right hand side of a plan should be found and eagerized."
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("p", "B")
      .apply()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("p", "C")
      .|.nodeByLabelScan("o", "B")
      .create(createNode("m", "C"))
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setLabels("p", "B")
        .eager(ListSet(LabelReadSetConflict(labelName("B")).withConflict(Conflict(Id(2), Id(6)))))
        .apply()
        .|.cartesianProduct()
        .|.|.nodeByLabelScan("p", "C")
        .|.nodeByLabelScan("o", "B")
        .eager(ListSet(LabelReadSetConflict(labelName("C")).withConflict(Conflict(Id(7), Id(5)))))
        .create(createNode("m", "C"))
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test(
    "should be eager between conflicts found inside cartesian product"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .apply()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("m", "Label")
      .|.nodeByLabelScan("n", "Label")
      .create(createNode("l", "Label"))
      .unwind("[1, 2] AS y")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("m")
        .apply()
        .|.cartesianProduct()
        .|.|.nodeByLabelScan("m", "Label")
        .|.nodeByLabelScan("n", "Label")
        .eager(ListSet(
          LabelReadSetConflict(labelName("Label")).withConflict(Conflict(Id(5), Id(3))),
          LabelReadSetConflict(labelName("Label")).withConflict(Conflict(Id(5), Id(4)))
        ))
        .create(createNode("l", "Label"))
        .unwind("[1, 2] AS y")
        .argument()
        .build()
    )
  }

  // DELETE Tests

  test("Should not be eager between two deletes") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .deleteNode("m")
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .nodeByLabelScan("n", "N")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("n")
        .deleteNode("m")
        .eager(ListSet(
          ReadDeleteConflict("m").withConflict(Conflict(Id(3), Id(5))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(5)))
        ))
        .cartesianProduct()
        .|.nodeByLabelScan("m", "M")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("Should be eager if deleted node is unstable") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .apply()
      .|.allNodeScan("n")
      .unwind("[0,1] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.allNodeScan("n")
        .unwind("[0,1] AS i")
        .argument()
        .build()
    )
  }

  test(
    "Should be eager if deleted node is unstable, and also protect projection after DELETE that will crash"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .projection("n.prop AS p")
      .deleteNode("n")
      .apply()
      .|.allNodeScan("n")
      .unwind("[0,1] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .projection("n.prop AS p")
        // If this Eager was missing, the projection might accidentally succeed, changing the result of the query
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(2)))))
        .deleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(3), Id(5)))))
        .apply()
        .|.allNodeScan("n")
        .unwind("[0,1] AS i")
        .argument()
        .build()
    )
  }

  test("Should not be eager if deleted node is stable") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("Should be eager if deleted node conflicts with unstable node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .apply()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("n")
        .eager(ListSet(ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager if deleted node conflicts with unstable node from nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .aggregation(Seq(), Seq("count(*) AS count"))
      .allNodeScan("m")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"COUNT { MATCH (m) }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .create(CreateNode(v"x", Set.empty, Some(mapOf("p" -> nestedPlanExpression))))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("x").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3)))
        ))
        .create(CreateNode(v"x", Set.empty, Some(mapOf("p" -> nestedPlanExpression))))
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager if deleted node (DeleteNode with expression) conflicts with unstable node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode(Head(listOf(v"n"))(pos))
      .apply()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode(Head(listOf(v"n"))(pos))
        .eager(ListSet(ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager if deleted node (DeleteExpression) conflicts with unstable node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteExpression(Head(listOf(v"n"))(pos))
      .apply()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteExpression(Head(listOf(v"n"))(pos))
        .eager(ListSet(ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager if deleted node (DetachDeleteNode with expression) conflicts with unstable node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("head(n)")
      .apply()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("head(n)")
        .eager(ListSet(ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager if deleted node (DetachDeleteExpression) conflicts with unstable node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteExpression("head([n])")
      .apply()
      .|.allNodeScan("m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteExpression("head([n])")
        .eager(ListSet(ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4)))))
        .apply()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager if deleted node conflicts with node that is introduced in Expand") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3)))
        ))
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Eager for DELETE conflict must be placed after the last predicate on matched node, even if higher cardinality"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n").withCardinality(30)
      .filter("m:M").withCardinality(30) // Filter can crash if executed on deleted node.
      .unwind("[1,2,3] AS i").withCardinality(60).newVar("i", CTInteger)
      .expand("(n)-[r]->(m)").withCardinality(20)
      .allNodeScan("n").withCardinality(10)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .filter("m:M")
        .unwind("[1,2,3] AS i")
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Eager for DELETE conflict must be placed after the last predicate on matched relationship, even if higher cardinality"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteRelationship("r").withCardinality(30)
      .filter("r:REL").withCardinality(30) // Filter can crash if executed on deleted relationship.
      .unwind("[1,2,3] AS j").withCardinality(45)
      .apply().withCardinality(15)
      .|.allRelationshipsScan("(n)-[r]->(m)").withCardinality(5)
      .unwind("[1,2,3] AS i").withCardinality(3)
      .argument().withCardinality(1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteRelationship("r")
        .eager(ListSet(
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(6)))
        ))
        .filter("r:REL") // Filter can crash if executed on deleted relationship.
        .unwind("[1,2,3] AS j")
        .apply()
        .|.allRelationshipsScan("(n)-[r]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test(
    "Eager for DELETE conflict must be placed after the last predicate on matched relationship (with confused semantic table), even if higher cardinality"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteRelationship("r").withCardinality(30)
      .filter("r:REL").withCardinality(30) // Filter can crash if executed on deleted relationship.
      .unwind("[1,2,3] AS j").withCardinality(45)
      .apply().withCardinality(15)
      .|.allRelationshipsScan("(n)-[r]->(m)").withCardinality(5)
      .newVar("r", CTAny.covariant) // Lie about the type of r
      .unwind("[1,2,3] AS i").withCardinality(3)
      .argument().withCardinality(1)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteRelationship("r")
        .eager(ListSet(
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(6)))
        ))
        .filter("r:REL") // Filter can crash if executed on deleted relationship.
        .unwind("[1,2,3] AS j")
        .apply()
        .|.allRelationshipsScan("(n)-[r]->(m)").newVar("r", CTAny.covariant)
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test(
    "Eager for DELETE conflict must be placed after the last reference to matched node, even if higher cardinality"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n").withCardinality(60)
      .projection("m.prop AS prop").withCardinality(60) // Property projection can crash if executed on deleted node.
      .unwind("[1,2,3] AS i").withCardinality(60).newVar("i", CTInteger)
      .expand("(n)-[r]->(m)").withCardinality(20)
      .allNodeScan("n").withCardinality(10)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .projection("m.prop AS prop")
        .unwind("[1,2,3] AS i")
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .apply()
      .|.nodeByLabelScan("a", "A")
      .deleteNode("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .apply()
        .|.nodeByLabelScan("a", "A")
        .eager(ListSet(ReadDeleteConflict("a").withConflict(Conflict(Id(4), Id(3)))))
        .deleteNode("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict, place eager before plan that introduces read node") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .filter("a.prop2 > 0").withCardinality(3)
      .filter("a.prop > 0").withCardinality(5)
      .apply().withCardinality(100)
      .|.nodeByLabelScan("a", "A").withCardinality(100)
      .deleteNode("n").withCardinality(10)
      .nodeByLabelScan("n", "A").withCardinality(10)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .filter("a.prop2 > 0")
        .filter("a.prop > 0")
        .apply()
        .|.nodeByLabelScan("a", "A")
        .eager(ListSet(ReadDeleteConflict("a").withConflict(Conflict(Id(6), Id(5)))))
        .deleteNode("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict, if not overlapping but no predicates on read node leaf plan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .filter("a:!A")
      .apply()
      .|.allNodeScan("a")
      .deleteNode("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .filter("a:!A")
        .apply()
        .|.allNodeScan("a")
        .eager(ListSet(ReadDeleteConflict("a").withConflict(Conflict(Id(5), Id(4)))))
        .deleteNode("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

// MATCH (n:N) UNION MATCH (n:N) DELETE n return count(*) as count
  test("Should be eager if deleted node conflicts with unstable node (identical plan to stable node)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .union().withCardinality(20)
      .|.nodeByLabelScan("n", "N").withCardinality(10)
      .nodeByLabelScan("n", "N")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("n")
        .union()
        .|.eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4)))))
        .|.nodeByLabelScan("n", "N")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("Should be eager if deleted node conflicts with unstable node in nested plan expression") {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .expandInto("(n)-[r]->(m)")
      .allNodeScan("m", "n")
      .build()

    val nestedPlanExpression = NestedPlanExistsExpression(
      nestedPlan,
      s"EXISTS { MATCH (n)-[r]->(m) }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("n")
      .filterExpression(nestedPlanExpression)
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("n")
        .eager(ListSet(ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3)))))
        .filterExpression(nestedPlanExpression)
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Eager for DELETE conflict must be placed after the last reference to matched node, in nested plan expression"
  ) {
    val nestedPlan = subPlanBuilderWithIdOffset()
      .projection("m.prop AS prop") // Property projection can crash if executed on deleted node.
      .argument("m")
      .build()

    val prop = v"prop"
    val nestedPlanExpression = NestedPlanCollectExpression(
      nestedPlan,
      prop,
      s"COLLECT { MATCH (m) RETURN m.prop AS prop }"
    )(pos)

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n").withCardinality(40)
      .projection(Map("props" -> nestedPlanExpression)).withCardinality(60)
      .unwind("[1,2,3] AS i").withCardinality(60).newVar("i", CTInteger)
      .expand("(n)-[r]->(m)").withCardinality(20)
      .allNodeScan("n").withCardinality(10)

    // This makes sure we do not mistake prop for a potential node
    planBuilder.newVariable(prop, StorableType.storableType)
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .projection(Map("props" -> nestedPlanExpression)).withCardinality(60)
        .unwind("[1,2,3] AS i")
        .expand("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Create") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .create(createRelationship("r", "n", "R", "n"))
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3)))))
        .create(createRelationship("r", "n", "R", "n"))
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Foreach - Create") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .foreach("x", "[1]", Seq(createPattern(Seq.empty, Seq(createRelationship("r", "n", "R", "n")))))
      .newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3)))))
        .foreach("x", "[1]", Seq(createPattern(Seq.empty, Seq(createRelationship("r", "n", "R", "n")))))
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Foreach - Remove label") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .foreach("x", "[1]", Seq(removeLabel("n", "N"))).newVar("x", CTInteger)
      .unwind("[1,2,3] AS i").newVar("i", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .foreach("x", "[1]", Seq(removeLabel("n", "N")))
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Foreach - Set label") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .foreach("x", "[1]", Seq(setLabel("n", "N"))).newVar("x", CTInteger)
      .unwind("[1,2,3] AS i").newVar("i", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .foreach("x", "[1]", Seq(setLabel("n", "N")))
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Foreach - Set node properties") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .foreach("x", "[1]", Seq(setNodeProperties("n", "prop" -> "5"))).newVar("x", CTInteger)
      .unwind("[1,2,3] AS i").newVar("i", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .foreach("x", "[1]", Seq(setNodeProperties("n", "prop" -> "5")))
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Foreach - Set node properties from map") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .foreach("x", "[1]", Seq(setNodePropertiesFromMap("n", "{prop: 5}", removeOtherProps = false)))
      .newVar("x", CTInteger)
      .unwind("[1,2,3] AS i").newVar("i", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .foreach("x", "[1]", Seq(setNodePropertiesFromMap("n", "{prop: 5}", removeOtherProps = false)))
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Foreach - Set node property") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .foreach("x", "[1]", Seq(setNodeProperty("n", "prop", "5"))).newVar("x", CTInteger)
      .unwind("[1,2,3] AS i").newVar("i", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .foreach("x", "[1]", Seq(setNodeProperty("n", "prop", "5")))
        .unwind("[1,2,3] AS i")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Merge") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      .|.merge(Seq.empty, Seq(createRelationship("r", "n", "R", "n")))
      .|.expandInto("(n)-[r:R]->(n)")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.merge(Seq.empty, Seq(createRelationship("r", "n", "R", "n")))
        .|.expandInto("(n)-[r:R]->(n)")
        .|.argument("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Merge - ON MATCH") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      .|.merge(
        Seq.empty,
        Seq(createRelationship("r", "n", "R", "n")),
        onMatch = Seq(setNodeProperty("n", "prop", "5"))
      )
      .|.expandInto("(n)-[r:R]->(n)")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.merge(
          Seq.empty,
          Seq(createRelationship("r", "n", "R", "n")),
          onMatch = Seq(setNodeProperty("n", "prop", "5"))
        )
        .|.expandInto("(n)-[r:R]->(n)")
        .|.argument("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Merge - ON CREATE") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      .|.merge(
        Seq.empty,
        Seq(createRelationship("r", "n", "R", "n")),
        onCreate = Seq(setNodeProperty("n", "prop", "5"))
      )
      .|.expandInto("(n)-[r:R]->(n)")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.merge(
          Seq.empty,
          Seq(createRelationship("r", "n", "R", "n")),
          onCreate = Seq(setNodeProperty("n", "prop", "5"))
        )
        .|.expandInto("(n)-[r:R]->(n)")
        .|.argument("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in RemoveLabels") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .removeLabels("n", "N")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .removeLabels("n", "N")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in SetLabels") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .setLabels("n", "N")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .setLabels("n", "N")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in SetNodeProperties") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .setNodeProperties("n", "prop" -> "5")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .setNodeProperties("n", "prop" -> "5")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in SetNodePropertiesFromMap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .setNodePropertiesFromMap("n", "{prop: 5}", removeOtherProps = false)
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .setNodePropertiesFromMap("n", "{prop: 5}", removeOtherProps = false)
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in SetNodeProperty") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .setNodeProperty("n", "prop", "5")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .setNodeProperty("n", "prop", "5")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("inserts no Eager between Delete and Create") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createNode("o"))
      .deleteNode("n")
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between Create and Delete") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .deleteNode("n")
      .create(createNode("o"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  // Relationship Tests

  // Reads
  test("Should be eager in set/read conflict with SetRelationshipProperty") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("r.prop AS prop")
      .setRelationshipProperty("r", "prop", "5")
      .apply()
      .|.allRelationshipsScan("(n)-[r]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("prop")
        .projection("r.prop AS prop")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(1)))))
        .setRelationshipProperty("r", "prop", "5")
        .apply()
        .|.allRelationshipsScan("(n)-[r]->(m)")
        .unwind("[1, 2, 3] AS i")
        .argument()
        .build()
    )
  }

  // CREATE Relationship
  test(
    "inserts eager between Create and RelationshipTypeScan if Type overlap, and other type in Filter after create"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .filter("r:O")
      .create(createRelationship("o", "q", "R", "z"))
      .cartesianProduct()
      .|.relationshipTypeScan("(n)-[r:R]->(m)")
      .allNodeScan("a")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .filter("r:O")
        .create(createRelationship("o", "q", "R", "z"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(2), Id(4)))))
        .cartesianProduct()
        .|.relationshipTypeScan("(n)-[r:R]->(m)")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedAllRelationshipsScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m"))
      .apply()
      .|.allRelationshipsScan("(n)-[r]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.allRelationshipsScan("(n)-[r]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedAllRelationshipsScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m"))
      .apply()
      .|.allRelationshipsScan("(n)-[r]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.allRelationshipsScan("(n)-[r]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipsTypeScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m"))
      .apply()
      .|.relationshipTypeScan("(n)-[r:R]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.relationshipTypeScan("(n)-[r:R]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be not eager in read/create conflict with DirectedRelationshipsTypeScan where types does not overlap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R2", "m"))
      .apply()
      .|.relationshipTypeScan("(n)-[r:R1]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipsTypeScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m"))
      .apply()
      .|.relationshipTypeScan("(n)-[r:R]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.relationshipTypeScan("(n)-[r:R]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test(
    "Should be not eager in read/create conflict with UndirectedRelationshipsTypeScan where types does not overlap"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R2", "m"))
      .apply()
      .|.relationshipTypeScan("(n)-[r:R1]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("Should be eager in read/create conflict with DirectedRelationshipIndexSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop>5)]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop>5)]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipIndexSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop>5)]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop>5)]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipIndexEndsWithScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop ENDS WITH 'foo')]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop ENDS WITH 'foo')]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipIndexEndsWithScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop ENDS WITH 'foo')]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop ENDS WITH 'foo')]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipIndexContainsScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop CONTAINS 'foo')]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop CONTAINS 'foo')]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipIndexContainsScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop CONTAINS 'foo')]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop CONTAINS 'foo')]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipUniqueIndexSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop>5)]->(m)", unique = true)
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop>5)]->(m)", unique = true)
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipUniqueIndexSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop>5)]-(m)", unique = true)
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop>5)]-(m)", unique = true)
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipIndexScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop)]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop)]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipIndexScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3))),
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipIdSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.directedRelationshipByIdSeek("r1", "x", "y", Set(), 23, 22.0, -1)
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.directedRelationshipByIdSeek("r1", "x", "y", Set(), 23, 22.0, -1)
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipIdSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.undirectedRelationshipByIdSeek("r1", "x", "y", Set(), 23, 22.0, -1)
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.undirectedRelationshipByIdSeek("r1", "x", "y", Set(), 23, 22.0, -1)
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedRelationshipByElementIdSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.directedRelationshipByElementIdSeek("r1", "x", "y", Set(), "23", "22.0", "-1")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.directedRelationshipByElementIdSeek("r1", "x", "y", Set(), "23", "22.0", "-1")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedRelationshipByElementIdSeek") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
      .apply()
      .|.undirectedRelationshipByElementIdSeek("r1", "x", "y", Set(), "23", "22.0", "-1")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R", "m", properties = Some("{prop: 1}")))
        .eager(ListSet(
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .apply()
        .|.undirectedRelationshipByElementIdSeek("r1", "x", "y", Set(), "23", "22.0", "-1")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with UndirectedUnionRelationshipTypesScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .apply()
      .|.unionRelationshipTypesScan("(n)-[r:R1|R2]-(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.unionRelationshipTypesScan("(n)-[r:R1|R2]-(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with DirectedUnionRelationshipTypesScan") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .apply()
      .|.unionRelationshipTypesScan("(n)-[r:R1|R2]->(m)")
      .unwind("[1,2,3] AS i")
      .argument()
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(3)))))
        .apply()
        .|.unionRelationshipTypesScan("(n)-[r:R1|R2]->(m)")
        .unwind("[1,2,3] AS i")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/create conflict with Expand") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .expand("(n)-[r:R1|R2]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .expand("(n)-[r:R1|R2]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should not be eager in read/create conflict with Expand with no overlapping types") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .expand("(n)-[r:R2|R3]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .expand("(n)-[r:R2|R3]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with Optional Expand") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .optionalExpandInto("(n)-[r:R1|R2]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .optionalExpandInto("(n)-[r:R1|R2]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should not be eager in read/create conflict with Optional Expand  with no overlapping types") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .optionalExpandInto("(n)-[r:R2|R3]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .optionalExpandInto("(n)-[r:R2|R3]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with VarExpand") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .expand("(n)-[r:R1|R2*1..2]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .expand("(n)-[r:R1|R2*1..2]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with PruningVarExpand") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .pruningVarExpand("(n)-[:R1|R2*1..2]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .pruningVarExpand("(n)-[r:R1|R2*1..2]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with BFSPruningVarExpand") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .bfsPruningVarExpand("(n)-[:R1|R2*1..2]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .bfsPruningVarExpand("(n)-[r:R1|R2*1..2]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with PathPropagatingBFS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .pathPropagatingBFS("(n)-[:R1|R2*1..2]->(m)")
      .|.argument("n", "m")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .pathPropagatingBFS("(n)-[:R1|R2*1..2]->(m)")
        .|.argument("n", "m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should not be eager in read/create conflict with VarExpand with no overlapping types") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .expand("(n)-[r:R2|R3*1..2]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .expand("(n)-[r:R2|R3*1..2]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with ShortestPath") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .create(createRelationship("r", "n", "R1", "m"))
      .shortestPath("(n)-[r]->(m)")
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .shortestPath("(n)-[r]->(m)")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with returning a full relationship entity") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .setRelationshipProperty("r", "prop", "5")
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(0)))))
        .setRelationshipProperty("r", "prop", "5")
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a property filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .setRelationshipProperty("r", "prop", "5")
      .filter("r.prop = 0")
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .setRelationshipProperty("r", "prop", "5")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(2)))))
        .filter("r.prop = 0")
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a getDegree filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createRelationship("r", "n", "R1", "m"))
      .filterExpression(getDegree(v"n", SemanticDirection.OUTGOING))
      .allNodeScan("n")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .filterExpression(getDegree(v"n", SemanticDirection.OUTGOING))
        .allNodeScan("n")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a HasDegree filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createRelationship("r", "n", "R1", "m"))
      .filterExpression(
        HasDegree(v"n", Some(relTypeName("R1")), SemanticDirection.OUTGOING, literalInt(10L))(InputPosition.NONE)
      )
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .filterExpression(HasDegree(v"n", Some(relTypeName("R1")), SemanticDirection.OUTGOING, literalInt(10L))(
          InputPosition.NONE
        ))
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a HasDegreeGreaterThan filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createRelationship("r", "n", "R1", "m"))
      .filterExpression(HasDegreeGreaterThan(
        v"n",
        Some(relTypeName("R1")),
        SemanticDirection.OUTGOING,
        literalInt(10L)
      )(InputPosition.NONE))
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .filterExpression(HasDegreeGreaterThan(
          v"n",
          Some(relTypeName("R1")),
          SemanticDirection.OUTGOING,
          literalInt(10L)
        )(InputPosition.NONE))
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a HasDegreeGreaterThanOrEqual filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createRelationship("r", "n", "R1", "m"))
      .filterExpression(HasDegreeGreaterThanOrEqual(
        v"n",
        Some(relTypeName("R1")),
        SemanticDirection.OUTGOING,
        literalInt(10L)
      )(InputPosition.NONE))
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .filterExpression(HasDegreeGreaterThanOrEqual(
          v"n",
          Some(relTypeName("R1")),
          SemanticDirection.OUTGOING,
          literalInt(10L)
        )(InputPosition.NONE))
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a HasDegreeLessThan filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createRelationship("r", "n", "R1", "m"))
      .filterExpression(HasDegreeLessThan(
        v"n",
        Some(relTypeName("R1")),
        SemanticDirection.OUTGOING,
        literalInt(10L)
      )(InputPosition.NONE))
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .filterExpression(HasDegreeLessThan(
          v"n",
          Some(relTypeName("R1")),
          SemanticDirection.OUTGOING,
          literalInt(10L)
        )(InputPosition.NONE))
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a HasDegreeLessThanOrEqual filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .create(createRelationship("r", "n", "R1", "m"))
      .filterExpression(HasDegreeLessThanOrEqual(
        v"n",
        Some(relTypeName("R1")),
        SemanticDirection.OUTGOING,
        literalInt(10L)
      )(InputPosition.NONE))
      .relationshipTypeScan("(n)-[r:R1]-(m)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("n")
        .create(createRelationship("r", "n", "R1", "m"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("R1")).withConflict(Conflict(Id(1), Id(2)))))
        .filterExpression(HasDegreeLessThanOrEqual(
          v"n",
          Some(relTypeName("R1")),
          SemanticDirection.OUTGOING,
          literalInt(10L)
        )(InputPosition.NONE))
        .relationshipTypeScan("(n)-[r:R1]-(m)")
        .build()
    )
  }

  test("Should be eager in read/create conflict with a HasLabelOrTypes filter") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .setLabels("c", "Foo")
      .filter("entity:Foo")
      .projection("$autoint_0 AS foo")
      .unwind("entities AS entity")
      .projection("[a, r, b] AS entities")
      .allRelationshipsScan("(a)-[r]->(b)")
    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("r")
        .setLabels("c", "Foo")
        .eager(ListSet(LabelReadSetConflict(labelName("Foo")).withConflict(Conflict(Id(1), Id(2)))))
        .filter("entity:Foo")
        .projection("$autoint_0 AS foo")
        .unwind("entities AS entity")
        .projection("[a, r, b] AS entities")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  // Writes

  test("Should be eager in read/set conflict with a SetRelationshipProperties") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("a", "b")
      .setRelationshipProperties("r", ("p1", "42"), ("p1", "42"))
      .filter("r.p1 = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("a", "b")
        .setRelationshipProperties("r", ("p1", "42"), ("p1", "42"))
        .eager(ListSet(PropertyReadSetConflict(propName("p1")).withConflict(Conflict(Id(1), Id(2)))))
        .filter("r.p1 = 0")
        .unwind("[1,2,3] AS x")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetPropertiesFromMap (relationship), removeOtherProps = true") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = true)
      .filter("r.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = true)
        .eager(ListSet(UnknownPropertyReadSetConflict.withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .unwind("[1,2,3] AS x")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetPropertiesFromMap (node), removeOtherProps = true") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setPropertiesFromMap("a", "{prop: 42}", removeOtherProps = true)
      .filter("a.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allNodeScan("a")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setPropertiesFromMap("a", "{prop: 42}", removeOtherProps = true)
        .eager(ListSet(UnknownPropertyReadSetConflict.withConflict(Conflict(Id(2), Id(3)))))
        .filter("a.prop = 0")
        .unwind("[1,2,3] AS x")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetPropertiesFromMap (relationship), removeOtherProps = false") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = false)
      .filter("r.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = false)
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .unwind("[1,2,3] AS x")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetPropertiesFromMap (node), removeOtherProps = false") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setPropertiesFromMap("a", "{prop: 42}", removeOtherProps = false)
      .filter("a.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allNodeScan("a")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setPropertiesFromMap("a", "{prop: 42}", removeOtherProps = false)
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("a.prop = 0")
        .unwind("[1,2,3] AS x")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetProperties (relationship)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setProperties("r", "prop" -> "42")
      .filter("r.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setProperties("r", "prop" -> "42")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .unwind("[1,2,3] AS x")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetProperties (node)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setProperties("r", "prop" -> "42")
      .filter("a.prop = 0")
      .allNodeScan("a")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setProperties("r", "prop" -> "42")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("a.prop = 0")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetProperty (relationship)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setProperty("r", "prop", "42")
      .filter("r.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setProperty("r", "prop", "42")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .unwind("[1,2,3] AS x")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetProperty (node)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setProperty("r", "prop", "42")
      .filter("a.prop = 0")
      .allNodeScan("a")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setProperty("r", "prop", "42")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("a.prop = 0")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetRelationshipPropertiesFromMap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setRelationshipPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = false)
      .filter("r.prop = 0")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .setRelationshipPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = false)
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .unwind("[1,2,3] AS x")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/delete conflict with a DeleteRelationship") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .deleteRelationship("r")
      .filter("r.prop = 0")
      .unwind("[1,2,3] AS i")
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .deleteRelationship("r")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .unwind("[1,2,3] AS i")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in read/delete conflict with a DeletePath") {
    val multiOutgoingRelationshipPath = PathExpression(
      NodePathStep(
        node = v"n",
        MultiRelationshipPathStep(
          rel = v"r",
          direction = OUTGOING,
          toNode = Some(v"m"),
          next = NilPathStep()(pos)
        )(pos)
      )(pos)
    )(InputPosition.NONE)
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .deletePath("p")
      .projection(Map("p" -> multiOutgoingRelationshipPath))
      .expandAll("(n)-[r:SMELLS*2]-(m)")
      .nodeByLabelScan("n", "START")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .deletePath("p")
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4)))
        ))
        .projection(Map("p" -> multiOutgoingRelationshipPath))
        .expandAll("(n)-[r:SMELLS*2]-(m)")
        .nodeByLabelScan("n", "START")
        .build()
    )
  }

  test("Should be eager in read/delete conflict with a DetachDeletePath") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .detachDeletePath("(a)-[r]->(b)")
      .filter("r.prop = 0")
      .allRelationshipsScan("(a)-[r]->(b)")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .detachDeletePath("(a)-[r]->(b)")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3)))))
        .filter("r.prop = 0")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test(
    "Should be eager in read/set conflict with a SetRelationshipPropertyPattern"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(foo)]-(m)")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setRelationshipProperty("i", "foo", "i_bar")
        )
      )
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(foo)]-(m)")
        .eager(ListSet(
          PropertyReadSetConflict(propName("foo")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Place")).withConflict(Conflict(Id(4), Id(3)))
        ))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setRelationshipProperty("i", "foo", "i_bar")
          )
        )
        .argument()
        .build()
    )
  }

  test(
    "Should be eager in read/set conflict with a SetRelationshipPropertiesPattern"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(p1)]-(m)")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setRelationshipProperties("i", ("p1", "42"), ("p1", "42"))
        )
      )
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(p1)]-(m)")
        .eager(ListSet(
          PropertyReadSetConflict(propName("p1")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Place")).withConflict(Conflict(Id(4), Id(3)))
        ))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setRelationshipProperties("i", ("p1", "42"), ("p1", "42"))
          )
        )
        .argument()
        .build()
    )
  }

  test(
    "Should be eager in read/set conflict with a SetRelationshipPropertiesFromMapPattern"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setRelationshipPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = false)
        )
      )
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
        .eager(ListSet(
          PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Place")).withConflict(Conflict(Id(4), Id(3)))
        ))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setRelationshipPropertiesFromMap("r", "{prop: 42, foo: a.bar}", removeOtherProps = false)
          )
        )
        .unwind("[1,2] AS x")
        .argument()
        .build()
    )
  }

  test(
    "Should be eager in read/set conflict with a SetPropertyPattern"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setProperty("r", "prop", "r_prop")
        )
      )
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
        .eager(ListSet(
          UnknownPropertyReadSetConflict.withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Place")).withConflict(Conflict(Id(4), Id(3)))
        ))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setProperty("r", "prop", "r_prop")
          )
        )
        .unwind("[1,2] AS x")
        .argument()
        .build()
    )
  }

  test(
    "Should be eager in read/set conflict with a SetPropertiesPattern"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setProperties("r", ("p1", "42"), ("p1", "42"))
        )
      )
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
        .eager(ListSet(
          UnknownPropertyReadSetConflict.withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Place")).withConflict(Conflict(Id(4), Id(3)))
        ))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setProperties("r", ("p1", "42"), ("p1", "42"))
          )
        )
        .unwind("[1,2] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in read/set conflict with a SetPropertyFromMapPattern") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .apply()
      .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
      .foreach(
        "x",
        "[1]",
        Seq(
          createPattern(
            Seq(createNode("e", "Event"), createNode("p", "Place")),
            Seq(createRelationship("i", "e", "IN", "p"))
          ),
          setPropertyFromMap("r", "{prop: 42, foo: a.bar}")
        )
      )
      .unwind("[1,2] AS x").newVar("x", CTInteger)
      .argument()

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .apply()
        .|.relationshipIndexOperator("(n)-[r:R(prop)]-(m)")
        .eager(ListSet(
          UnknownPropertyReadSetConflict.withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Event")).withConflict(Conflict(Id(4), Id(3))),
          LabelReadSetConflict(labelName("Place")).withConflict(Conflict(Id(4), Id(3)))
        ))
        .foreach(
          "x",
          "[1]",
          Seq(
            createPattern(
              Seq(createNode("e", "Event"), createNode("p", "Place")),
              Seq(createRelationship("i", "e", "IN", "p"))
            ),
            setPropertyFromMap("r", "{prop: 42, foo: a.bar}")
          )
        )
        .unwind("[1,2] AS x")
        .argument()
        .build()
    )
  }

  test(
    "Should be eager in read/delete conflict with a DetachDeleteNode and possible conflict with unknown relationship"
  ) {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .detachDeleteNode("a")
      .union().withCardinality(20)
      .|.relationshipTypeScan("(n)-[r:R1]->(m)").withCardinality(10)
      .allNodeScan("a")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("result")
        .projection("1 AS result")
        .detachDeleteNode("a")
        .union()
        .|.eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4)))
        ))
        .|.relationshipTypeScan("(n)-[r:R1]->(m)")
        .allNodeScan("a")
        .build()
    )
  }

  test("inserts no Eager between relationship Create and Create") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .create(createRelationship("o", "m", "R", "n"))
      .create(createRelationship("r", "n", "R", "m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between relationship Create and Create with subsequent filters") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .filter("r.prop > 0")
      .filter("o.prop > 0")
      .create(createRelationship("o", "m", "R", "n"))
      .create(createRelationship("r", "n", "R", "m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between relationship Create and Create in Foreach") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .foreach("i", "[1]", Seq(createPattern(Seq.empty, Seq(createRelationship("o", "m", "R", "n")))))
      .create(createRelationship("r", "n", "R", "m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between relationship Delete and Create") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .deleteRelationship("r")
      .create(createRelationship("r", "n", "R", "m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("inserts no Eager between relationship Create and Delete") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("m")
      .deleteRelationship("r")
      .create(createRelationship("r", "n", "R", "m"))
      .allNodeScan("n")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(plan)
  }

  test("Should be eager in Delete/Read conflict with read in NodeHashJoin") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .nodeHashJoin("n")
      .|.nodeByLabelScan("n", "B")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (NodeHashJoin)
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .nodeHashJoin("n")
        .|.nodeByLabelScan("n", "B")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in LeftOuterHashJoin") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .leftOuterHashJoin("n")
      .|.nodeByLabelScan("n", "B")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (LeftOuterHashJoin)
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .leftOuterHashJoin("n")
        .|.nodeByLabelScan("n", "B")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in RightOuterHashJoin") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .rightOuterHashJoin("n")
      .|.nodeByLabelScan("n", "B")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (RightOuterHashJoin)
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .rightOuterHashJoin("n")
        .|.nodeByLabelScan("n", "B")
        .unwind("[1,2,3] AS x")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Sort") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      // Placed Sort on RHS of Apply. Sort is Eager, so if it was on top, we would not need an Eager.
      .|.sort("n DESC")
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 4 (Sort)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.sort("n DESC")
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in PartialSort") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .partialSort(Seq("x ASC"), Seq("n DESC"))
      .apply()
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger).newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (PartialSort)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5)))
        ))
        .partialSort(Seq("x ASC"), Seq("n DESC"))
        .apply()
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Top") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      // Placed Top on RHS of Apply. Top is Eager, so if it was on top, we would not need an Eager.
      .|.top(1, "n DESC")
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 4 (Top)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.top(1, "n DESC")
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Top1WithTies") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      // Placed Top on RHS of Apply. Top is Eager, so if it was on top, we would not need an Eager.
      .|.top1WithTies("n DESC")
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 4 (Top1WithTies)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.top1WithTies("n DESC")
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in PartialTop") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .partialTop(2, Seq("x ASC"), Seq("n DESC"))
      .apply()
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (PartialTop)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5)))
        ))
        .partialTop(2, Seq("x ASC"), Seq("n DESC"))
        .apply()
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in OrderedUnion") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .orderedUnion("n ASC")
      .|.nodeByLabelScan("n", "B")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (OrderedUnion)
        .eager(ListSet(ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3)))))
        .orderedUnion("n ASC")
        .|.nodeByLabelScan("n", "B")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in ConditionalApply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .conditionalApply("n")
      .|.argument("n")
      .apply()
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (ConditionalApply)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(6)))
        ))
        .conditionalApply("n")
        .|.argument("n")
        .apply()
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in AntiConditionalApply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .antiConditionalApply("n")
      .|.argument("n")
      .apply()
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (AntiConditionalApply)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(6)))
        ))
        .antiConditionalApply("n")
        .|.argument("n")
        .apply()
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in RollUpApply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .rollUpApply("list", "n").newVar("list", CTList(CTNode))
      .|.expand("(n)-[r]->(m)")
      .|.argument("n")
      .apply()
      .|.nodeByLabelScan("n", "A")
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        .eager(ListSet(
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4))),
          // Important that this is ID 3 (RollUpApply)
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(7)))
        ))
        .rollUpApply("list", "n")
        .|.expand("(n)-[r]->(m)")
        .|.argument("n")
        .apply()
        .|.nodeByLabelScan("n", "A")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in AssertSameNode") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .assertSameNode("n")
      .|.nodeIndexOperator("n:B(prop = 0)", unique = true)
      .unwind("[1,2,3] AS x").newVar("x", CTInteger)
      .nodeIndexOperator("n:A(prop = 0)", unique = true)
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (AssertSameNode)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4)))
        ))
        .assertSameNode("n")
        .|.nodeIndexOperator("n:B(prop = 0)", unique = true)
        .unwind("[1,2,3] AS x")
        .nodeIndexOperator("n:A(prop = 0)", unique = true)
        .build()
    )
  }

  test("should correctly eagerize AssertSameNode on the RHS of Apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.expand("(o)-->(anotherO)")
      .|.assertSameNode("o")
      .|.|.nodeIndexOperator("o:O(prop = 0)", unique = true)
      .|.nodeIndexOperator("o:OO(prop = 0)", unique = true)
      .create(createNodeWithProperties("newO", Seq("O", "OO"), "{prop: 0}"))
      .filter("m:M")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    val expectedPlan = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .apply()
      .|.expand("(o)-->(anotherO)")
      .|.assertSameNode("o")
      .|.|.nodeIndexOperator("o:O(prop = 0)", unique = true)
      .|.nodeIndexOperator("o:OO(prop = 0)", unique = true)
      .eager(ListSet(
        PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(5))),
        PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(7), Id(6))),
        LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(7), Id(3))),
        LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(7), Id(5))),
        LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(7), Id(6))),
        LabelReadSetConflict(labelName("OO")).withConflict(Conflict(Id(7), Id(3))),
        LabelReadSetConflict(labelName("OO")).withConflict(Conflict(Id(7), Id(5))),
        LabelReadSetConflict(labelName("OO")).withConflict(Conflict(Id(7), Id(6)))
      ))
      .create(createNodeWithProperties("newO", Seq("O", "OO"), "{prop: 0}"))
      .filter("m:M")
      .expand("(n)-[r]->(m)")
      .allNodeScan("n")
      .build()

    result shouldEqual expectedPlan
  }

  test("Should be eager in Delete/Read conflict with read in AssertSameRelationship") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteRelationship("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(n2)-[r:R(prop < 100)]->(m2)", unique = true)
      .unwind("[1,2,3] AS i")
      .relationshipIndexOperator("(n)-[r:R(prop > 0)]->(m)", unique = true)
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteRelationship("r")
        // Important that this is ID 3 (AssertSameRelationship)
        .eager(ListSet(
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4)))
        ))
        .assertSameRelationship("r")
        .|.relationshipIndexOperator("(n2)-[r:R(prop < 100)]->(m2)", unique = true)
        .unwind("[1,2,3] AS i")
        .relationshipIndexOperator("(n)-[r:R(prop > 0)]->(m)", unique = true)
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Optional") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .apply()
      .|.optional("n")
      .|.filter("n:B")
      .|.argument("n")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 4 (Optional)
        .eager(ListSet(
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(5)))
        ))
        .apply()
        .|.optional("n")
        .|.filter("n:B")
        .|.argument("n")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Projection after the delete") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("e")
      // r was removed by the detach delete, this will be a node with id -1
      .projection("endNode(r) AS e")
      // Default type of variables in tests is CTAny.invariant (which does not include CTNode).
      .newVar("e", CTNode)
      .detachDeleteNode("a")
      .allRelationshipsScan("(a)-[r]->(b)")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("e")
        .projection("endNode(r) AS e")
        .eager(ListSet(
          ReadDeleteConflict("e").withConflict(Conflict(Id(2), Id(1))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(1)))
        ))
        .detachDeleteNode("a")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Projection (property) after the delete (detach delete)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("e")
      // r was removed by the detach delete, this will not work
      .projection("r.prop AS e").newVar("e", CTInteger)
      .detachDeleteNode("a")
      .allRelationshipsScan("(a)-[r]->(b)")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("e")
        .projection("r.prop AS e")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(1)))))
        .detachDeleteNode("a")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Projection (property) after the delete (delete rel)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("e")
      // r was removed by the delete, this will not work
      .projection("r.prop AS e").newVar("e", CTInteger)
      .deleteRelationship("r")
      .expand("(a)-[r]->(b)")
      .allNodeScan("a")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("e")
        .projection("r.prop AS e")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(1)))))
        .deleteRelationship("r")
        .eager(ListSet(ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3)))))
        .expand("(a)-[r]->(b)")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in Projection (property) after the delete (delete node)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("e")
      // if a = b and a was removed by the delete, this will not work
      .projection("a.prop AS e").newVar("e", CTInteger)
      .deleteNode("b")
      .expand("(a)-[r]->(b)")
      .allNodeScan("a")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("e")
        .projection("a.prop AS e")
        .eager(ListSet(ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(1)))))
        .deleteNode("b")
        .eager(ListSet(
          ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(3)))
        ))
        .expand("(a)-[r]->(b)")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in another delete after the delete (detach delete)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      // r was removed by the detach delete a, this will be a node with id -1, nothing will be deleted here.
      .detachDeleteNode("endNode(r)")
      .detachDeleteNode("a")
      .allRelationshipsScan("(a)-[r]->(b)")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .detachDeleteNode("endNode(r)")
        .eager(ListSet(
          ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(3), Id(2)))
        ))
        .detachDeleteNode("a")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with read in another delete after the delete (delete node / rel)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      // r2 could have been removed by the delete r, this will be a node with id -1, nothing will be deleted here.
      .deleteNode("endNode(r2)")
      .deleteRelationship("r")
      .cartesianProduct()
      .|.allRelationshipsScan("(c)-[r2]->(d)")
      .allRelationshipsScan("(a)-[r]->(b)")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .deleteNode("endNode(r2)")
        .eager(ListSet(ReadDeleteConflict("r2").withConflict(Conflict(Id(3), Id(2)))))
        .deleteRelationship("r")
        .eager(ListSet(
          ReadDeleteConflict("c").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("d").withConflict(Conflict(Id(2), Id(5))),
          ReadDeleteConflict("r2").withConflict(Conflict(Id(3), Id(5)))
        ))
        .cartesianProduct()
        .|.allRelationshipsScan("(c)-[r2]->(d)")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with node read in ProjectEndpoints") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .detachDeleteNode("n")
      .projectEndpoints("(n)-[r]->(m2)", startInScope = true, endInScope = false)
      .expand("(n)-[r]->(m)")
      .nodeByLabelScan("n", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .detachDeleteNode("n")
        // Important that this is ID 3 (ProjectEndpoints)
        .eager(ListSet(
          ReadDeleteConflict("m2").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("n").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("m").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(4)))
        ))
        .projectEndpoints("(n)-[r]->(m2)", startInScope = true, endInScope = false)
        .expand("(n)-[r]->(m)")
        .nodeByLabelScan("n", "A")
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with relationship read in ProjectEndpoints") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteRelationship("r")
      .projectEndpoints("(n)-[r]->(m2)", startInScope = true, endInScope = false)
      .apply()
      .|.allRelationshipsScan("(n)-[r]->(m)")
      .unwind("[1,2,3] AS x")
      .argument()
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteRelationship("r")
        // Important that this is ID 3 (ProjectEndpoints)
        .eager(ListSet(
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("r").withConflict(Conflict(Id(2), Id(5)))
        ))
        .projectEndpoints("(n)-[r]->(m2)", startInScope = true, endInScope = false)
        .apply()
        .|.allRelationshipsScan("(n)-[r]->(m)")
        .unwind("[1,2,3] AS x")
        .argument()
        .build()
    )
  }

  test("Should be eager in Delete/Read conflict with node read in TriadicSelection") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .deleteNode("a")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2]->(c)")
      .|.argument("b", "r1")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "X")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .deleteNode("a")
        // Important that this includes ID 3 (TriadicSelection)
        .eager(ListSet(
          ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("a").withConflict(Conflict(Id(2), Id(6))),
          ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(4))),
          ReadDeleteConflict("b").withConflict(Conflict(Id(2), Id(6))),
          ReadDeleteConflict("c").withConflict(Conflict(Id(2), Id(3))),
          ReadDeleteConflict("c").withConflict(Conflict(Id(2), Id(4)))
        ))
        .triadicSelection(positivePredicate = false, "a", "b", "c")
        .|.expandAll("(b)-[r2]->(c)")
        .|.argument("b", "r1")
        .expandAll("(a)-[r1]->(b)")
        .nodeByLabelScan("a", "X")
        .build()
    )
  }

  test("Should insert 2 Eagers in Delete/Read conflicts with last references on LHS and RHS of SubqueryForeach") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .subqueryForeach()
      .|.deleteNode("b")
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("a")
      .filter("a.prop = 0")
      .nodeByLabelScan("a", "A")
    val plan = planBuilder.build()

    val result = eagerizePlan(planBuilder, plan)
    result should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .subqueryForeach()
        .|.deleteNode("b")
        .|.eager(ListSet(
          ReadDeleteConflict("a").withConflict(Conflict(Id(3), Id(4))),
          ReadDeleteConflict("b").withConflict(Conflict(Id(3), Id(4)))
        ))
        .|.expandAll("(a)-[r]->(b)")
        .|.argument("a")
        .eager(ListSet(ReadDeleteConflict("a").withConflict(Conflict(Id(3), Id(6)))))
        .filter("a.prop = 0")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  // SHORTEST TESTS
  test("Insert eager between shortest path pattern and create") {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a_inner)")
      .addTransition(1, 2, "(a_inner)-[r_inner]->(b_inner)")
      .addTransition(2, 1, "(b_inner) (a_inner)")
      .addTransition(2, 3, "(b_inner) (v_inner WHERE v_inner.prop = 42)")
      .addTransition(3, 4, "(v_inner) (c_inner)")
      .addTransition(4, 5, "(c_inner)-[s_inner]->(d_inner)")
      .addTransition(5, 4, "(d_inner) (c_inner)")
      .addTransition(5, 6, "(d_inner) (w_inner WHERE w_inner:N)")
      .setFinalState(6)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNode("o", "M", "O"))
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
        None,
        Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
        Set(("r_inner", "r"), ("s_inner", "s")),
        singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("u", "User")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createNode("o", "M", "O"))
        .eager(ListSet(
          LabelReadSetConflict(labelName("M")).withConflict(Conflict(Id(1), Id(2))),
          LabelReadSetConflict(labelName("O")).withConflict(Conflict(Id(1), Id(2)))
        ))
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
          None,
          Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
          Set(("r_inner", "r"), ("s_inner", "s")),
          singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Insert eager between shortest path pattern and create relationship") {
    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a)-[r_inner]->(b_inner)")
      .setFinalState(1)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createRelationship("r2", "a", "REL", "b"))
      .statefulShortestPath(
        "a",
        "b",
        "SHORTEST 1 ((a)-[r]->(b))",
        None,
        Set(),
        Set(),
        singletonNodeVariables = Set("b_inner" -> "b"),
        singletonRelationshipVariables = Set("r_inner" -> "r"),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("a", "A")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .create(createRelationship("r2", "a", "REL", "b"))
        .eager(ListSet(TypeReadSetConflict(relTypeName("REL")).withConflict(Conflict(Id(1), Id(2)))))
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a)-[r]->(b))",
          None,
          Set(),
          Set(),
          singletonNodeVariables = Set("b_inner" -> "b"),
          singletonRelationshipVariables = Set("r_inner" -> "r"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        )
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("Insert eager between shortest path pattern and delete") {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a_inner)")
      .addTransition(1, 2, "(a_inner)-[r_inner]->(b_inner)")
      .addTransition(2, 1, "(b_inner) (a_inner)")
      .addTransition(2, 3, "(b_inner) (v_inner WHERE v_inner.prop = 42)")
      .addTransition(3, 4, "(v_inner) (c_inner)")
      .addTransition(4, 5, "(c_inner)-[s_inner]->(d_inner)")
      .addTransition(5, 4, "(d_inner) (c_inner)")
      .addTransition(5, 6, "(d_inner) (w_inner WHERE w_inner:N)")
      .setFinalState(6)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .deleteNode("w")
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
        None,
        Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
        Set(("r_inner", "r"), ("s_inner", "s")),
        singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("u", "User")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .deleteNode("w")
        .eager(ListSet(
          ReadDeleteConflict("u").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("a_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("b_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("v").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("c_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("d_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("w").withConflict(Conflict(Id(1), Id(2)))
        ))
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
          None,
          Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
          Set(("r_inner", "r"), ("s_inner", "s")),
          singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Insert eager between shortest path pattern and detach delete") {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a_inner)")
      .addTransition(1, 2, "(a_inner)-[r_inner]->(b_inner)")
      .addTransition(2, 1, "(b_inner) (a_inner)")
      .addTransition(2, 3, "(b_inner) (v_inner WHERE v_inner.prop = 42)")
      .addTransition(3, 4, "(v_inner) (c_inner)")
      .addTransition(4, 5, "(c_inner)-[s_inner]->(d_inner)")
      .addTransition(5, 4, "(d_inner) (c_inner)")
      .addTransition(5, 6, "(d_inner) (w_inner WHERE w_inner:N)")
      .setFinalState(6)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .detachDeleteNode("w")
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
        None,
        Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
        Set(("r_inner", "r"), ("s_inner", "s")),
        singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("u", "User")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .detachDeleteNode("w")
        .eager(ListSet(
          ReadDeleteConflict("u").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("a_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("r_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("b_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("v").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("c_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("s_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("d_inner").withConflict(Conflict(Id(1), Id(2))),
          ReadDeleteConflict("w").withConflict(Conflict(Id(1), Id(2)))
        ))
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
          None,
          Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
          Set(("r_inner", "r"), ("s_inner", "s")),
          singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Insert eager between shortest path pattern and set property") {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a_inner)")
      .addTransition(1, 2, "(a_inner)-[r]->(b_inner)")
      .addTransition(2, 1, "(b_inner) (a_inner)")
      .addTransition(2, 3, "(b_inner) (v_inner WHERE v_inner.prop = 42)")
      .addTransition(3, 4, "(v_inner) (c_inner)")
      .addTransition(4, 5, "(c_inner)-[s_inner]->(d_inner)")
      .addTransition(5, 4, "(d_inner) (c_inner)")
      .addTransition(5, 6, "(d_inner) (w_inner WHERE w_inner:N)")
      .setFinalState(6)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .setNodeProperty("w", "prop", "2")
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
        None,
        Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
        Set(("r_inner", "r"), ("s_inner", "s")),
        singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("u", "User")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("o")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(0)))))
        .setNodeProperty("w", "prop", "2")
        .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(1), Id(2)))))
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE v.prop IN [42] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) AND w:N)",
          None,
          Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
          Set(("r_inner", "r"), ("s_inner", "s")),
          singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Do not insert eager between shortest path pattern and create when there is no overlap") {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a_inner)")
      .addTransition(1, 2, "(a_inner)-[r_inner WHERE r_inner:A]->(b_inner WHERE b_inner:A)")
      .addTransition(2, 1, "(b_inner) (a_inner WHERE a_inner:A)")
      .addTransition(2, 3, "(b_inner) (v_inner WHERE v_inner:A)")
      .addTransition(3, 4, "(v_inner) (c_inner WHERE c_inner:A)")
      .addTransition(4, 5, "(c_inner)-[s_inner WHERE s_inner:A]->(d_inner WHERE d_inner:A)")
      .addTransition(5, 4, "(d_inner) (c_inner WHERE c_inner:A)")
      .addTransition(5, 6, "(d_inner) (w_inner WHERE w_inner:A)")
      .setFinalState(6)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("q", Seq.empty, "{prop1: 5}"))
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) " +
          "AND a:A AND b:A AND c:A AND d:A AND r:A AND s:A AND v:A AND w:A)",
        None,
        Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
        Set(("r_inner", "r"), ("s_inner", "s")),
        singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("u", "User")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test(
    "Do not insert eager between shortest path pattern and create when there is no overlap (nonInlinedPreFilters)"
  ) {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a_inner)")
      .addTransition(1, 2, "(a_inner)-[r_inner WHERE r_inner:A]->(b_inner WHERE b_inner:A)")
      .addTransition(2, 1, "(b_inner) (a_inner WHERE a_inner:A)")
      .addTransition(2, 3, "(b_inner) (v_inner)")
      .addTransition(3, 4, "(v_inner) (c_inner WHERE c_inner:A)")
      .addTransition(4, 5, "(c_inner)-[s_inner WHERE s_inner:A]->(d_inner WHERE d_inner:A)")
      .addTransition(5, 4, "(d_inner) (c_inner WHERE c_inner:A)")
      .addTransition(5, 6, "(d_inner) (w_inner WHERE w_inner:A)")
      .setFinalState(6)
      .build()

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("o")
      .create(createNodeWithProperties("q", Seq.empty, "{prop1: 5}"))
      .statefulShortestPath(
        "u",
        "w",
        "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`) " +
          "AND a:A AND b:A AND c:A AND d:A AND r:A AND s:A AND v:A AND w:A)",
        Some("v:A"),
        Set(("a_inner", "a"), ("b_inner", "b"), ("c_inner", "c"), ("d_inner", "d")),
        Set(("r_inner", "r"), ("s_inner", "s")),
        singletonNodeVariables = Set("v_inner" -> "v", "w_inner" -> "w"),
        singletonRelationshipVariables = Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("u", "User")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  // Ignored tests

  // Update LabelExpressionEvaluator to return a boolean or a set of the conflicting Labels
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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .apply()
        .|.nodeByLabelScan("d", "Event")
        .eager(ListSet(LabelReadSetConflict(labelName("Event"))))
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
    val result = eagerizePlan(planBuilder, plan)

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
    val result = eagerizePlan(planBuilder, plan)

    result should equal(plan)
  }

  test("should rewrite eagerness reasons into summary") {
    val readDeleteA = ReadDeleteConflict("a")
    val labelReadRemoveA = LabelReadRemoveConflict(labelName("a"))
    val typeReadSetRel = TypeReadSetConflict(relTypeName("REL"))

    val plan = new LogicalPlanBuilder(wholePlan = false)
      .eager(ListSet(
        WriteAfterCallInTransactions,
        readDeleteA.withConflict(Conflict(Id(1), Id(2))),
        readDeleteA.withConflict(Conflict(Id(2), Id(3))),
        readDeleteA.withConflict(Conflict(Id(3), Id(4))),
        UpdateStrategyEager,
        ReadCreateConflict.withConflict(Conflict(Id(4), Id(5))),
        ReadCreateConflict.withConflict(Conflict(Id(5), Id(6)))
      ))
      .eager(ListSet(
        labelReadRemoveA.withConflict(Conflict(Id(6), Id(7))),
        labelReadRemoveA.withConflict(Conflict(Id(7), Id(8))),
        typeReadSetRel.withConflict(Conflict(Id(8), Id(9)))
      ))
      .argument()
      .build()

    val rewrittenPlan = plan.endoRewrite(EagerWhereNeededRewriter.summarizeEagernessReasonsRewriter)

    val expectedPlan = new LogicalPlanBuilder(wholePlan = false)
      .eager(ListSet(
        WriteAfterCallInTransactions,
        UpdateStrategyEager,
        Summarized(Map(
          readDeleteA -> SummaryEntry(Conflict(Id(1), Id(2)), 3),
          ReadCreateConflict -> SummaryEntry(Conflict(Id(4), Id(5)), 2)
        ))
      ))
      .eager(ListSet(
        Summarized(Map(
          labelReadRemoveA -> SummaryEntry(Conflict(Id(6), Id(7)), 2),
          typeReadSetRel -> SummaryEntry(Conflict(Id(8), Id(9)), 1)
        ))
      ))
      .argument()
      .build()

    rewrittenPlan shouldEqual expectedPlan
  }

  test("should insert eager and rewrite eagerness reasons into summary") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("n", "B")
      .setLabels("n", "B")
      .setLabels("n", "B")
      .apply()
      .|.nodeByLabelScan("m", "B")
      .nodeByLabelScan("n", "A")

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan, shouldCompressReasons = true)

    val expectedPlan = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("1 AS result")
      .setLabels("n", "B")
      .setLabels("n", "B")
      .setLabels("n", "B")
      .eager(ListSet(Summarized(Map(
        LabelReadSetConflict(labelName("B")) -> SummaryEntry(Conflict(Id(2), Id(6)), 3)
      ))))
      .apply()
      .|.nodeByLabelScan("m", "B")
      .nodeByLabelScan("n", "A")
      .build()

    result shouldEqual expectedPlan
  }

  test("should insert eager on top of filter that might read deleted NODE through a hidden alias") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .deleteNode("n")
      .filter("secretN.prop IS NOT NULL").withCardinality(200)
      .unwind("range(1,10) AS increaseCardinality").withCardinality(100)
      .projection("head([n, 123]) AS secretN").withCardinality(10).newVar("secretN", CTAny.covariant)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    val expectedPlan = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .deleteNode("n")
      .eager(ListSet(
        ReadDeleteConflict("secretN").withConflict(Conflict(Id(2), Id(3))),
        ReadDeleteConflict("secretN").withConflict(Conflict(Id(2), Id(5)))
      ))
      .filter("secretN.prop IS NOT NULL")
      .unwind("range(1, 10) AS increaseCardinality")
      .projection("head([n, 123]) AS secretN")
      .allNodeScan("n")
      .build()

    result shouldEqual expectedPlan
  }

  test("should insert eager on top of filter that might read a deleted RELATIONSHIP through a hidden alias") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .deleteRelationship("r")
      .filter("secretR.prop IS NOT NULL").withCardinality(200)
      .unwind("range(1,10) AS increaseCardinality").withCardinality(100)
      .projection("head([r, 123]) AS secretR").withCardinality(10).newVar("secretR", CTAny.covariant)
      .allRelationshipsScan("(a)-[r]->(b)").withCardinality(10)

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    val expectedPlan = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .deleteRelationship("r")
      .eager(ListSet(
        ReadDeleteConflict("secretR").withConflict(Conflict(Id(2), Id(3))),
        ReadDeleteConflict("secretR").withConflict(Conflict(Id(2), Id(5)))
      ))
      .filter("secretR.prop IS NOT NULL")
      .unwind("range(1, 10) AS increaseCardinality")
      .projection("head([r, 123]) AS secretR")
      .allRelationshipsScan("(a)-[r]->(b)")
      .build()

    result shouldEqual expectedPlan
  }

  // Eager here is unnecessary, should be removed if possible
  test("conflict between CREATE and a hidden node alias") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .create(createNode("newNode"))
      .projection("head([n, 123]) AS secretN").withCardinality(10)
      .newVar("secretN", CTAny.covariant)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    val expectedPlan = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .create(createNode("newNode"))
      .eager(ListSet(ReadCreateConflict.withConflict(Conflict(Id(2), Id(3)))))
      .projection("head([n, 123]) AS secretN")
      .allNodeScan("n")
      .build()

    result shouldEqual expectedPlan
  }

  test("should not introduce any additional conflicts between SET and a hidden node alias") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .setNodeProperty("n", "prop", "123")
      .filter("secretN.prop IS NOT NULL")
      .projection("head([n, 123]) AS secretN").withCardinality(10)
      .newVar("secretN", CTAny.covariant)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val result = eagerizePlan(planBuilder, plan)

    val expectedPlan = new LogicalPlanBuilder()
      .produceResults()
      .emptyResult()
      .setNodeProperty("n", "prop", "123")
      .eager(ListSet(
        // this conflict is expected regardless of whether we read property through an alias or the original variable
        PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(2), Id(3)))
      ))
      .filter("secretN.prop IS NOT NULL")
      .projection("head([n, 123]) AS secretN")
      .allNodeScan("n")
      .build()

    result shouldEqual expectedPlan
  }
}
