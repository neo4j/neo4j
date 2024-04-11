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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.config.PropertyCachingMode
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class InsertCachedPropertiesTest extends CypherFunSuite with PlanMatchHelp with LogicalPlanConstructionTestSupport {
  // Have specific input positions to test semantic table (not DummyPosition)
  private val n = Variable("n")(InputPosition.NONE)
  private val r = Variable("r")(InputPosition.NONE)
  private val x = Variable("x")(InputPosition.NONE)
  private val m = Variable("m")(InputPosition.NONE)
  private val prop = PropertyKeyName("prop")(InputPosition.NONE)
  private val foo = PropertyKeyName("foo")(InputPosition.NONE)
  // Same property in different positions
  private val nProp1 = Property(n, prop)(InputPosition.NONE)
  private val nProp2 = Property(n, prop)(InputPosition(1, 2, 3))
  private val nProp3 = Property(n, prop)(InputPosition(4, 5, 6))
  private val rProp1 = Property(r, prop)(InputPosition.NONE)
  private val rProp2 = Property(r, prop)(InputPosition(7, 8, 9))
  // Same property in different positions
  private val nFoo1 = Property(n, foo)(InputPosition.NONE)
  private val cachedNProp1 = CachedProperty(n, n, prop, NODE_TYPE)(nProp1.position)
  private val cachedNHasProp1 = CachedHasProperty(n, n, prop, NODE_TYPE)(nProp1.position)
  private val cachedNFoo1 = CachedProperty(n, n, foo, NODE_TYPE)(nFoo1.position)
  private val cachedNProp2 = CachedProperty(n, n, prop, NODE_TYPE)(nProp2.position)
  private val cachedNHasProp2 = CachedHasProperty(n, n, prop, NODE_TYPE)(nProp2.position)
  private val cachedNProp3 = CachedProperty(n, n, prop, NODE_TYPE)(nProp3.position)
  private val cachedRRelProp1 = CachedProperty(r, r, prop, RELATIONSHIP_TYPE)(rProp1.position)
  private val cachedRRelHasProp1 = CachedHasProperty(r, r, prop, RELATIONSHIP_TYPE)(rProp1.position)
  private val cachedRRelProp2 = CachedProperty(r, r, prop, RELATIONSHIP_TYPE)(rProp2.position)
  private val cachedRRelHasProp2 = CachedHasProperty(r, r, prop, RELATIONSHIP_TYPE)(rProp2.position)

  private val xProp = Property(x, prop)(InputPosition.NONE)

  def nodeIndexScan(
    node: String,
    label: String,
    property: String,
    getValueFromIndex: GetValueFromIndexBehavior
  ): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek(s"$node:$label($property)", _ => getValueFromIndex)

  def nodeIndexSeek(
    node: String,
    label: String,
    property: String,
    getValueFromIndex: GetValueFromIndexBehavior
  ): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek(s"$node:$label($property = 42)", _ => getValueFromIndex)

  def nodeUniqueIndexSeek(
    node: String,
    label: String,
    property: String,
    getValueFromIndex: GetValueFromIndexBehavior
  ): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek(s"$node:$label($property = 42)", _ => getValueFromIndex, unique = true)

  def nodeIndexContainsScan(
    node: String,
    label: String,
    property: String,
    getValueFromIndex: GetValueFromIndexBehavior
  ): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek("n:Awesome(prop CONTAINS 'foo')", _ => getValueFromIndex)

  def nodeIndexEndsWithScan(
    node: String,
    label: String,
    property: String,
    getValueFromIndex: GetValueFromIndexBehavior
  ): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek("n:Awesome(prop ENDS WITH 'foo')", _ => getValueFromIndex)

  def relationshipIndexScan(
    rel: String,
    typ: String,
    property: String,
    getValueFromIndex: GetValueFromIndexBehavior
  ): RelationshipIndexLeafPlan =
    IndexSeek.relationshipIndexSeek(s"(x)-[$rel:$typ($property)]->(y)", _ => getValueFromIndex)

  for (
    (indexOperator, name) <- Seq(
      (nodeIndexScan _, "indexScan"),
      (nodeIndexSeek _, "indexSeek"),
      (nodeUniqueIndexSeek _, "uniqueIndexSeek"),
      (nodeIndexContainsScan _, "indexContainsScan"),
      (nodeIndexEndsWithScan _, "indexEndsWithScan")
    )
  ) {
    test(
      s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in selection after index operator: $name"
    ) {
      val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
      val plan = Selection(
        Seq(equals(nProp1, literalInt(1))),
        indexOperator("n", "L", "prop", CanGetValue)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Selection(
          Seq(equals(cachedNProp1, literalInt(1))),
          indexOperator("n", "L", "prop", GetValue)
        )
      )
      val initialType = initialTable.types(nProp1)
      newTable.types(cachedNProp1) should be(initialType)
    }

    test(
      s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in projection after index operator: $name"
    ) {
      val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
      val plan = Projection(
        indexOperator("n", "L", "prop", CanGetValue),
        Map(v"x" -> nProp1)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          indexOperator("n", "L", "prop", GetValue),
          Map(v"x" -> cachedNProp1)
        )
      )
      val initialType = initialTable.types(nProp1)
      newTable.types(cachedNProp1) should be(initialType)
    }

    test(s"multiple usages after index should not be knownToAccessStore: $name") {
      val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
      val plan = Projection(
        Selection(Seq(equals(nProp1, literalInt(2))), indexOperator("n", "L", "prop", CanGetValue)),
        Map(v"x" -> nProp2)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          Selection(
            Seq(equals(cachedNProp1, literalInt(2))),
            indexOperator("n", "L", "prop", GetValue)
          ),
          Map(v"x" -> cachedNProp2)
        )
      )

      val initialType = initialTable.types(nProp1)
      newTable.types(cachedNProp1) should be(initialType)
      newTable.types(cachedNProp2) should be(initialType)
    }
  }

  for ((indexOperator, name) <- Seq((relationshipIndexScan _, "indexScan"))) {
    test(
      s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in selection after relationship index operator: $name"
    ) {
      val initialTable = semanticTable(rProp1 -> CTInteger, r -> CTRelationship)
      val plan = Selection(
        Seq(equals(rProp1, literalInt(1))),
        indexOperator("r", "L", "prop", CanGetValue)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Selection(
          Seq(equals(cachedRRelProp1, literalInt(1))),
          indexOperator("r", "L", "prop", GetValue)
        )
      )
      val initialType = initialTable.types(rProp1)
      newTable.types(cachedRRelProp1) should be(initialType)
    }

    test(
      s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in projection after relationship index operator: $name"
    ) {
      val initialTable = semanticTable(rProp1 -> CTInteger, r -> CTRelationship)
      val plan = Projection(
        indexOperator("r", "L", "prop", CanGetValue),
        Map(v"x" -> rProp1)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          indexOperator("r", "L", "prop", GetValue),
          Map(v"x" -> cachedRRelProp1)
        )
      )
      val initialType = initialTable.types(rProp1)
      newTable.types(cachedRRelProp1) should be(initialType)
    }

    test(s"multiple usages after relationship index should not be knownToAccessStore: $name") {
      val initialTable = semanticTable(rProp1 -> CTInteger, rProp2 -> CTInteger, r -> CTNode)
      val plan = Projection(
        Selection(Seq(equals(rProp1, literalInt(2))), indexOperator("r", "L", "prop", CanGetValue)),
        Map(v"x" -> rProp2)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          Selection(
            Seq(equals(cachedRRelProp1, literalInt(2))),
            indexOperator("r", "L", "prop", GetValue)
          ),
          Map(v"x" -> cachedRRelProp2)
        )
      )

      val initialType = initialTable.types(rProp1)
      newTable.types(cachedRRelProp1) should be(initialType)
      newTable.types(cachedRRelProp2) should be(initialType)
    }
  }

  test("should not rewrite prop(n, prop) if index cannot get value") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(nProp1, literalInt(1))),
      nodeIndexScan("n", "L", "prop", DoNotGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should set DoNotGetValue if there is no usage of prop(n, prop)") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nFoo1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(nFoo1, literalInt(1))),
      nodeIndexScan("n", "L", "prop", CanGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should equal(
      Selection(
        Seq(equals(nFoo1, literalInt(1))),
        nodeIndexScan("n", "L", "prop", DoNotGetValue)
      )
    )
    newTable should be(initialTable)
  }

  test("should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in projection after index scan") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Projection(
      nodeIndexScan("n", "L", "prop", CanGetValue),
      Map(v"x" -> nProp1)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Projection(
        nodeIndexScan("n", "L", "prop", GetValue),
        Map(v"x" -> cachedNProp1)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test("should reorder predicates in selection to match what now became the cheapest ordering.") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nFoo1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(greaterThan(prop("n", "otherProp"), literalInt(1)), equals(prop("n", "prop"), prop("n", "foo"))),
      Projection(
        nodeIndexScan("n", "L", "prop", CanGetValue),
        Map(v"x" -> nFoo1)
      )
    )
    val (newPlan, _) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(cachedNProp1, cachedNFoo1), greaterThan(prop("n", "otherProp"), literalInt(1))),
        Projection(
          nodeIndexScan("n", "L", "prop", GetValue),
          Map(v"x" -> cachedNFoo1.copy(knownToAccessStore = true)(cachedNFoo1.position))
        )
      )
    )
  }

  test("should rewrite [prop(n, prop)] to [CachedProperty(n.prop)] with usage in selection after index scan") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(listOf(nProp1), listOfInt(1))),
      nodeIndexScan("n", "L", "prop", CanGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(listOf(cachedNProp1), listOfInt(1))),
        nodeIndexScan("n", "L", "prop", GetValue)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test(
    "should rewrite {foo: prop(n, prop)} to {foo: CachedProperty(n.prop)} with usage in selection after index scan"
  ) {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(mapOf("foo" -> nProp1), mapOfInt(("foo", 1)))),
      nodeIndexScan("n", "L", "prop", CanGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(mapOf("foo" -> cachedNProp1), mapOfInt(("foo", 1)))),
        nodeIndexScan("n", "L", "prop", GetValue)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test("should not explode on missing type info") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val propWithoutSemanticType = propEquality("m", "prop", 2)

    val plan = Selection(
      Seq(equals(nProp1, literalInt(1)), propWithoutSemanticType),
      NodeHashJoin(
        Set(v"n"),
        nodeIndexScan("n", "L", "prop", CanGetValue),
        nodeIndexScan("m", "L", "prop", CanGetValue)
      )
    )
    val (_, newTable) = replace(plan, initialTable)
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test("should cache node property on multiple usages") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)

    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))), AllNodesScan(v"n", Set.empty)),
      Map(v"x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cp1, literalInt(2))), AllNodesScan(v"n", Set.empty)),
        Map(v"x" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedNProp2) should be(initialType)
  }

  test("should cache node has property on multiple usages") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
    val cp1 = cachedNHasProp1.copy(knownToAccessStore = true)(cachedNProp1.position)

    val plan = Projection(
      Selection(Seq(isNotNull(nProp1)), AllNodesScan(v"n", Set.empty)),
      Map(v"x" -> isNotNull(nProp2))
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(isNotNull(cp1)), AllNodesScan(v"n", Set.empty)),
        Map(v"x" -> isNotNull(cachedNHasProp2))
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedNHasProp2) should be(initialType)
  }

  test("should cache node property on multiple usages, one IS NOT NULL and one access") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)

    val plan = Projection(
      Selection(Seq(isNotNull(nProp1)), AllNodesScan(v"n", Set.empty)),
      Map(v"x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(isNotNull(cp1)), AllNodesScan(v"n", Set.empty)),
        Map(v"x" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedNProp2) should be(initialType)
  }

  test("multiple accesses to the same property in the same plan should all be knownToAccessStore") {
    // If there are multiple accesses to the same property in the same plan, we cannot know which one will read first.
    // This is especially important for Selections, if two predicates use the same property.
    // We would otherwise reorder predicates such that the one which had `knownToAccessStore==false` almost certainly would come first,
    // which defeats the purpose of reordering the predicates.

    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, nProp3 -> CTInteger, n -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)
    val cp2 = cachedNProp2.copy(knownToAccessStore = true)(cachedNProp2.position)

    val plan = Projection(
      Selection(
        Seq(equals(nProp1, literalInt(2)), greaterThan(nProp2, literalInt(1))),
        AllNodesScan(v"n", Set.empty)
      ),
      Map(v"x" -> nProp3)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(
          Seq(equals(cp1, literalInt(2)), greaterThan(cp1, literalInt(1))),
          AllNodesScan(v"n", Set.empty)
        ),
        Map(v"x" -> cachedNProp3)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cp2) should be(initialType)
    newTable.types(cachedNProp3) should be(initialType)
  }

  test("should not cache node property on renamed usages within union") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode, m -> CTNode, x -> CTNode)
    val plan = new LogicalPlanBuilder()
      .produceResults("z")
      .projection("m.prop as z")
      .apply()
      .|.union()
      .|.|.projection("n as m")
      .|.|.argument("n")
      .|.projection("x as m")
      .|.expand("(n)-->(m)")
      .|.argument("n")
      .filter("n.prop = 2")
      .allNodeScan("n")
      .build()

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should cache node property for properties with identical renaming across union") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode, m -> CTNode)
    val plan = new LogicalPlanBuilder()
      .produceResults("z")
      .projection("m.prop as z")
      .apply()
      .|.union()
      .|.|.projection("n as m")
      .|.|.argument("n")
      .|.projection("n as m")
      .|.argument("n")
      .filter("n.prop = 2")
      .allNodeScan("n")
      .build()

    val (newPlan, _) = replace(plan, initialTable)
    val expectedPlan = new LogicalPlanBuilder()
      .produceResults("z")
      .projection(Map("z" -> cachedNodeProp("n", "prop", "m")))
      .apply()
      .|.union()
      .|.|.projection("n as m")
      .|.|.argument("n")
      .|.projection("n as m")
      .|.argument("n")
      .filter("cacheFromStore[n.prop] = 2")
      .allNodeScan("n")
      .build()

    newPlan should be(expectedPlan)
  }

  test("should cache node property in union branch") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode, m -> CTNode)
    val plan = new LogicalPlanBuilder()
      .produceResults("z")
      .projection("m.prop as z")
      .apply()
      .|.union()
      .|.|.projection("n as m")
      .|.|.filter("n.prop = 2")
      .|.|.argument("n")
      .|.projection("n as m")
      .|.filter("n.prop = 3")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val (newPlan, _) = replace(plan, initialTable)
    val expectedPlan = new LogicalPlanBuilder()
      .produceResults("z")
      .projection(Map("z" -> CachedProperty(n, m, prop, NODE_TYPE)(InputPosition.NONE)))
      .apply()
      .|.union()
      .|.|.projection("n as m")
      .|.|.filter("cacheN[n.prop] = 2")
      .|.|.argument("n")
      .|.projection("n as m")
      .|.filter("cacheFromStore[n.prop] = 3")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    newPlan should be(expectedPlan)
  }

  test("should not rewrite node property if there is only one usage") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nFoo1 -> CTInteger, n -> CTNode)
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))), AllNodesScan(v"n", Set.empty)),
      Map(v"x" -> nFoo1)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should cache relationship property on multiple usages") {
    val initialTable = semanticTable(rProp1 -> CTInteger, rProp2 -> CTInteger, r -> CTRelationship)
    val cp1 = cachedRRelProp1.copy(knownToAccessStore = true)(cachedRRelProp1.position)

    val plan = Projection(
      Selection(
        Seq(equals(rProp1, literalInt(2))),
        DirectedRelationshipByIdSeek(
          v"r",
          SingleSeekableArg(literalInt(25)),
          v"a",
          v"b",
          Set.empty
        )
      ),
      Map(v"x" -> rProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(
          Seq(equals(cp1, literalInt(2))),
          DirectedRelationshipByIdSeek(
            v"r",
            SingleSeekableArg(literalInt(25)),
            v"a",
            v"b",
            Set.empty
          )
        ),
        Map(v"x" -> cachedRRelProp2)
      )
    )
    val initialType = initialTable.types(rProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedRRelProp2) should be(initialType)
  }

  test("should not cache relationship property if there is only one usage") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTRelationship)
    val plan = Projection(
      DirectedRelationshipByIdSeek(v"r", SingleSeekableArg(literalInt(25)), v"a", v"b", Set.empty),
      Map(v"x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should cache relationship has property on multiple usages") {
    val initialTable = semanticTable(rProp1 -> CTInteger, rProp2 -> CTInteger, r -> CTRelationship)
    val cp1 = cachedRRelHasProp1.copy(knownToAccessStore = true)(cachedRRelProp1.position)

    val plan = Projection(
      Selection(
        Seq(isNotNull(rProp1)),
        DirectedRelationshipByIdSeek(
          v"r",
          SingleSeekableArg(literalInt(25)),
          v"a",
          v"b",
          Set.empty
        )
      ),
      Map(v"x" -> isNotNull(rProp2))
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(
          Seq(isNotNull(cp1)),
          DirectedRelationshipByIdSeek(
            v"r",
            SingleSeekableArg(literalInt(25)),
            v"a",
            v"b",
            Set.empty
          )
        ),
        Map(v"x" -> isNotNull(cachedRRelHasProp2))
      )
    )
    val initialType = initialTable.types(rProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedRRelHasProp2) should be(initialType)
  }

  test("should not do anything with map properties") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTMap)
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))), Argument(Set(v"n"))),
      Map(v"x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should not do anything with untyped variables") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger)
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))), Argument(Set(v"n"))),
      Map(v"x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("renamed variable: n AS x") {
    val initialTable = semanticTable(nProp1 -> CTInteger, xProp -> CTInteger, n -> CTNode, x -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)
    val plan =
      Projection(
        Projection(
          Selection(Seq(equals(nProp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"x" -> n)
        ),
        Map(v"xProp" -> xProp)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Projection(
          Selection(Seq(equals(cp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"x" -> n)
        ),
        Map(v"xProp" -> cachedNProp1.copy(entityVariable = n.copy("x")(n.position))(cachedNProp1.position))
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
  }

  test("renamed variable: n AS x, reading in 2 Selection operators") {
    val cpASx = cachedNProp1.copy(entityVariable = x)(cachedNProp1.position)
    val cpFromStore = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)

    val builder = new LogicalPlanBuilder(wholePlan = false)
      .filterExpression(greaterThan(xProp, literalInt(42)))
      .projection("n AS x")
      .filterExpression(greaterThan(nProp1, literalInt(42)))
      .allNodeScan("n")

    builder.newNode(v"x")
    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types =
        builderTable.types
          .updated(nProp1, ExpressionTypeInfo(CTInteger))
          .updated(xProp, ExpressionTypeInfo(CTInteger))
      )

    val (newPlan, newTable) = replace(builder.build(), initialTable)
    newPlan should be(
      new LogicalPlanBuilder(wholePlan = false)
        .filterExpression(greaterThan(cpASx, literalInt(42)))
        .projection("n AS x")
        .filterExpression(greaterThan(cpFromStore, literalInt(42)))
        .allNodeScan("n")
        .build()
    )
    newTable.types(cpASx) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cpFromStore) should be(ExpressionTypeInfo(CTInteger))
  }

  test("renamed variable: n AS n") {
    val initialTable = semanticTable(nProp1 -> CTInteger, xProp -> CTInteger, n -> CTNode, x -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)
    val plan =
      Projection(
        Projection(
          Selection(Seq(equals(nProp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"n" -> n)
        ),
        Map(v"nProp" -> nProp2)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Projection(
          Selection(Seq(equals(cp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"n" -> n)
        ),
        Map(v"nProp" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
  }

  // The Namespacer should guarantee that this cannot happen
  test("should throw if there is a short cycle in name definitions") {
    val initialTable = semanticTable(n -> CTNode, m -> CTNode, nProp1 -> CTInteger)
    val plan =
      Projection(
        Projection(
          Argument(Set(v"n")),
          Map(v"m" -> n)
        ),
        Map(v"n" -> m, v"p" -> nProp1)
      )

    an[IllegalStateException] should be thrownBy {
      replace(plan, initialTable)
    }
  }

  test("should throw if there is a short cycle in name definitions in one projection") {
    val initialTable = semanticTable(n -> CTNode, m -> CTNode, nProp1 -> CTInteger)
    val plan =
      Projection(
        Argument(Set(v"n")),
        Map(v"m" -> n, v"n" -> m, v"p" -> nProp1)
      )

    an[IllegalStateException] should be thrownBy {
      replace(plan, initialTable)
    }
  }

  test("should throw if there is a longer cycle in name definitions") {
    val initialTable = semanticTable(n -> CTNode, m -> CTNode, x -> CTNode, nProp1 -> CTInteger)
    val plan = Projection(
      Projection(
        Projection(
          Argument(Set(v"n")),
          Map(v"m" -> n)
        ),
        Map(v"x" -> m)
      ),
      Map(v"n" -> x, v"p" -> nProp1)
    )

    an[IllegalStateException] should be thrownBy {
      replace(plan, initialTable)
    }
  }

  // More complex renamings are affected by the Namespacer
  // A test for that can be found in CachedPropertyAcceptanceTest and CachedPropertiesPlanningIntegrationTest

  test("aggregation") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)
    val plan =
      Projection(
        Aggregation(
          Selection(Seq(equals(nProp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"n" -> n),
          Map(v"count(1)" -> count(literalInt(1)))
        ),
        Map(v"nProp" -> nProp2)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Aggregation(
          Selection(Seq(equals(cp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"n" -> n),
          Map(v"count(1)" -> count(literalInt(1)))
        ),
        Map(v"nProp" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedNProp2) should be(initialType)
  }

  test("aggregation with renaming") {
    val initialTable = semanticTable(nProp1 -> CTInteger, xProp -> CTInteger, n -> CTNode, x -> CTNode)
    val cp1 = cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)
    val plan =
      Projection(
        Aggregation(
          Selection(Seq(equals(nProp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"x" -> n),
          Map(v"count(1)" -> count(literalInt(1)))
        ),
        Map(v"xProp" -> xProp)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Aggregation(
          Selection(Seq(equals(cp1, literalInt(2))), Argument(Set(v"n"))),
          Map(v"x" -> n),
          Map(v"count(1)" -> count(literalInt(1)))
        ),
        Map(v"xProp" -> cachedNProp1.copy(entityVariable = n.copy("x")(n.position))(cachedNProp1.position))
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
  }

  test("handle cached property with missing input position") {
    // MATCH (n:A) WITH n AS n2, count(n) AS count MATCH (n2), (m:B) WITH n2 AS n3 MATCH (n3) RETURN n3.prop
    val n2InputPosition = InputPosition(10, 2, 3)
    val plan = new LogicalPlanBuilder()
      .produceResults("`n3.prop`").withEffectiveCardinality(200)
      .projection("n3.prop as `n3.prop`").withEffectiveCardinality(200)
      .projection("n2 AS n3").withEffectiveCardinality(200)
      .apply().withEffectiveCardinality(200)
      .|.nodeByLabelScan("m", "B").withEffectiveCardinality(200)
      // Note, push down properties doesn't keep track of input position in all cases and can produce output like the following
      .cacheProperties(Set[LogicalProperty](Property(
        Variable("n2")(InputPosition.NONE),
        PropertyKeyName("prop")(InputPosition.NONE)
      )(InputPosition.NONE)))
      .aggregation(Seq("n as n2"), Seq("count(n) AS count")).withEffectiveCardinality(3).newVar(
        "n2",
        n2InputPosition,
        CTNode
      )
      .nodeByLabelScan("n", "A").withEffectiveCardinality(10)

    val (resultPlan, _) = replace(plan.build(), plan.getSemanticTable)
    resultPlan shouldBe
      new LogicalPlanBuilder()
        .produceResults("`n3.prop`")
        .projection(Map("n3.prop" -> CachedProperty(
          n,
          Variable("n3")(n2InputPosition),
          PropertyKeyName("prop")(InputPosition.NONE),
          NODE_TYPE
        )(InputPosition.NONE)))
        .projection("n2 AS n3")
        .apply()
        .|.nodeByLabelScan("m", "B")
        .cacheProperties(Set[LogicalProperty](CachedProperty(
          n,
          Variable("n2")(n2InputPosition),
          PropertyKeyName("prop")(InputPosition.NONE),
          NODE_TYPE,
          knownToAccessStore = true
        )(InputPosition.NONE)))
        .aggregation(Seq("n as n2"), Seq("count(n) AS count"))
        .nodeByLabelScan("n", "A")
        .build()
  }

  test("pushed down properties should be knownToAccessStore") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x").withEffectiveCardinality(50)
      .projection("n.prop AS x").withEffectiveCardinality(50)
      .expandAll("(n)-->(m)").withEffectiveCardinality(50)
      .allNodeScan("n").withEffectiveCardinality(5)

    val (resultPlan, _) = replace(
      plan.build(),
      plan.getSemanticTable,
      plan.effectiveCardinalities,
      plan.idGen,
      pushdownPropertyReads = true
    )

    resultPlan shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("cache[n.prop] AS x")
        .expandAll("(n)-->(m)")
        .cacheProperties(Set[LogicalProperty](CachedProperty(
          n,
          Variable("n")(InputPosition.NONE),
          PropertyKeyName("prop")(InputPosition.NONE),
          NODE_TYPE,
          knownToAccessStore = true
        )(InputPosition.NONE)))
        .allNodeScan("n")
        .build()
  }

  test("should cache in nested plan expression") {
    val cp2FromStore = cachedNProp2.copy(knownToAccessStore = true)(cachedNProp2.position)

    val builder = new LogicalPlanBuilder(wholePlan = false)
      .nestedPlanExistsExpressionProjection("list")
      .|.projection(Map("bar" -> nProp1))
      .|.projection(Map("foo" -> nProp2))
      .|.allNodeScan("n")
      .argument()

    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types =
        builderTable.types
          .updated(nProp1, ExpressionTypeInfo(CTInteger))
          .updated(nProp2, ExpressionTypeInfo(CTInteger))
      )

    val (newPlan, newTable) = replace(builder.build(), initialTable)
    newPlan should be(
      new LogicalPlanBuilder(wholePlan = false)
        .nestedPlanExistsExpressionProjection("list")
        .|.projection(Map("bar" -> cachedNProp1))
        .|.projection(Map("foo" -> cp2FromStore))
        .|.allNodeScan("n")
        .argument()
        .build()
    )
    newTable.types(cachedNProp1) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cp2FromStore) should be(ExpressionTypeInfo(CTInteger))
  }

  test("should cache in LHS and RHS with Apply") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .projection("n.prop AS baz", "n.lt AS bazLT", "n.rt AS bazRT")
      .apply()
      .|.projection("n.prop AS bar", "n.lr AS barLR", "n.rt AS barRT")
      .|.argument("n")
      .projection("n.prop AS foo", "n.lr AS fooLR", "n.lt AS fooLT")
      .allNodeScan("n")

    val (newPlan, newTable) = replace(builder.build(), builder.getSemanticTable)
    newPlan should be(
      new LogicalPlanBuilder(wholePlan = false)
        .projection(Map(
          "baz" -> cachedNProp1,
          "bazLT" -> cachedNodeProp("n", "lt"),
          "bazRT" -> cachedNodeProp("n", "rt")
        ))
        .apply()
        .|.projection(Map(
          "bar" -> cachedNProp1,
          "barLR" -> cachedNodeProp("n", "lr"),
          "barRT" -> cachedNodePropFromStore("n", "rt")
        ))
        .|.argument("n")
        .projection(Map(
          "foo" -> cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position),
          "fooLR" -> cachedNodePropFromStore("n", "lr"),
          "fooLT" -> cachedNodePropFromStore("n", "lt")
        ))
        .allNodeScan("n")
        .build()
    )
  }

  test("should not cache in Apply plan with only 1 usage in LHS") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.argument("n")
      .projection("n.prop AS foo")
      .allNodeScan("n")

    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types =
        builderTable.types
          .updated(nProp1, ExpressionTypeInfo(CTInteger))
      )

    val plan = builder.build()
    val (newPlan, _) = replace(plan, initialTable)
    newPlan should be(plan)
  }

  test("should cache in LHS and RHS with CartesianProduct") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .projection("n.prop AS baz", "n.lt AS bazLT", "n.rt AS bazRT")
      .cartesianProduct()
      .|.projection("n.prop AS bar", "n.lr AS barLR", "n.rt AS barRT")
      .|.allNodeScan("n")
      .projection("n.prop AS foo", "n.lr AS fooLR", "n.lt AS fooLT")
      .allNodeScan("n")

    val (newPlan, newTable) = replace(builder.build(), builder.getSemanticTable)
    newPlan should be(
      new LogicalPlanBuilder(wholePlan = false)
        .projection(Map(
          "baz" -> cachedNProp1,
          "bazLT" -> cachedNodeProp("n", "lt"),
          "bazRT" -> cachedNodeProp("n", "rt")
        ))
        .cartesianProduct()
        .|.projection(Map(
          "bar" -> cachedNProp1,
          "barLR" -> cachedNodeProp("n", "lr"),
          "barRT" -> cachedNodePropFromStore("n", "rt")
        ))
        .|.allNodeScan("n")
        .projection(Map(
          "foo" -> cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position),
          "fooLR" -> cachedNodePropFromStore("n", "lr"),
          "fooLT" -> cachedNodePropFromStore("n", "lt")
        ))
        .allNodeScan("n")
        .build()
    )
  }

  test("should cache on the RHS of a nested index join") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .projection("a.prop AS `a.prop`")
      .apply()
      .|.nodeIndexOperator(
        "b:B(prop = ???)",
        argumentIds = Set("a"),
        paramExpr = Some(prop("a", "prop")),
        indexType = IndexType.RANGE
      )
      .nodeIndexOperator("a:A(prop > 'foo')", indexType = IndexType.RANGE)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder(wholePlan = false)
      .projection("cacheN[a.prop] AS `a.prop`")
      .apply()
      .|.nodeIndexOperator(
        "b:B(prop = ???)",
        argumentIds = Set("a"),
        paramExpr = Some(cachedNodePropFromStore("a", "prop")),
        indexType = IndexType.RANGE
      )
      .nodeIndexOperator("a:A(prop > 'foo')", indexType = IndexType.RANGE)
      .build()
  }

  test("should find property usage in value hash join") {
    val builder = new LogicalPlanBuilder()
      .produceResults("a", "b")
      .valueHashJoin("a.x = b.y")
      .|.nodeIndexOperator("b:B(y < 200)", getValue = _ => CanGetValue, indexType = IndexType.RANGE)
      .nodeIndexOperator("a:A(x > 100)", getValue = _ => CanGetValue, indexType = IndexType.RANGE)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("a", "b")
      .valueHashJoin("cacheN[a.x] = cacheN[b.y]")
      .|.nodeIndexOperator("b:B(y < 200)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .nodeIndexOperator("a:A(x > 100)", getValue = _ => GetValue, indexType = IndexType.RANGE)
      .build()
  }

  test("should not find extra property usages due to Apply and multiple Arguments") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.nodeHashJoin("n")
      .|.|.argument("n")
      .|.argument("n")
      .filter("n.prop = 2")
      .allNodeScan("n")

    val initialPlan = planBuilder.build()
    val initialTable = planBuilder.getSemanticTable

    val (newPlan, newTable) = replace(initialPlan, initialTable)
    newPlan shouldBe initialPlan
    newTable shouldBe initialTable
  }

  test("should get values from index on LHS of a Union when the same property is also used on RHS") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("a.x AS result")
      .union()
      .|.filter("a.prop < 321")
      .|.nodeIndexOperator("a:A(otherProp)", getValue = _ => CanGetValue)
      .filter("a.prop < 123")
      .nodeIndexOperator("a:A(prop)", getValue = _ => CanGetValue)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("a.x AS result")
      .union()
      .|.filter("cacheNFromStore[a.prop] < 321")
      .|.nodeIndexOperator("a:A(otherProp)", getValue = _ => DoNotGetValue)
      .filter("cacheN[a.prop] < 123")
      .nodeIndexOperator("a:A(prop)", getValue = _ => GetValue)
      .build()
  }

  test("should not cache properties from a single case branch") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection(
        "CASE WHEN n.p1 > n.p2 THEN n.p1 WHEN n.p3 > n.p4 THEN n.p4 END AS result"
      )
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe builder.build()
  }

  /*
  The insert cached properties logic doesn't handle correctly the case where you have two indexes, with different capabilities, on the same property.
  It would attempt to get the value from both indexes, including the one that doesn't support it, leading to an exception at runtime.
  For example, the following query would fail:
    CREATE TEXT INDEX FOR (n:L) ON (n.p)
    CREATE INDEX FOR (n:L) ON (n.p)
    CREATE (:L {p: "test"})
    MATCH (n:L) WHERE (n.p =~ "") XOR (n.p ENDS WITH "") RETURN n
   This test ensures that it doesn't happen anymore.
   */
  test("Do _not_ get value from an index that doesn't support it when there are two indexes on the same property") {
    val builder = new LogicalPlanBuilder()
      .produceResults("a")
      .union()
      .|.filter("a.prop < 321")
      .|.nodeIndexOperator("a:A(prop)", getValue = _ => DoNotGetValue)
      .filter("a.prop < 123")
      .nodeIndexOperator("a:A(prop)", getValue = _ => CanGetValue)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("a")
      .union()
      // arguably, this shouldn't be cached, this is the limitation of this fix
      .|.filter("cacheNFromStore[a.prop] < 321")
      .|.nodeIndexOperator("a:A(prop)", getValue = _ => DoNotGetValue)
      .filter("cacheN[a.prop] < 123")
      .nodeIndexOperator("a:A(prop)", getValue = _ => GetValue)
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .eager()
      .setProperty("n", "prop", "coalesce(n.prop + 1, 0)")
      .eager()
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheN[n.prop] as newValue")
      .eager()
      .setProperty("n", "prop", "coalesce(n.prop + 1, 0)")
      .eager()
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetProperties") {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "newValue2", "oldValue", "oldValue2")
      .projection("n.prop as newValue", "n.prop2 as newValue2")
      .setProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1")
      .projection("n.prop as oldValue", "n.prop2 as oldValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "newValue2", "oldValue", "oldValue2")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue", "cacheN[n.prop2] as newValue2")
      .setProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1")
      .projection("cacheNFromStore[n.prop] as oldValue", "cacheNFromStore[n.prop2] as oldValue2")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetPropertiesFromMap") {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      .projection("n.prop as newValue")
      .setPropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false)
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue")
      .setPropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false)
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetNodeProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      .projection("n.prop as newValue")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetNodeProperties") {
    val builder = new LogicalPlanBuilder()
      .produceResults("oldValue", "oldValue2", "newValue", "newValue2")
      .projection("n.prop as newValue", "n.prop2 as newValue2")
      .setNodeProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1")
      .projection("n.prop as oldValue", "n.prop2 as oldValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("oldValue", "oldValue2", "newValue", "newValue2")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue", "cacheN[n.prop2] as newValue2")
      .setNodeProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1")
      .projection("cacheNFromStore[n.prop] as oldValue", "cacheNFromStore[n.prop2] as oldValue2")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetNodePropertiesFromMap") {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      .projection("n.prop as newValue")
      .setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false)
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue")
      .setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false)
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetRelationshipProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop as newValue")
      .setRelationshipProperty("r", "prop", "r.prop + 1")
      .projection("r.prop as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheR[r.prop] as newValue")
      .setRelationshipProperty("r", "prop", "r.prop + 1")
      .projection("cacheRFromStore[r.prop] as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in expression that is written to same property - SetRelationshipProperties") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop1 as newValue1", "r.prop2 as newValue2")
      .setRelationshipProperties("r", ("prop1", "r.prop1 + 1"), ("prop2", "r.prop2 + 1"))
      .projection("r.prop1 as oldValue1", "r.prop2 as oldValue2")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheR[r.prop1] as newValue1", "cacheR[r.prop2] as newValue2")
      .setRelationshipProperties("r", ("prop1", "r.prop1 + 1"), ("prop2", "r.prop2 + 1"))
      .projection("cacheRFromStore[r.prop1] as oldValue1", "cacheRFromStore[r.prop2] as oldValue2")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in expression that is written to same property - SetRelationshipPropertiesFromMap"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop as newValue")
      .setRelationshipPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = false)
      .projection("r.prop as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheR[r.prop] as newValue")
      .setRelationshipPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = false)
      .projection("cacheRFromStore[r.prop] as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test("should not use cached properties in merge on match expression that is written to same property - SetProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperty("n", "prop", "coalesce(n.prop + 1, 0)"))
      )
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperty("n", "prop", "coalesce(n.prop + 1, 0)"))
      )
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetProperties"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "newValue2", "oldValue", "oldValue2")
      .projection("n.prop as newValue", "n.prop2 as newValue2")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1"))
      )
      .projection("n.prop as oldValue", "n.prop2 as oldValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "newValue2", "oldValue", "oldValue2")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue", "cacheN[n.prop2] as newValue2")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1"))
      )
      .projection("cacheNFromStore[n.prop] as oldValue", "cacheNFromStore[n.prop2] as oldValue2")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetNodeProperty"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      .projection("n.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperty("n", "prop", "n.prop + 1"))
      )
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperty("n", "prop", "n.prop + 1"))
      )
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetNodeProperties"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("oldValue", "oldValue2", "newValue", "newValue2")
      .projection("n.prop as newValue", "n.prop2 as newValue2")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1"))
      )
      .projection("n.prop as oldValue", "n.prop2 as oldValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("oldValue", "oldValue2", "newValue", "newValue2")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue", "cacheN[n.prop2] as newValue2")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperties("n", "prop" -> "n.prop + 1", "prop2" -> "n.prop2 + 1"))
      )
      .projection("cacheNFromStore[n.prop] as oldValue", "cacheNFromStore[n.prop2] as oldValue2")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetNodePropertiesFromMap"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      .projection("n.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false))
      )
      .projection("n.prop as oldValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "oldValue")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheN[n.prop] as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false))
      )
      .projection("cacheNFromStore[n.prop] as oldValue")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetRelationshipProperty"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setRelationshipProperty("r", "prop", "r.prop + 1"))
      )
      .projection("r.prop as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheR[r.prop] AS newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setRelationshipProperty("r", "prop", "r.prop + 1"))
      )
      .projection("cacheRFromStore[r.prop] AS oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetRelationshipProperties"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setRelationshipProperties("r", "prop" -> "r.prop + 1"))
      )
      .projection("r.prop as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheR[r.prop] AS newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setRelationshipProperties("r", "prop" -> "r.prop + 1"))
      )
      .projection("cacheRFromStore[r.prop] AS oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should not use cached properties in merge on match expression that is written to same property - SetRelationshipPropertiesFromMap"
  ) {

    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setRelationshipPropertiesFromMap("r", "{prop: r.prop + 1}"))
      )
      .projection("r.prop as oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      // Note: Cached properties in this final read is both useless and harmless. Properties will be read from the
      // transaction context in both cases. It would be preferred *not* to insert cached properties here but we deemed
      // it not worth the effort right now.
      .projection("cacheR[r.prop] AS newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setRelationshipPropertiesFromMap("r", "{prop: r.prop + 1}"))
      )
      .projection("cacheRFromStore[r.prop] AS oldValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties - same property and entity in read and set, but the read entity is aliased to a different variable name"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .eager()
      .setNodeProperty("n", "prop", "n2.prop + 1")
      .eager()
      .projection("n2.prop as oldValue")
      .projection("n as n2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    val otherPlan = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheN[n.prop] as newValue")
      .eager()
      .setNodeProperty(
        "n",
        "prop",
        value = Add(
          CachedProperty(n, Variable("n2")(pos), PropertyKeyName("prop")(pos), NODE_TYPE)(pos),
          SignedDecimalIntegerLiteral("1")(pos)
        )(pos)
      )
      .eager()
      .projection(Map("oldValue" -> CachedProperty(
        n,
        Variable("n2")(pos),
        PropertyKeyName("prop")(pos),
        NODE_TYPE,
        knownToAccessStore = true
      )(pos)))
      .projection("n as n2")
      .allNodeScan("n")
      .build()
    newPlan shouldBe otherPlan
  }

  test("should use cached properties - same property but different node in read and set") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .eager()
      .setNodeProperty("n", "prop", "n2.prop + 1")
      .eager()
      .projection("n2.prop as oldValue")
      .cartesianProduct()
      .|.allNodeScan("n2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    val otherPlan = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .eager()
      .setNodeProperty("n", "prop", "cacheN[n2.prop] + 1")
      .eager()
      .projection("cacheNFromStore[n2.prop] as oldValue")
      .cartesianProduct()
      .|.allNodeScan("n2")
      .allNodeScan("n")
      .build()

    otherPlan shouldBe newPlan
  }

  test("should not cache in set node property with surrounding property reads") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as value3")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .projection("n.prop as value2")
      .projection("n.prop as value1")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    val otherPlan = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheN[n.prop] AS value3")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .projection("cacheN[n.prop] AS value2")
      .projection("cacheNFromStore[n.prop] AS value1")
      .allNodeScan("n")
      .build()

    otherPlan shouldBe newPlan
  }

  test("should not cache property in set property below property reads") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as value2")
      .projection("n.prop as value1")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheN[n.prop] AS value2")
      .projection("cacheNFromStore[n.prop] AS value1")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .allNodeScan("n")
      .build()
  }

  test("should cache property in merge on match") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperty("n", "prop", "n.prop + 1"))
      )
      .projection("n.prop as oldValue")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheN[n.prop] AS newValue")
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(setNodeProperty("n", "prop", "n.prop + 1"))
      )
      .projection("cacheNFromStore[n.prop] AS oldValue")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .allNodeScan("n")
      .build()
  }

  test("should cache property in merge with multiple on match") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection(
        "n.prop as newValue",
        "n.prop2 as newValue2",
        "r.prop as newValue3",
        "r.prop2 as newValue4"
      )
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(
          setNodeProperty("n", "prop", "n.prop + 1"),
          setNodeProperties("n", "other" -> "n.prop2", "prop2" -> "n.prop2 + 1"),
          setRelationshipProperty("r", "prop", "r.prop + 1")
        )
      )
      .projection(
        "n.prop as oldValue",
        "n.prop2 as oldValue2",
        "r.prop as oldValue3",
        "r.prop2 as oldValue4"
      )
      .expandAll("(n)-[r]->(n)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection(
        "cacheN[n.prop] as newValue",
        "cacheN[n.prop2] as newValue2",
        "cacheR[r.prop] as newValue3",
        "cacheR[r.prop2] as newValue4"
      )
      .merge(
        nodes = Seq(createNode("n")),
        onMatch = Seq(
          setNodeProperty("n", "prop", "n.prop + 1"),
          setNodeProperties("n", "other" -> "cacheN[n.prop2]", "prop2" -> "n.prop2 + 1"),
          setRelationshipProperty("r", "prop", "r.prop + 1")
        )
      )
      .projection(
        "cacheNFromStore[n.prop] as oldValue",
        "cacheNFromStore[n.prop2] as oldValue2",
        "cacheRFromStore[r.prop] as oldValue3",
        "cacheRFromStore[r.prop2] as oldValue4"
      )
      .expandAll("(n)-[r]->(n)")
      .allNodeScan("n")
      .build()
  }

  test("should use cached properties in expression that is written to different property - SetProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .eager()
      .setProperty("n", "prop", "coalesce(n.prop2 + 1, 0)")
      .eager()
      .projection("n.prop2 as otherValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop as newValue")
      .eager()
      .setProperty("n", "prop", "coalesce(cacheN[n.prop2] + 1, 0)")
      .eager()
      .projection("cacheNFromStore[n.prop2] as otherValue")
      .allNodeScan("n")
      .build()
  }

  test("should use cached properties in expression that is written to different properties - SetProperties") {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "newValue2", "otherValue", "otherValue2")
      .projection("n.prop3 as newValue", "n.prop4 as newValue2")
      .setProperties("n", "prop3" -> "n.prop + 1", "prop4" -> "n.prop2 + 1")
      .projection("n.prop as otherValue", "n.prop2 as otherValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "newValue2", "otherValue", "otherValue2")
      .projection("n.prop3 as newValue", "n.prop4 as newValue2")
      .setProperties("n", "prop3" -> "cacheN[n.prop] + 1", "prop4" -> "cacheN[n.prop2] + 1")
      .projection("cacheNFromStore[n.prop] as otherValue", "cacheNFromStore[n.prop2] as otherValue2")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties in expression that is written to different properties - SetPropertiesFromMap"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "otherValue")
      .projection("n.prop2 as newValue")
      .setPropertiesFromMap("n", "{prop2: n.prop + 1}", removeOtherProps = false)
      .projection("n.prop as otherValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "otherValue")
      .projection("n.prop2 as newValue")
      .setPropertiesFromMap("n", "{prop2: cacheN[n.prop] + 1}", removeOtherProps = false)
      .projection("cacheNFromStore[n.prop] as otherValue")
      .allNodeScan("n")
      .build()
  }

  test("should use cached properties in expression that is written to different property - SetNodeProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "otherValue")
      .projection("n.prop2 as newValue")
      .setNodeProperty("n", "prop2", "n.prop + 1")
      .projection("n.prop as otherValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "otherValue")
      .projection("n.prop2 as newValue")
      .setNodeProperty("n", "prop2", "cacheN[n.prop] + 1")
      .projection("cacheNFromStore[n.prop] as otherValue")
      .allNodeScan("n")
      .build()
  }

  test("should use cached properties in expression that is written to different properties - SetNodeProperties") {
    val builder = new LogicalPlanBuilder()
      .produceResults("otherValue", "otherValue2", "newValue", "newValue2")
      .projection("n.prop3 as newValue", "n.prop4 as newValue2")
      .setNodeProperties("n", "prop3" -> "n.prop + 1", "prop4" -> "n.prop2 + 1")
      .projection("n.prop as otherValue", "n.prop2 as otherValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("otherValue", "otherValue2", "newValue", "newValue2")
      .projection("n.prop3 as newValue", "n.prop4 as newValue2")
      .setNodeProperties("n", "prop3" -> "cacheN[n.prop] + 1", "prop4" -> "cacheN[n.prop2] + 1")
      .projection("cacheNFromStore[n.prop] as otherValue", "cacheNFromStore[n.prop2] as otherValue2")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties in expression that is written to different properties - SetNodePropertiesFromMap"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("newValue", "otherValue")
      .projection("n.prop2 as newValue")
      .setNodePropertiesFromMap("n", "{prop2: n.prop + 1}", removeOtherProps = false)
      .projection("n.prop as otherValue")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("newValue", "otherValue")
      .projection("n.prop2 as newValue")
      .setNodePropertiesFromMap("n", "{prop2: cacheN[n.prop] + 1}", removeOtherProps = false)
      .projection("cacheNFromStore[n.prop] as otherValue")
      .allNodeScan("n")
      .build()
  }

  test("should use cached properties in expression that is written to different property - SetRelationshipProperty") {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop2 as newValue")
      .setRelationshipProperty("r", "prop2", "r.prop + 1")
      .projection("r.prop as otherValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop2 as newValue")
      .setRelationshipProperty("r", "prop2", "cacheR[r.prop] + 1")
      .projection("cacheRFromStore[r.prop] as otherValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties in expression that is written to different properties - SetRelationshipProperties"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop3 as newValue1", "r.prop4 as newValue2")
      .setRelationshipProperties("r", ("prop3", "r.prop1 + 1"), ("prop4", "r.prop2 + 1"))
      .projection("r.prop1 as otherValue1", "r.prop2 as otherValue2")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop3 as newValue1", "r.prop4 as newValue2")
      .setRelationshipProperties("r", ("prop3", "cacheR[r.prop1] + 1"), ("prop4", "cacheR[r.prop2] + 1"))
      .projection("cacheRFromStore[r.prop1] as otherValue1", "cacheRFromStore[r.prop2] as otherValue2")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties in expression that is written to different properties - SetRelationshipPropertiesFromMap"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop2 as newValue")
      .setRelationshipPropertiesFromMap("r", "{prop2: r.prop + 1}", removeOtherProps = false)
      .projection("r.prop as otherValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop2 as newValue")
      .setRelationshipPropertiesFromMap("r", "{prop2: cacheR[r.prop] + 1}", removeOtherProps = false)
      .projection("cacheRFromStore[r.prop] as otherValue")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties in expression that is written to different properties when they overlap with other entries in list - SetNodeProperties"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("n.prop1 as newValue1", "n.prop2 as newValue2")
      .setNodeProperties("n", "prop1" -> "n.prop1 + 1", "prop2" -> "n.prop1 + 1")
      .projection("n.prop1 as otherValue1", "n.prop2 as otherValue2")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheN[n.prop1] as newValue1", "cacheN[n.prop2] as newValue2")
      .setNodeProperties("n", "prop1" -> "n.prop1 + 1", "prop2" -> "cacheN[n.prop1] + 1")
      .projection("cacheNFromStore[n.prop1] as otherValue1", "cacheNFromStore[n.prop2] as otherValue2")
      .allNodeScan("n")
      .build()
  }

  test(
    "should use cached properties in expression that is written to different properties when they overlap with other entries in map - SetRelationshipPropertiesFromMap"
  ) {
    val builder = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("r.prop1 as newValue1", "r.prop2 as newValue2")
      .setRelationshipPropertiesFromMap("r", "{prop1: r.prop1 + 1, prop2 : r.prop1 + 1}", removeOtherProps = false)
      .projection("r.prop1 as otherValue1", "r.prop2 as otherValue2")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("result")
      .projection("cacheR[r.prop1] as newValue1", "cacheR[r.prop2] as newValue2")
      .setRelationshipPropertiesFromMap(
        "r",
        "{prop1: r.prop1 + 1, prop2 : cacheR[r.prop1] + 1}",
        removeOtherProps = false
      )
      .projection("cacheRFromStore[r.prop1] as otherValue1", "cacheRFromStore[r.prop2] as otherValue2")
      .expandAll("(n)-[r]-(m)")
      .allNodeScan("n")
      .build()
  }

  test("union should not cache single access") {
    val builder = new LogicalPlanBuilder()
      .produceResults("prop")
      .union()
      .|.projection("n.prop as prop")
      .|.allNodeScan("n")
      .projection("n.prop as prop")
      .allNodeScan("n")

    val plan = builder.build()
    val (newPlan, _) = replace(plan, builder.getSemanticTable)
    newPlan shouldBe plan
  }

  // For braver programmers
  ignore("union should cache on RHS") {
    val builder = new LogicalPlanBuilder()
      .produceResults("prop")
      .union()
      .|.projection("n.prop as propAgain")
      .|.projection("n.prop as prop")
      .|.allNodeScan("n")
      .projection("n.prop as prop")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("a")
      .union()
      .|.projection("cacheN[n.prop] as propAgain")
      .|.projection("cacheNFromStore[n.prop] as prop")
      .|.allNodeScan("n")
      .projection("n.prop as prop")
      .allNodeScan("n")
      .build()
  }

  test("nested union should cache") {
    val builder = new LogicalPlanBuilder()
      .produceResults("prop", "propAgain")
      .apply()
      .|.union()
      .|.|.projection("n.prop2 as propAgain")
      .|.|.argument("n")
      .|.projection("n.prop as propAgain")
      .|.argument("n")
      .union()
      .|.projection("n.prop as prop")
      .|.allNodeScan("n")
      .projection("n.prop as prop")
      .allNodeScan("n")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("prop", "propAgain")
      .apply()
      .|.union()
      .|.|.projection("n.prop2 as propAgain")
      .|.|.argument("n")
      .|.projection("cacheN[n.prop] as propAgain")
      .|.argument("n")
      .union()
      .|.projection("cacheN[n.prop] as prop")
      .|.allNodeScan("n")
      .projection("cacheNFromStore[n.prop] as prop")
      .allNodeScan("n")
      .build()
  }

  test("should cache properties of returned indexed nodes") {
    val builder = new LogicalPlanBuilder()
      .produceResults("n")
      .nodeIndexOperator("n:L(prop > 123)", getValue = _ => CanGetValue, indexOrder = IndexOrderAscending)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("n")
      .nodeIndexOperator("n:L(prop > 123)", getValue = _ => GetValue, indexOrder = IndexOrderAscending)
      .build()
  }

  test("should cache properties of returned and renamed indexed nodes") {
    val builder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n AS x")
      .nodeIndexOperator("n:L(prop > 123)", getValue = _ => CanGetValue, indexOrder = IndexOrderAscending)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n AS x")
      .nodeIndexOperator("n:L(prop > 123)", getValue = _ => GetValue, indexOrder = IndexOrderAscending)
      .build()
  }

  test("should cache properties of returned and renamed indexed nodes on both sides of a union") {
    val builder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n AS x")
      .union()
      .|.projection("b AS n")
      .|.nodeIndexOperator("b:L(prop < 123)", getValue = _ => CanGetValue, indexOrder = IndexOrderAscending)
      .projection("a AS n")
      .nodeIndexOperator("a:L(prop > 123)", getValue = _ => CanGetValue, indexOrder = IndexOrderDescending)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n AS x")
      .union()
      .|.projection("b AS n")
      .|.nodeIndexOperator("b:L(prop < 123)", getValue = _ => GetValue, indexOrder = IndexOrderAscending)
      .projection("a AS n")
      .nodeIndexOperator("a:L(prop > 123)", getValue = _ => GetValue, indexOrder = IndexOrderDescending)
      .build()
  }

  test("should cache properties of returned and renamed indexed nodes on both sides of a union under apply") {
    val builder = new LogicalPlanBuilder()
      .produceResults("x")
      .apply()
      .|.projection("n AS x")
      .|.union()
      .|.|.projection("b AS n")
      .|.|.nodeIndexOperator(
        "b:L(prop < ???)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderAscending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .|.projection("a AS n")
      .|.nodeIndexOperator(
        "a:L(prop > ???)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderDescending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .allNodeScan("z")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("x")
      .apply()
      .|.projection("n AS x")
      .|.union()
      .|.|.projection("b AS n")
      .|.|.nodeIndexOperator(
        "b:L(prop < ???)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderAscending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .|.projection("a AS n")
      .|.nodeIndexOperator(
        "a:L(prop > ???)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderDescending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .allNodeScan("z")
      .build()
  }

  test("should cache properties of returned indexed nodes if we don't rely on index ordering") {
    val builder = new LogicalPlanBuilder()
      .produceResults("n")
      .nodeIndexOperator("n:L(prop > 123)", getValue = _ => CanGetValue, indexOrder = IndexOrderNone)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("n")
      .nodeIndexOperator("n:L(prop > 123)", getValue = _ => GetValue)
      .build()
  }

  test("should cache properties of returned indexed relationships") {
    val builder = new LogicalPlanBuilder()
      .produceResults("r")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop > 123)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderAscending
      )

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("r")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop > 123)]->(b)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderAscending
      )
      .build()
  }

  test("should cache properties of returned and renamed indexed relationships") {
    val builder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("r AS x")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop > 123)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderAscending
      )

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("x")
      .projection("r AS x")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop > 123)]->(b)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderAscending
      )
      .build()
  }

  test("should cache properties of returned and renamed indexed relationships on both sides of a union") {
    val builder = new LogicalPlanBuilder()
      .produceResults("y")
      .projection("x AS y")
      .union()
      .|.projection("q AS x")
      .|.relationshipIndexOperator(
        "(a)-[q:REL(prop > 123)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderDescending
      )
      .projection("p AS x")
      .relationshipIndexOperator(
        "(a)-[p:REL(prop > 123)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderAscending
      )

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("y")
      .projection("x AS y")
      .union()
      .|.projection("q AS x")
      .|.relationshipIndexOperator(
        "(a)-[q:REL(prop > 123)]->(b)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderDescending
      )
      .projection("p AS x")
      .relationshipIndexOperator(
        "(a)-[p:REL(prop > 123)]->(b)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderAscending
      )
      .build()
  }

  test("should cache properties of returned and renamed indexed relationships on both sides of a union under apply") {
    val builder = new LogicalPlanBuilder()
      .produceResults("y")
      .apply()
      .|.projection("x AS y")
      .|.union()
      .|.|.projection("q AS x")
      .|.|.relationshipIndexOperator(
        "(a)-[q:REL(prop > ???)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderAscending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .|.projection("p AS x")
      .|.relationshipIndexOperator(
        "(a)-[p:REL(prop > ???)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderDescending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .allNodeScan("z")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("y")
      .apply()
      .|.projection("x AS y")
      .|.union()
      .|.|.projection("q AS x")
      .|.|.relationshipIndexOperator(
        "(a)-[q:REL(prop > ???)]->(b)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderAscending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .|.projection("p AS x")
      .|.relationshipIndexOperator(
        "(a)-[p:REL(prop > ???)]->(b)",
        getValue = _ => GetValue,
        indexOrder = IndexOrderDescending,
        argumentIds = Set("z"),
        paramExpr = Some(prop("z", "prop"))
      )
      .allNodeScan("z")
      .build()
  }

  test("should cache properties of returned indexed relationships if we don't rely on index ordering") {
    val builder = new LogicalPlanBuilder()
      .produceResults("r")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop > 123)]->(b)",
        getValue = _ => CanGetValue,
        indexOrder = IndexOrderNone
      )

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)

    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("r")
      .relationshipIndexOperator("(a)-[r:REL(prop > 123)]->(b)", getValue = _ => GetValue)
      .build()
  }

  private def replace(
    plan: LogicalPlan,
    initialTable: SemanticTable,
    effectiveCardinalities: EffectiveCardinalities = new EffectiveCardinalities,
    idGen: IdGen = new SequentialIdGen(),
    pushdownPropertyReads: Boolean = false
  ): (LogicalPlan, SemanticTable) = {
    val state = LogicalPlanState(InitialState("", None, IDPPlannerName, new AnonymousVariableNameGenerator))
      .withSemanticTable(initialTable)
      .withMaybeLogicalPlan(Some(plan))
      .withNewPlanningAttributes(PlanningAttributes.newAttributes.copy(effectiveCardinalities = effectiveCardinalities))

    val icp = new InsertCachedProperties(pushdownPropertyReads = pushdownPropertyReads)

    val config = mock[CypherPlannerConfiguration]
    when(config.propertyCachingMode).thenReturn(PropertyCachingMode.CacheProperties)

    val plannerContext = mock[PlannerContext]
    when(plannerContext.logicalPlanIdGen).thenReturn(idGen)
    when(plannerContext.tracer).thenReturn(NO_TRACING)
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    when(plannerContext.config).thenReturn(config)

    val resultState = icp.transform(state, plannerContext)
    (resultState.logicalPlan, resultState.semanticTable())
  }

  private def semanticTable(types: (Expression, TypeSpec)*): SemanticTable = {
    val mappedTypes = types.map { case (expr, typeSpec) => expr -> ExpressionTypeInfo(typeSpec) }
    SemanticTable(types = ASTAnnotationMap[Expression, ExpressionTypeInfo](mappedTypes: _*))
  }
}
