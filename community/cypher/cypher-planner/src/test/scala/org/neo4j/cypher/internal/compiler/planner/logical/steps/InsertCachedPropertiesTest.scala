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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

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
  private val rProp3 = Property(r, prop)(InputPosition(10, 11, 12))
  // Same property in different positions
  private val nFoo1 = Property(n, foo)(InputPosition.NONE)
  private val cachedNProp1 = CachedProperty("n", n, prop, NODE_TYPE)(nProp1.position)
  private val cachedNHasProp1 = CachedHasProperty("n", n, prop, NODE_TYPE)(nProp1.position)
  private val cachedNFoo1 = CachedProperty("n", n, foo, NODE_TYPE)(nFoo1.position)
  private val cachedNProp2 = CachedProperty("n", n, prop, NODE_TYPE)(nProp2.position)
  private val cachedNHasProp2 = CachedHasProperty("n", n, prop, NODE_TYPE)(nProp2.position)
  private val cachedNProp3 = CachedProperty("n", n, prop, NODE_TYPE)(nProp3.position)
  private val cachedRRelProp1 = CachedProperty("r", r, prop, RELATIONSHIP_TYPE)(rProp1.position)
  private val cachedRRelHasProp1 = CachedHasProperty("r", r, prop, RELATIONSHIP_TYPE)(rProp1.position)
  private val cachedRRelProp2 = CachedProperty("r", r, prop, RELATIONSHIP_TYPE)(rProp2.position)
  private val cachedRRelHasProp2 = CachedHasProperty("r", r, prop, RELATIONSHIP_TYPE)(rProp2.position)

  private val xProp = Property(x, prop)(InputPosition.NONE)

  def nodeIndexScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek(s"$node:$label($property)", _ => getValueFromIndex)
  def nodeIndexSeek(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek(s"$node:$label($property = 42)", _ => getValueFromIndex)
  def nodeUniqueIndexSeek(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek(s"$node:$label($property = 42)", _ => getValueFromIndex, unique = true)
  def nodeIndexContainsScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek("n:Awesome(prop CONTAINS 'foo')", _ => getValueFromIndex)
  def nodeIndexEndsWithScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): NodeIndexLeafPlan = IndexSeek.nodeIndexSeek("n:Awesome(prop ENDS WITH 'foo')", _ => getValueFromIndex)

  def relationshipIndexScan(rel: String, typ: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): RelationshipIndexLeafPlan = IndexSeek.relationshipIndexSeek(s"(x)-[$rel:$typ($property)]->(y)", _ => getValueFromIndex)

  for((indexOperator, name) <- Seq((nodeIndexScan _, "indexScan"),
                                  (nodeIndexSeek _, "indexSeek"),
                                  (nodeUniqueIndexSeek _, "uniqueIndexSeek"),
                                  (nodeIndexContainsScan _, "indexContainsScan"),
                                  (nodeIndexEndsWithScan _, "indexEndsWithScan"))) {
    test(s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in selection after index operator: $name") {
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

    test(s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in projection after index operator: $name") {
      val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
      val plan = Projection(
        indexOperator("n", "L", "prop", CanGetValue),
        Map("x" -> nProp1),
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          indexOperator("n", "L", "prop", GetValue),
          Map("x" -> cachedNProp1),
        )
      )
      val initialType = initialTable.types(nProp1)
      newTable.types(cachedNProp1) should be(initialType)
    }

    test(s"multiple usages after index should not be knownToAccessStore: $name") {
      val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
      val plan = Projection(
        Selection(Seq(equals(nProp1, literalInt(2))),
          indexOperator("n", "L", "prop", CanGetValue)),
        Map("x" -> nProp2)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          Selection(
            Seq(equals(cachedNProp1, literalInt(2))),
            indexOperator("n", "L", "prop", GetValue)
          ),
          Map("x" -> cachedNProp2))
      )

      val initialType = initialTable.types(nProp1)
      newTable.types(cachedNProp1) should be(initialType)
      newTable.types(cachedNProp2) should be(initialType)
    }
  }

  for((indexOperator, name) <- Seq((relationshipIndexScan _, "indexScan"))) {
    test(s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in selection after relationship index operator: $name") {
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

    test(s"should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in projection after relationship index operator: $name") {
      val initialTable = semanticTable(rProp1 -> CTInteger, r -> CTRelationship)
      val plan = Projection(
        indexOperator("r", "L", "prop", CanGetValue),
        Map("x" -> rProp1),
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          indexOperator("r", "L", "prop", GetValue),
          Map("x" -> cachedRRelProp1),
        )
      )
      val initialType = initialTable.types(rProp1)
      newTable.types(cachedRRelProp1) should be(initialType)
    }

    test(s"multiple usages after relationship index should not be knownToAccessStore: $name") {
      val initialTable = semanticTable(rProp1 -> CTInteger, rProp2 -> CTInteger, r -> CTNode)
      val plan = Projection(
        Selection(Seq(equals(rProp1, literalInt(2))),
          indexOperator("r", "L", "prop", CanGetValue)),
        Map("x" -> rProp2)
      )
      val (newPlan, newTable) = replace(plan, initialTable)

      newPlan should equal(
        Projection(
          Selection(
            Seq(equals(cachedRRelProp1, literalInt(2))),
            indexOperator("r", "L", "prop", GetValue)
          ),
          Map("x" -> cachedRRelProp2))
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
      Map("x" -> nProp1)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Projection(
        nodeIndexScan("n", "L", "prop", GetValue),
        Map("x" -> cachedNProp1)
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
        Map("x" -> nFoo1)
      )
    )
    val (newPlan, _) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(cachedNProp1, cachedNFoo1), greaterThan(prop("n", "otherProp"), literalInt(1))),
        Projection(
          nodeIndexScan("n", "L", "prop", GetValue),
          Map("x" -> cachedNFoo1.copy(knownToAccessStore = true)(cachedNFoo1.position))
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

  test("should rewrite {foo: prop(n, prop)} to {foo: CachedProperty(n.prop)} with usage in selection after index scan") {
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
      NodeHashJoin(Set("n"),
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
      Selection(Seq(equals(nProp1, literalInt(2))),
        AllNodesScan("n", Set.empty)),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cp1, literalInt(2))),
          AllNodesScan("n", Set.empty)),
        Map("x" -> cachedNProp2)
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
      Selection(Seq(isNotNull(nProp1)),
        AllNodesScan("n", Set.empty)),
      Map("x" -> isNotNull(nProp2))
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(isNotNull(cp1)),
          AllNodesScan("n", Set.empty)),
        Map("x" -> isNotNull(cachedNHasProp2))
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
      Selection(Seq(isNotNull(nProp1)),
        AllNodesScan("n", Set.empty)),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(isNotNull(cp1)),
          AllNodesScan("n", Set.empty)),
        Map("x" -> cachedNProp2)
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
      Selection(Seq(equals(nProp1, literalInt(2)), greaterThan(nProp2, literalInt(1))),
        AllNodesScan("n", Set.empty)),
      Map("x" -> nProp3)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cp1, literalInt(2)), greaterThan(cp1, literalInt(1))),
          AllNodesScan("n", Set.empty)),
        Map("x" -> cachedNProp3)
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
    newPlan should be (plan)
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
      .projection(Map("z" -> CachedProperty("n", m, prop, NODE_TYPE)(InputPosition.NONE)))
      .apply()
      .|.union()
      .|.|.projection("n as m")
      .|.|.filter("cacheFromStore[n.prop] = 2")
      .|.|.argument("n")
      .|.projection("n as m")
      .|.filter("cache[n.prop] = 3")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    newPlan should be(expectedPlan)
  }

  test("should not rewrite node property if there is only one usage") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nFoo1 -> CTInteger, n -> CTNode)
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))),
        AllNodesScan("n", Set.empty)),
      Map("x" -> nFoo1)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should cache relationship property on multiple usages") {
    val initialTable = semanticTable(rProp1 -> CTInteger, rProp2 -> CTInteger, r -> CTRelationship)
    val cp1 = cachedRRelProp1.copy(knownToAccessStore = true)(cachedRRelProp1.position)

    val plan = Projection(
      Selection(Seq(equals(rProp1, literalInt(2))),
        DirectedRelationshipByIdSeek("r", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
      ),
      Map("x" -> rProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cp1, literalInt(2))),
          DirectedRelationshipByIdSeek("r", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
        ),
        Map("x" -> cachedRRelProp2)
      )
    )
    val initialType = initialTable.types(rProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedRRelProp2) should be(initialType)
  }

  test("should not cache relationship property if there is only one usage") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTRelationship)
    val plan = Projection(
      DirectedRelationshipByIdSeek("r", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should cache relationship has property on multiple usages") {
    val initialTable = semanticTable(rProp1 -> CTInteger, rProp2 -> CTInteger, r -> CTRelationship)
    val cp1 = cachedRRelHasProp1.copy(knownToAccessStore = true)(cachedRRelProp1.position)

    val plan = Projection(
      Selection(Seq(isNotNull(rProp1)),
        DirectedRelationshipByIdSeek("r", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
      ),
      Map("x" -> isNotNull(rProp2))
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(isNotNull(cp1)),
          DirectedRelationshipByIdSeek("r", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
        ),
        Map("x" -> isNotNull(cachedRRelHasProp2))
      )
    )
    val initialType = initialTable.types(rProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedRRelHasProp2) should be(initialType)
  }

  test("should not do anything with map properties") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTMap)
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))),
        Argument(Set("n"))),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }

  test("should not do anything with untyped variables") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger)
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))),
        Argument(Set("n"))),
      Map("x" -> nProp2)
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
          Selection(Seq(equals(nProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("x" -> n)),
        Map("xProp" -> xProp)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Projection(
          Selection(Seq(equals(cp1, literalInt(2))),
            Argument(Set("n"))),
          Map("x" -> n)),
        Map("xProp" -> cachedNProp1.copy(entityVariable = n.copy("x")(n.position))(cachedNProp1.position))
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

    builder.newNode(varFor("x"))
    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types = builderTable.types
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
          Selection(Seq(equals(nProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("n" -> n)),
        Map("nProp" -> nProp2)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Projection(
          Selection(Seq(equals(cp1, literalInt(2))),
            Argument(Set("n"))),
          Map("n" -> n)),
        Map("nProp" -> cachedNProp2)
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
          Argument(Set("n")),
          Map("m" -> n)),
        Map("n" -> m, "p" -> nProp1)
      )

    an[IllegalStateException] should be thrownBy {
      replace(plan, initialTable)
    }
  }

  test("should throw if there is a short cycle in name definitions in one projection") {
    val initialTable = semanticTable(n -> CTNode, m -> CTNode, nProp1 -> CTInteger)
    val plan =
      Projection(
        Argument(Set("n")),
        Map("m" -> n, "n" -> m, "p" -> nProp1)
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
          Argument(Set("n")),
          Map("m" -> n)),
        Map("x" -> m)),
        Map("n" -> x, "p" -> nProp1)
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
          Selection(Seq(equals(nProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("n" -> n),
          Map("count(1)" -> count(literalInt(1)))),
        Map("nProp" -> nProp2)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Aggregation(
          Selection(Seq(equals(cp1, literalInt(2))),
            Argument(Set("n"))),
          Map("n" -> n),
          Map("count(1)" -> count(literalInt(1)))),
        Map("nProp" -> cachedNProp2)
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
          Selection(Seq(equals(nProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("x" -> n),
          Map("count(1)" -> count(literalInt(1)))),
        Map("xProp" -> xProp)
      )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Aggregation(
          Selection(Seq(equals(cp1, literalInt(2))),
            Argument(Set("n"))),
          Map("x" -> n),
          Map("count(1)" -> count(literalInt(1)))),
        Map("xProp" -> cachedNProp1.copy(entityVariable = n.copy("x")(n.position))(cachedNProp1.position))
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
      // Note, push down properties don't keep track of input position in all cases and can produce output like the following
      .cacheProperties(Set[LogicalProperty](Property(Variable("n2")(InputPosition.NONE), PropertyKeyName("prop")(InputPosition.NONE))(InputPosition.NONE)))
      .aggregation(Seq("n as n2"), Seq("count(n) AS count")).withEffectiveCardinality(3).newVar("n2", n2InputPosition, CTNode)
      .nodeByLabelScan("n", "A").withEffectiveCardinality(10)

    val (resultPlan, _) = replace(plan.build(), plan.getSemanticTable)
    resultPlan shouldBe
      new LogicalPlanBuilder()
        .produceResults("`n3.prop`")
        .projection(Map("n3.prop" -> CachedProperty("n", Variable("n3")(n2InputPosition), PropertyKeyName("prop")(InputPosition.NONE), NODE_TYPE)(InputPosition.NONE)))
        .projection("n2 AS n3")
        .apply()
        .|.nodeByLabelScan("m", "B")
        .cacheProperties(Set[LogicalProperty](CachedProperty("n", Variable("n2")(n2InputPosition), PropertyKeyName("prop")(InputPosition.NONE), NODE_TYPE, knownToAccessStore = true)(InputPosition.NONE)))
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

    val (resultPlan, _) = replace(plan.build(), plan.getSemanticTable, plan.effectiveCardinalities, plan.idGen, pushdownPropertyReads = true)

    resultPlan shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("cache[n.prop] AS x")
        .expandAll("(n)-->(m)")
        .cacheProperties(Set[LogicalProperty](CachedProperty("n", Variable("n")(InputPosition.NONE), PropertyKeyName("prop")(InputPosition.NONE), NODE_TYPE, knownToAccessStore = true)(InputPosition.NONE)))
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
      .copy(types = builderTable.types
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
        .projection(Map("baz" -> cachedNProp1, "bazLT" -> cachedNodeProp("n", "lt"), "bazRT" -> cachedNodeProp("n", "rt")))
        .apply()
        .|.projection(Map("bar" -> cachedNProp1, "barLR" -> cachedNodeProp("n", "lr"), "barRT" -> cachedNodePropFromStore("n", "rt")))
        .|.argument("n")
        .projection(Map("foo" -> cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position), "fooLR" -> cachedNodePropFromStore("n", "lr"), "fooLT" -> cachedNodePropFromStore("n", "lt")))
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
      .copy(types = builderTable.types
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
        .projection(Map("baz" -> cachedNProp1, "bazLT" -> cachedNodeProp("n", "lt"), "bazRT" -> cachedNodeProp("n", "rt")))
        .cartesianProduct()
        .|.projection(Map("bar" -> cachedNProp1, "barLR" -> cachedNodeProp("n", "lr"), "barRT" -> cachedNodePropFromStore("n", "rt")))
        .|.allNodeScan("n")
        .projection(Map("foo" -> cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position), "fooLR" -> cachedNodePropFromStore("n", "lr"), "fooLT" -> cachedNodePropFromStore("n", "lt")))
        .allNodeScan("n")
        .build()
    )
  }

  test("should cache on the RHS of a nested index join") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .projection("a.prop AS `a.prop`")
      .apply()
      .|.nodeIndexOperator("b:B(prop = ???)", argumentIds = Set("a"), paramExpr = Some(prop("a", "prop")))
      .nodeIndexOperator("a:A(prop > 'foo')")

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder(wholePlan = false)
      .projection("cacheN[a.prop] AS `a.prop`")
      .apply()
      .|.nodeIndexOperator("b:B(prop = ???)", argumentIds = Set("a"), paramExpr = Some(cachedNodePropFromStore("a", "prop")))
      .nodeIndexOperator("a:A(prop > 'foo')")
      .build()
  }

  test("should find property usage in value hash join") {
    val builder = new LogicalPlanBuilder()
      .produceResults("a", "b")
      .valueHashJoin("a.x = b.y")
      .|.nodeIndexOperator("b:B(y < 200)", getValue = _ => CanGetValue)
      .nodeIndexOperator("a:A(x > 100)", getValue = _ => CanGetValue)

    val (newPlan, _) = replace(builder.build(), builder.getSemanticTable)
    newPlan shouldBe new LogicalPlanBuilder()
      .produceResults("a", "b")
      .valueHashJoin("cacheN[a.x] = cacheN[b.y]")
      .|.nodeIndexOperator("b:B(y < 200)", getValue = _ => GetValue)
      .nodeIndexOperator("a:A(x > 100)", getValue = _ => GetValue)
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

  private def replace(plan: LogicalPlan,
                      initialTable: SemanticTable,
                      effectiveCardinalities: EffectiveCardinalities = new EffectiveCardinalities,
                      idGen: IdGen = new SequentialIdGen(),
                      pushdownPropertyReads: Boolean = false): (LogicalPlan, SemanticTable) = {
    val state = LogicalPlanState(InitialState("", None, IDPPlannerName, new AnonymousVariableNameGenerator))
      .withSemanticTable(initialTable)
      .withMaybeLogicalPlan(Some(plan))
      .withNewPlanningAttributes(PlanningAttributes.newAttributes.copy(effectiveCardinalities = effectiveCardinalities))

    val icp = new InsertCachedProperties(pushdownPropertyReads = pushdownPropertyReads) {
      // Override so that we do not have to provide so many mocks.
      override protected[steps] def resortSelectionPredicates(from: LogicalPlanState,
                                                              context: PlannerContext,
                                                              s: Selection): Seq[Expression] = {
        s.predicate.exprs.sortBy(_.treeCount { case _: Property => true })
      }
    }

    val plannerContext = mock[PlannerContext]
    when(plannerContext.logicalPlanIdGen).thenReturn(idGen)
    when(plannerContext.tracer).thenReturn(NO_TRACING)

    val resultState =  icp.transform(state, plannerContext)
    (resultState.logicalPlan, resultState.semanticTable())
  }

  private def semanticTable(types:(Expression, TypeSpec)*): SemanticTable = {
    val mappedTypes = types.map { case(expr, typeSpec) => expr -> ExpressionTypeInfo(typeSpec)}
    SemanticTable(types = ASTAnnotationMap[Expression, ExpressionTypeInfo](mappedTypes:_*))
  }
}
