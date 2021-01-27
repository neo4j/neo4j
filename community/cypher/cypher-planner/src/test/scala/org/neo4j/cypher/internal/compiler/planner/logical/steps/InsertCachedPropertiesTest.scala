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
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
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
  private val x = Variable("x")(InputPosition.NONE)
  private val m = Variable("m")(InputPosition.NONE)
  private val prop = PropertyKeyName("prop")(InputPosition.NONE)
  private val foo = PropertyKeyName("foo")(InputPosition.NONE)
  // Same property in different positions
  private val nProp1 = Property(n, prop)(InputPosition.NONE)
  private val nProp2 = Property(n, prop)(InputPosition.NONE.bumped())
  private val nProp3 = Property(n, prop)(InputPosition.NONE.bumped().bumped())
  // Same property in different positions
  private val nFoo1 = Property(n, foo)(InputPosition.NONE)
  private val cachedNProp1 = CachedProperty("n", n, prop, NODE_TYPE)(InputPosition.NONE)
  private val cachedNFoo1 = CachedProperty("n", n, foo, NODE_TYPE)(InputPosition.NONE)
  private val cachedNProp2 = CachedProperty("n", n, prop, NODE_TYPE)(InputPosition.NONE.bumped())
  private val cachedNProp3 = CachedProperty("n", n, prop, NODE_TYPE)(InputPosition.NONE.bumped().bumped())
  private val cachedNRelProp1 = CachedProperty("n", n, prop, RELATIONSHIP_TYPE)(InputPosition.NONE)
  private val cachedNRelProp2 = CachedProperty("n", n, prop, RELATIONSHIP_TYPE)(InputPosition.NONE.bumped())

  private val xProp = Property(x, prop)(InputPosition.NONE)

  def indexScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): IndexLeafPlan = IndexSeek(s"$node:$label($property)", getValueFromIndex)
  def indexSeek(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): IndexLeafPlan = IndexSeek(s"$node:$label($property = 42)", getValueFromIndex)
  def uniqueIndexSeek(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): IndexLeafPlan = IndexSeek(s"$node:$label($property = 42)", getValueFromIndex, unique = true)
  def indexContainsScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): IndexLeafPlan = IndexSeek("n:Awesome(prop CONTAINS 'foo')", getValueFromIndex)
  def indexEndsWithScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior): IndexLeafPlan = IndexSeek("n:Awesome(prop ENDS WITH 'foo')", getValueFromIndex)

  for((indexOperator, name) <- Seq((indexScan _, "indexScan"), (indexSeek _, "indexSeek"), (uniqueIndexSeek _, "uniqueIndexSeek"), (indexContainsScan _, "indexContainsScan"), (indexEndsWithScan _, "indexEndsWithScan"))) {
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

  test("should not rewrite prop(n, prop) if index cannot get value") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(nProp1, literalInt(1))),
      indexScan("n", "L", "prop", DoNotGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
  }


  test("should set DoNotGetValue if there is no usage of prop(n, prop)") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nFoo1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(nFoo1, literalInt(1))),
      indexScan("n", "L", "prop", CanGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should equal(
      Selection(
        Seq(equals(nFoo1, literalInt(1))),
        indexScan("n", "L", "prop", DoNotGetValue)
      )
    )
    newTable should be(initialTable)
  }

  test("should rewrite prop(n, prop) to CachedProperty(n.prop) with usage in projection after index scan") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Projection(
      indexScan("n", "L", "prop", CanGetValue),
      Map("x" -> nProp1)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Projection(
        indexScan("n", "L", "prop", GetValue),
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
        indexScan("n", "L", "prop", CanGetValue),
        Map("x" -> nFoo1)
      )
    )
    val (newPlan, _) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(cachedNProp1, cachedNFoo1), greaterThan(prop("n", "otherProp"), literalInt(1))),
        Projection(
          indexScan("n", "L", "prop", GetValue),
          Map("x" -> cachedNFoo1.copy(knownToAccessStore = true)(cachedNFoo1.position))
        )
      )
    )
  }

  test("should rewrite [prop(n, prop)] to [CachedProperty(n.prop)] with usage in selection after index scan") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(listOf(prop("n", "prop")), listOfInt(1))),
      indexScan("n", "L", "prop", CanGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(listOf(cachedNProp1), listOfInt(1))),
        indexScan("n", "L", "prop", GetValue)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test("should rewrite {foo: prop(n, prop)} to {foo: CachedProperty(n.prop)} with usage in selection after index scan") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val plan = Selection(
      Seq(equals(mapOf("foo" -> prop("n", "prop")), mapOfInt(("foo", 1)))),
      indexScan("n", "L", "prop", CanGetValue)
    )
    val (newPlan, newTable) = replace(plan, initialTable)

    newPlan should equal(
      Selection(
        Seq(equals(mapOf("foo" -> cachedNProp1), mapOfInt(("foo", 1)))),
        indexScan("n", "L", "prop", GetValue)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test("should not explode on missing type info") {
    val initialTable = semanticTable(nProp1 -> CTInteger, n -> CTNode)
    val propWithoutSemanticType = propEquality("m", "prop", 2)

    val plan = Selection(
      Seq(propEquality("n", "prop", 1), propWithoutSemanticType),
      NodeHashJoin(Set("n"),
        indexScan("n", "L", "prop", CanGetValue),
        indexScan("m", "L", "prop", CanGetValue)
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
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTRelationship)
    val cp1 = cachedNRelProp1.copy(knownToAccessStore = true)(cachedNRelProp1.position)

    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))),
        DirectedRelationshipByIdSeek("n", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
      ),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cp1, literalInt(2))),
          DirectedRelationshipByIdSeek("n", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
        ),
        Map("x" -> cachedNRelProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cp1) should be(initialType)
    newTable.types(cachedNRelProp2) should be(initialType)
  }

  test("should not cache relationship property if there is only one usage") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTRelationship)
    val plan = Projection(
      DirectedRelationshipByIdSeek("n", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(plan)
    newTable should be(initialTable)
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
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .filter("x.prop > 42")
      .projection("n AS x")
      .filter("n.prop > 42")
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
        .filterExpression(greaterThan(cachedNodeProp("n", "prop", "x"), literalInt(42) ))
        .projection("n AS x")
        .filterExpression(greaterThan(cachedNodePropFromStore("n", "prop"), literalInt(42) ))
        .allNodeScan("n")
        .build()
    )
    newTable.types(cachedNodePropFromStore("n", "prop")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "prop", "x")) should be(ExpressionTypeInfo(CTInteger))
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
      .produceResults("`n3.prop`").withCardinality(200)
      .projection("n3.prop as `n3.prop`").withCardinality(200)
      .projection("n2 AS n3").withCardinality(200).newVar("n3", CTNode)
      .apply().withCardinality(200)
      .|.nodeByLabelScan("m", "B").withCardinality(200)
      // Note, push down properties don't keep track of input position in all cases and can produce output like the following
      .cacheProperties(Set[LogicalProperty](Property(Variable("n2")(InputPosition.NONE), PropertyKeyName("prop")(InputPosition.NONE))(InputPosition.NONE)))
      .aggregation(Seq("n as n2"), Seq("count(n) AS count")).withCardinality(3).newVar("n2", n2InputPosition, CTNode)
      .nodeByLabelScan("n", "A").withCardinality(10)

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
      .produceResults("x").withCardinality(50)
      .projection("n.prop AS x").withCardinality(50)
      .expandAll("(n)-->(m)").withCardinality(50)
      .allNodeScan("n").withCardinality(5)

    val (resultPlan, _) = replace(plan.build(), plan.getSemanticTable, plan.cardinalities, plan.idGen, pushdownPropertyReads = true)

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
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .nestedPlanExistsExpressionProjection("list")
      .|.projection("n.prop AS bar")
      .|.projection("n.prop AS foo")
      .|.allNodeScan("n")
      .argument()

    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types = builderTable.types
        .updated(nProp1, ExpressionTypeInfo(CTInteger))
      )

    val (newPlan, newTable) = replace(builder.build(), initialTable)
    newPlan should be(
      new LogicalPlanBuilder(wholePlan = false)
        .nestedPlanExistsExpressionProjection("list")
        .|.projection(Map("bar" -> cachedNProp1))
        .|.projection(Map("foo" -> cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)))
        .|.allNodeScan("n")
        .argument()
        .build()
    )
    newTable.types(cachedNProp1) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)) should be(ExpressionTypeInfo(CTInteger))
  }

  test("should cache in LHS and RHS with Apply") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
      .projection("n.prop AS baz", "n.lt AS bazLT", "n.rt AS bazRT")
      .apply()
      .|.projection("n.prop AS bar", "n.lr AS barLR", "n.rt AS barRT")
      .|.argument("n")
      .projection("n.prop AS foo", "n.lr AS fooLR", "n.lt AS fooLT")
      .allNodeScan("n")

    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types = builderTable.types
        .updated(nProp1, ExpressionTypeInfo(CTInteger))
        .updated(prop("n", "lt"), ExpressionTypeInfo(CTInteger))
        .updated(prop("n", "lr"), ExpressionTypeInfo(CTInteger))
        .updated(prop("n", "rt"), ExpressionTypeInfo(CTInteger))
      )

    val (newPlan, newTable) = replace(builder.build(), initialTable)
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
    newTable.types(cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNProp1) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodePropFromStore("n", "lt")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "lt")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodePropFromStore("n", "lr")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "lr")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodePropFromStore("n", "rt")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "rt")) should be(ExpressionTypeInfo(CTInteger))
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

    val builderTable = builder.getSemanticTable
    val initialTable = builderTable
      .copy(types = builderTable.types
        .updated(nProp1, ExpressionTypeInfo(CTInteger))
        .updated(prop("n", "lt"), ExpressionTypeInfo(CTInteger))
        .updated(prop("n", "lr"), ExpressionTypeInfo(CTInteger))
        .updated(prop("n", "rt"), ExpressionTypeInfo(CTInteger))
      )

    val (newPlan, newTable) = replace(builder.build(), initialTable)
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
    newTable.types(cachedNProp1.copy(knownToAccessStore = true)(cachedNProp1.position)) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNProp1) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodePropFromStore("n", "lt")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "lt")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodePropFromStore("n", "lr")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "lr")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodePropFromStore("n", "rt")) should be(ExpressionTypeInfo(CTInteger))
    newTable.types(cachedNodeProp("n", "rt")) should be(ExpressionTypeInfo(CTInteger))
  }

  private def replace(plan: LogicalPlan,
                      initialTable: SemanticTable,
                      cardinalities: Cardinalities = new Cardinalities,
                      idGen: IdGen = new SequentialIdGen(),
                      pushdownPropertyReads: Boolean = false): (LogicalPlan, SemanticTable) = {
    val state = LogicalPlanState(InitialState("", None, IDPPlannerName))
      .withSemanticTable(initialTable)
      .withMaybeLogicalPlan(Some(plan))
      .withNewPlanningAttributes(PlanningAttributes.newAttributes.copy(cardinalities = cardinalities))

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

    val resultState =  icp.transform(state, plannerContext)
    (resultState.logicalPlan, resultState.semanticTable())
  }

  private def semanticTable(types:(Expression, TypeSpec)*): SemanticTable = {
    val mappedTypes = types.map { case(expr, typeSpec) => expr -> ExpressionTypeInfo(typeSpec)}
    SemanticTable(types = ASTAnnotationMap[Expression, ExpressionTypeInfo](mappedTypes:_*))
  }
}
