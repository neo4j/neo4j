/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext}
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.logical.plans.{Aggregation, AllNodesScan, Argument, CanGetValue, DirectedRelationshipByIdSeek, DoNotGetValue, GetValue, GetValueFromIndexBehavior, IndexSeek, LogicalPlan, NodeHashJoin, Projection, Selection, SingleSeekableArg}
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.frontend.phases.InitialState
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.InputPosition

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
  // Same property in different positions
  private val nFoo1 = Property(n, foo)(InputPosition.NONE)
  private val cachedNProp1 = CachedProperty("n", n, prop, NODE_TYPE)(InputPosition.NONE)
  private val cachedNProp2 = CachedProperty("n", n, prop, NODE_TYPE)(InputPosition.NONE.bumped())
  private val cachedNRelProp1 = CachedProperty("n", n, prop, RELATIONSHIP_TYPE)(InputPosition.NONE)
  private val cachedNRelProp2 = CachedProperty("n", n, prop, RELATIONSHIP_TYPE)(InputPosition.NONE.bumped())

  private val xProp = Property(x, prop)(InputPosition.NONE)

  def indexScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior) = IndexSeek(s"$node:$label($property)", getValueFromIndex)
  def indexSeek(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior) = IndexSeek(s"$node:$label($property = 42)", getValueFromIndex)
  def uniqueIndexSeek(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior) = IndexSeek(s"$node:$label($property = 42)", getValueFromIndex, unique = true)
  def indexContainsScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior) = IndexSeek("n:Awesome(prop CONTAINS 'foo')", getValueFromIndex)
  def indexEndsWithScan(node: String, label: String, property: String, getValueFromIndex: GetValueFromIndexBehavior) = IndexSeek("n:Awesome(prop ENDS WITH 'foo')", getValueFromIndex)

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
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))),
        AllNodesScan("n", Set.empty)),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cachedNProp1, literalInt(2))),
          AllNodesScan("n", Set.empty)),
        Map("x" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
    newTable.types(cachedNProp2) should be(initialType)
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
    val plan = Projection(
      Selection(Seq(equals(nProp1, literalInt(2))),
        DirectedRelationshipByIdSeek("n", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
      ),
      Map("x" -> nProp2)
    )

    val (newPlan, newTable) = replace(plan, initialTable)
    newPlan should be(
      Projection(
        Selection(Seq(equals(cachedNRelProp1, literalInt(2))),
          DirectedRelationshipByIdSeek("n", SingleSeekableArg(literalInt(25)), "a", "b", Set.empty)
        ),
        Map("x" -> cachedNRelProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNRelProp1) should be(initialType)
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
          Selection(Seq(equals(cachedNProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("x" -> n)),
        Map("xProp" -> cachedNProp1.copy(entityVariable = n.copy("x")(n.position))(cachedNProp1.position))
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  test("renamed variable: n AS n") {
    val initialTable = semanticTable(nProp1 -> CTInteger, xProp -> CTInteger, n -> CTNode, x -> CTNode)
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
          Selection(Seq(equals(cachedNProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("n" -> n)),
        Map("nProp" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  // More complex renamings are affected by the Namespacer
  // A test for that can be found in CachedPropertyAcceptanceTest and CachedPropertiesPlanningIntegrationTest

  test("aggregation") {
    val initialTable = semanticTable(nProp1 -> CTInteger, nProp2 -> CTInteger, n -> CTNode)
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
          Selection(Seq(equals(cachedNProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("n" -> n),
          Map("count(1)" -> count(literalInt(1)))),
        Map("nProp" -> cachedNProp2)
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
    newTable.types(cachedNProp2) should be(initialType)
  }

  test("aggregation with renaming") {
    val initialTable = semanticTable(nProp1 -> CTInteger, xProp -> CTInteger, n -> CTNode, x -> CTNode)
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
          Selection(Seq(equals(cachedNProp1, literalInt(2))),
            Argument(Set("n"))),
          Map("x" -> n),
          Map("count(1)" -> count(literalInt(1)))),
        Map("xProp" -> cachedNProp1.copy(entityVariable = n.copy("x")(n.position))(cachedNProp1.position))
      )
    )
    val initialType = initialTable.types(nProp1)
    newTable.types(cachedNProp1) should be(initialType)
  }

  private def replace(plan: LogicalPlan, initialTable: SemanticTable): (LogicalPlan, SemanticTable) = {
    val state = LogicalPlanState(InitialState("", None, IDPPlannerName)).withSemanticTable(initialTable).withMaybeLogicalPlan(Some(plan))
    val resultState = InsertCachedProperties(pushdownPropertyReads = false).transform(state, mock[PlannerContext])
    (resultState.logicalPlan, resultState.semanticTable())
  }

  private def semanticTable(types:(Expression, TypeSpec)*): SemanticTable = {
    val mappedTypes = types.map { case(expr, typeSpec) => expr -> ExpressionTypeInfo(typeSpec)}
    SemanticTable(types = ASTAnnotationMap[Expression, ExpressionTypeInfo](mappedTypes:_*))
  }
}
