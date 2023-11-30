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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leverageOrder.OrderToLeverageWithAliases
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.SortedMap

class LeverageOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should leverage ASC order for exact match with grouping column") {
    val po = ProvidedOrder.asc(varFor("a"))
    val grouping = Map[LogicalVariable, Expression](v"newA" -> varFor("a"))
    leverageOrder(po, grouping, Set.empty) should be(OrderToLeverageWithAliases(Seq(varFor("a")), grouping))
  }

  test("should leverage DESC order for exact match with grouping column") {
    val po = ProvidedOrder.desc(varFor("a"))
    val grouping = Map[LogicalVariable, Expression](v"newA" -> varFor("a"))
    leverageOrder(po, grouping, Set.empty) should be(OrderToLeverageWithAliases(Seq(varFor("a")), grouping))
  }

  test("should leverage order for prefix match with grouping column") {
    val po = ProvidedOrder.asc(varFor("a")).desc(varFor("b"))
    val grouping = Map[LogicalVariable, Expression](v"newA" -> varFor("a"))
    leverageOrder(po, grouping, Set.empty) should be(OrderToLeverageWithAliases(Seq(varFor("a")), grouping))
  }

  test("should leverage order for exact match with one of grouping columns") {
    val po = ProvidedOrder.asc(varFor("a"))
    val grouping = Map[LogicalVariable, Expression](v"newA" -> varFor("a"), v"newB" -> varFor("b"))
    leverageOrder(po, grouping, Set.empty) should be(OrderToLeverageWithAliases(Seq(varFor("a")), grouping))
  }

  test("should leverage order for prefix match with one of grouping columns") {
    val po = ProvidedOrder.asc(varFor("a")).desc(varFor("b"))
    val grouping = Map[LogicalVariable, Expression](v"newA" -> varFor("a"), v"newC" -> varFor("c"))
    leverageOrder(po, grouping, Set.empty) should be(OrderToLeverageWithAliases(Seq(varFor("a")), grouping))
  }

  test("should leverage order for prefix match with one of grouping columns as prefix and one as suffix") {
    val po = ProvidedOrder.asc(varFor("a")).desc(varFor("b")).asc(varFor("c"))
    val grouping = Map[LogicalVariable, Expression](v"newA" -> varFor("a"), v"newC" -> varFor("c"))
    leverageOrder(po, grouping, Set.empty) should be(OrderToLeverageWithAliases(Seq(varFor("a")), grouping))
  }

  test("should alias expressions if there are symbols available") {
    val po = ProvidedOrder.asc(prop("a", "prop"))
    val grouping = Map[LogicalVariable, Expression](v"aprop" -> prop("a", "prop"))
    val aliasedOrder = Seq(varFor("aprop"))
    val aliasedGroupings = Map[LogicalVariable, Expression](v"aprop" -> varFor("aprop"))
    leverageOrder(po, grouping, Set(v"aprop")) should be(OrderToLeverageWithAliases(aliasedOrder, aliasedGroupings))
  }

  test("should alias multiple identical expressions if there are symbols available ASC") {
    val po = ProvidedOrder.asc(prop("a", "prop"))
    val grouping = Map[LogicalVariable, Expression](v"aprop" -> prop("a", "prop"), v"xxx" -> prop("a", "prop"))
    val aliasedOrder = Seq(varFor("aprop"))
    val aliasedGroupings = Map[LogicalVariable, Expression](v"aprop" -> varFor("aprop"), v"xxx" -> varFor("aprop"))
    leverageOrder(po, grouping, Set(v"aprop")) should be(OrderToLeverageWithAliases(aliasedOrder, aliasedGroupings))
  }

  test("should alias multiple identical expressions if there are symbols available DESC") {
    val po = ProvidedOrder.desc(prop("a", "prop"))
    val grouping = Map[LogicalVariable, Expression](v"aprop" -> prop("a", "prop"), v"xxx" -> prop("a", "prop"))
    val aliasedOrder = Seq(varFor("aprop"))
    val aliasedGroupings = Map[LogicalVariable, Expression](v"aprop" -> varFor("aprop"), v"xxx" -> varFor("aprop"))
    leverageOrder(po, grouping, Set(v"aprop")) should be(OrderToLeverageWithAliases(aliasedOrder, aliasedGroupings))
  }

  test("should use the instances from the grouping expressions - not the instances from the provided order") {
    val providedOrderInstance = varFor("a")
    val groupingInstance = varFor("a")
    val po = ProvidedOrder.asc(providedOrderInstance)
    val grouping = Map[LogicalVariable, Expression](v"newA" -> groupingInstance)

    leverageOrder(po, grouping, Set.empty) match {
      case OrderToLeverageWithAliases(Seq(v), _) =>
        v should be theSameInstanceAs (groupingInstance)
      case order => throw new IllegalArgumentException(s"Unexpected order: $order")
    }
  }

  test("should not rewrite grouping columns with variables as projected expressions") {
    for (stringOrdering <- Seq(Ordering.String, Ordering.String.reverse)) {
      val mapOrdering = Ordering.by[LogicalVariable, String](_.name)(stringOrdering)

      val aVar = varFor("a")
      // WITH a, a AS b, a AS c
      val grouping = SortedMap[LogicalVariable, Expression](v"a" -> aVar, v"b" -> aVar, v"c" -> aVar)(mapOrdering)
      val providedOrder = ProvidedOrder.empty
      val res = leverageOrder(providedOrder, grouping, Set(v"a", v"b", v"c"))
      res shouldBe OrderToLeverageWithAliases(Seq.empty, grouping)
    }
  }

  test("should leverage aliased provided order column") {
    for (stringOrdering <- Seq(Ordering.String, Ordering.String.reverse)) {
      val mapOrdering = Ordering.by[LogicalVariable, String](_.name)(stringOrdering)

      val aVar = varFor("a")
      val bVar = varFor("b")
      // WITH a, a AS b, a AS c
      val grouping = SortedMap[LogicalVariable, Expression](v"a" -> aVar, v"b" -> aVar, v"c" -> aVar)(mapOrdering)
      val providedOrder = ProvidedOrder.asc(bVar)
      val res = leverageOrder(providedOrder, grouping, Set(v"a", v"b", v"c"))
      res shouldBe OrderToLeverageWithAliases(Seq(aVar), grouping)
    }
  }
}
