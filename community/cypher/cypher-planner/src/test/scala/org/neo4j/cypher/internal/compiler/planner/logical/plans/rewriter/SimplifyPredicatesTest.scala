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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SimplifyPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should rewrite WHERE x.prop in [1] to WHERE x.prop = 1") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val predicate = in(prop("x", "prop"), listOfInt(1))
    val cleanPredicate = propEquality("x", "prop", 1)
    val selection = Selection(Seq(predicate), argument)

    selection.endoRewrite(simplifyPredicates) should equal(
      Selection(Seq(cleanPredicate), argument)
    )
  }

  test("should not rewrite WHERE x.prop in [1, 2]") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val orgPredicate = in(prop("x", "prop"), listOfInt(1, 2))
    val selection = Selection(Seq(orgPredicate), argument)

    selection.endoRewrite(simplifyPredicates) should equal(selection)
  }

  test("should rewrite WHERE AndedPropertyInequality(x.prop, 1) to WHERE x.prop > 42") {
    val argument: LogicalPlan = Argument(Set(v"x"))
    val predicate = propGreaterThan("x", "prop", 42)
    val complexForm = AndedPropertyInequalities(v"x", prop("x", "prop"), NonEmptyList(predicate))
    val selection = Selection(Seq(complexForm), argument)
    val expectedSelection = Selection(Seq(predicate), argument)

    selection.endoRewrite(simplifyPredicates) should equal(expectedSelection)
  }

  test("should rewrite WHERE AndedPropertyInequality into multiple predicates") {
    val argument: LogicalPlan = Argument(Set(v"x"))
    val predicateGT = propGreaterThan("x", "prop", 42)
    val predicateLT = propLessThan("x", "prop", 321)
    val complexForm = AndedPropertyInequalities(v"x", prop("x", "prop"), NonEmptyList(predicateGT, predicateLT))
    val selection = Selection(Seq(complexForm), argument)
    val expectedSelection = Selection(Seq(predicateGT, predicateLT), argument)

    selection.endoRewrite(simplifyPredicates) should equal(expectedSelection)
  }

  test("should rewrite WHERE x.prop in $autoList to WHERE x.prop = $autoList[0] if size is 1") {
    val argument: LogicalPlan = Argument(Set(v"a"))

    val autoParamList0 = autoParameter("autoList", CTList(CTInteger), Some(0))
    val autoParamList1 = autoParameter("autoList", CTList(CTInteger), Some(1))
    val autoParamList10 = autoParameter("autoList", CTList(CTInteger), Some(10))
    val autoParamListUnknown = autoParameter("autoList", CTList(CTInteger), sizeHint = None)
    val autoParamString = autoParameter("autoList", CTString, Some(1))

    // should rewrite
    Selection(Seq(in(prop("x", "prop"), autoParamList1)), argument).endoRewrite(simplifyPredicates) should equal(
      Selection(Seq(propEquality("x", "prop", containerIndex(autoParamList1, 0))), argument)
    )

    // should not rewrite
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), autoParamList0)), argument))
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), autoParamList10)), argument))
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), autoParamListUnknown)), argument))
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), autoParamString)), argument))
  }

  test("should rewrite WHERE x.prop in $p to WHERE x.prop = $p[0] if size is 1") {
    val argument: LogicalPlan = Argument(Set(v"a"))

    val paramList0 = parameter("p", CTList(CTInteger), Some(0))
    val paramList1 = parameter("p", CTList(CTInteger), Some(1))
    val paramList10 = parameter("p", CTList(CTInteger), Some(10))
    val paramListUnknown = parameter("p", CTList(CTInteger), sizeHint = None)
    val paramString = parameter("p", CTString, Some(1))

    // should rewrite
    Selection(Seq(in(prop("x", "prop"), paramList1)), argument).endoRewrite(simplifyPredicates) should equal(
      Selection(Seq(propEquality("x", "prop", containerIndex(paramList1, 0))), argument)
    )

    // should not rewrite
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), paramList0)), argument))
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), paramList10)), argument))
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), paramListUnknown)), argument))
    shouldNotRewrite(Selection(Seq(in(prop("x", "prop"), paramString)), argument))
  }

  private def shouldNotRewrite(plan: LogicalPlan): Unit = {
    plan.endoRewrite(simplifyPredicates) should equal(plan)
  }
}
