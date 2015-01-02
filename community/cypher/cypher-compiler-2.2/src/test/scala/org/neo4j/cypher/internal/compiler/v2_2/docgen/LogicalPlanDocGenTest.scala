/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.gen.{DocHandlerTestSuite, toStringDocGen}
import org.neo4j.cypher.internal.compiler.v2_2.perty.print.{PrintNewLine, PrintText, condense}
import org.neo4j.cypher.internal.compiler.v2_2.planner.PlannerQuery
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, LogicalLeafPlan, LogicalPlan}

class LogicalPlanDocGenTest extends DocHandlerTestSuite[Any] {

  val docGen =
    logicalPlanDocGen.lift[Any] orElse
    plannerDocGen orElse
    toStringDocGen

  override def docFormatter = DocFormatters.pageFormatter(80)

  test("Prints leaf plans") {
    pprintToString(TestLeafPlan(12)) should equal("TestLeafPlan[a](12)")
  }

  test("Prints pipe plans") {
    val doc = convert(TestPipePlan(TestLeafPlan(1)))
    val result = condense(docFormatter(doc))

    result should equal(Seq(
      PrintText("TestPipePlan[b]()"),
      PrintNewLine(0),
      PrintText("↳ TestLeafPlan[a](1)")
     ))
  }

  test("Prints long pipe plans") {
    val doc = convert(TestPipePlan(TestPipePlan(TestLeafPlan(1))))
    val result = condense(docFormatter(doc))
    result should equal(Seq(
      PrintText("TestPipePlan[b]()"),
      PrintNewLine(0),
      PrintText("↳ TestPipePlan[b]()"),
      PrintNewLine(0),
      PrintText("↳ TestLeafPlan[a](1)")
    ))
  }

  test("Prints combo plans") {
    val doc = convert(TestComboPlan(TestLeafPlan(1), TestLeafPlan(2)))
    val result = condense(docFormatter(doc))
    result should equal(Seq(
      PrintText("TestComboPlan[c,d]()"),
      PrintNewLine(2),
      PrintText("↳ left = TestLeafPlan[a](1)"),
      PrintNewLine(2),
      PrintText("↳ right = TestLeafPlan[a](2)")
    ))
  }

  /* re-enable perty to make it pass */ ignore("Prints on toString") {
    TestLeafPlan(12).toString should equal("TestLeafPlan[a](12)")
  }

  case class TestLeafPlan(x: Int) extends LogicalLeafPlan {
    def availableSymbols = Set[IdName](IdName("a"))
    def solved: PlannerQuery = ???
    def argumentIds: Set[IdName] = ???
  }

  case class TestPipePlan(left: LogicalPlan) extends LogicalPlan {
    def lhs = Some(left)
    def rhs = None
    def availableSymbols = Set[IdName](IdName("b"))
    def solved: PlannerQuery = ???
  }

  case class TestComboPlan(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan {
    def lhs = Some(left)
    def rhs = Some(right)
    def availableSymbols = Set[IdName](IdName("c"), IdName("d"))
    def solved: PlannerQuery = ???
  }
}
