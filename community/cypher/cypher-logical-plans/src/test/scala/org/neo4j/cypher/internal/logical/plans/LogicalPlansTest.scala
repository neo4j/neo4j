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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class LogicalPlansTest extends CypherFunSuite {

  private val pos = InputPosition(1, 1, 1)

  test("LogicalPlans.map") {

    implicit val idGen: IdGen = new SequentialIdGen

    /*
     *         p5(Apply)
     *        /  \
     *       p1  p4(Apply)
     *      /   /  \
     *     p0  p2  p3
     */
    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = Selection(List(True()(pos)), p0)
    val p2 = AllNodesScan(varFor("p2"), Set.empty)
    val p3 = AllNodesScan(varFor("p3"), Set.empty)
    val p4 = Apply(p2, p3)
    val p5 = Apply(p1, p4)

    val idStrings = new IdStrings()
    LogicalPlans.map(p5, idStrings)

    idStrings.calls should equal(List(
      "onLeaf p3",
      "onLeaf p2",
      "onTwoChildPlan p4 p2 p3",
      "onLeaf p0",
      "onOneChildPlan p1 p0",
      "onTwoChildPlan p5 p1 p4"
    ))
  }

  class IdStrings() extends LogicalPlans.Mapper[String] {

    val calls = new ArrayBuffer[String]

    override def onLeaf(plan: LogicalPlan): String = {
      val str = id(plan)
      calls += s"onLeaf $str"
      str
    }

    override def onOneChildPlan(plan: LogicalPlan, source: String): String = {
      val str = id(plan)
      calls += s"onOneChildPlan $str $source"
      str
    }

    override def onTwoChildPlan(plan: LogicalPlan, lhs: String, rhs: String): String = {
      val str = id(plan)
      calls += s"onTwoChildPlan $str $lhs $rhs"
      str
    }
  }

  test("LogicalPlans.foldPlan: apply plan") {

    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = Selection(List(True()(pos)), p0)
    val p2 = AllNodesScan(varFor("p2"), Set.empty)
    val p3 = Apply(p1, p2)

    val foldedString =
      LogicalPlans.foldPlan("")(p3, (acc, plan) => s"$acc->${id(plan)}", (_, rhs, plan) => s"$rhs=>${id(plan)}")

    foldedString shouldBe "->p0->p1->p2=>p3"
  }

  test("LogicalPlans.foldPlan: apply plan with selection on rhs") {

    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = Selection(List(True()(pos)), p1)
    val p3 = Apply(p0, p2)

    val foldedString =
      LogicalPlans.foldPlan("")(p3, (acc, plan) => s"$acc->${id(plan)}", (_, rhs, plan) => s"$rhs=>${id(plan)}")

    foldedString shouldBe "->p0->p1->p2=>p3"
  }

  test("LogicalPlans.foldPlan: apply plan with selection on top") {

    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = Apply(p0, p1)
    val p3 = Selection(List(True()(pos)), p2)

    val foldedString =
      LogicalPlans.foldPlan("")(p3, (acc, plan) => s"$acc->${id(plan)}", (_, rhs, plan) => s"$rhs=>${id(plan)}")

    foldedString shouldBe "->p0->p1=>p2->p3"
  }

  test("LogicalPlans.foldPlan: should map argument accumulator of apply") {
    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = Apply(p0, p1)
    val p3 = Selection(List(True()(pos)), p2)

    case class Acc(str: String = "", uppercase: Boolean = false)

    val Acc(foldedString, _) =
      LogicalPlans.foldPlan(Acc())(
        p3,
        {
          case (Acc(str, uppercase), plan) => Acc(s"$str->${id(plan, uppercase)}", uppercase)
        },
        (lhs, rhs, plan) => Acc(s"${rhs.str}=>${id(plan, lhs.uppercase)}", lhs.uppercase),
        mapArguments = {
          case (acc, `p2`) => acc.copy(uppercase = true)
          case _           => fail("Should only call mapArguments with p2")
        }
      )

    foldedString shouldBe "->p0->P1=>p2->p3"
  }

  test("LogicalPlans.foldPlan: should not map argument accumulator of CartesianProduct") {
    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = CartesianProduct(p0, p1)
    val p3 = Selection(List(True()(pos)), p2)

    case class Acc(str: String = "", uppercase: Boolean = false)

    val Acc(foldedString, _) =
      LogicalPlans.foldPlan(Acc())(
        p3,
        {
          case (Acc(str, uppercase), plan) => Acc(s"$str->${id(plan, uppercase)}", uppercase)
        },
        (lhs, rhs, plan) => Acc(s"${id(plan, lhs.uppercase)}(${lhs.str}, ${rhs.str})", lhs.uppercase),
        mapArguments = (acc, _) => acc.copy(uppercase = true)
      )

    foldedString shouldBe "p2(->p0, ->p1)->p3"
  }

  test("LogicalPlans.foldPlan: cartesian product") {

    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = CartesianProduct(p0, p1)
    val p3 = Selection(List(True()(pos)), p2)

    val foldedString =
      LogicalPlans.foldPlan("")(p3, (acc, plan) => s"$acc->${id(plan)}", (lhs, rhs, plan) => s"${id(plan)}($lhs, $rhs)")

    foldedString shouldBe "p2(->p0, ->p1)->p3"
  }

  test("LogicalPlans.foldPlan: applies and cartesian product") {

    implicit val idGen: IdGen = new SequentialIdGen

    /*
     *         p7(Apply)
     *        /  \
     *       p1  p6(Apply)
     *      /   /   \
     *     p0  p2   p5(CartesianProduct)
     *             /   \
     *            p3    p4
     */
    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = Selection(List(True()(pos)), p0)
    val p2 = AllNodesScan(varFor("p2"), Set.empty)
    val p3 = AllNodesScan(varFor("p3"), Set.empty)
    val p4 = AllNodesScan(varFor("p4"), Set.empty)
    val p5 = CartesianProduct(p3, p4)
    val p6 = Apply(p2, p5)
    val p7 = Apply(p1, p6)

    val foldedString =
      LogicalPlans.foldPlan("")(
        p7,
        (acc, plan) => s"$acc->${id(plan)}",
        (lhs, rhs, plan) =>
          plan match {
            case _: Apply => s"$rhs=>${id(plan)}"
            case _        => s"${id(plan)}($lhs, $rhs)"
          }
      )

    foldedString shouldBe "p5(->p0->p1->p2->p3, ->p0->p1->p2->p4)=>p6=>p7"
  }

  test("LogicalPlans.foldPlan: should map argument accumulator of apply with nested CartesianProduct") {
    implicit val idGen: IdGen = new SequentialIdGen

    /*
     *         p5(Apply)
     *        /  \
     *       p1  p4(CartesianProduct)
     *      /   /   \
     *     p0  p2   p3
     */
    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = Selection(List(True()(pos)), p0)
    val p2 = AllNodesScan(varFor("p2"), Set.empty)
    val p3 = AllNodesScan(varFor("p3"), Set.empty)
    val p4 = CartesianProduct(p2, p3)
    val p5 = Apply(p1, p4)

    case class Acc(str: String = "", uppercase: Boolean = false)

    val Acc(foldedString, _) =
      LogicalPlans.foldPlan(Acc())(
        p5,
        {
          case (Acc(str, uppercase), plan) => Acc(s"$str->${id(plan, uppercase)}", uppercase)
        },
        (lhs, rhs, plan) =>
          plan match {
            case _: Apply => Acc(s"${rhs.str}=>${id(plan, lhs.uppercase)}", lhs.uppercase)
            case _        => Acc(s"${id(plan, lhs.uppercase)}(${lhs.str}, ${rhs.str})", lhs.uppercase)
          },
        mapArguments = {
          case (acc, _) => acc.copy(uppercase = true)
        }
      )

    foldedString shouldBe "P4(->p0->p1->P2, ->p0->p1->P3)=>p5"
  }

  test("LogicalPlans.foldPlan: cartesian product and applies") {

    implicit val idGen: IdGen = new SequentialIdGen

    /*
     *         p7
     *         |
     *         p6(CartesianProduct)
     *        /         \
     *       p2(Apply)  p5(Apply)
     *      /   \      /    \
     *     p0   p1    p3    p4
     */
    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = Apply(p0, p1)
    val p3 = AllNodesScan(varFor("p3"), Set.empty)
    val p4 = AllNodesScan(varFor("p4"), Set.empty)
    val p5 = Apply(p3, p4)
    val p6 = CartesianProduct(p2, p5)
    val p7 = Selection(List(True()(pos)), p6)

    val foldedString =
      LogicalPlans.foldPlan("")(
        p7,
        (acc, plan) => s"$acc->${id(plan)}",
        (lhs, rhs, plan) =>
          plan match {
            case _: Apply => s"$rhs=>${id(plan)}"
            case _        => s"${id(plan)}($lhs, $rhs)"
          }
      )

    foldedString shouldBe "p6(->p0->p1=>p2, ->p3->p4=>p5)->p7"
  }

  test("LogicalPlans.simpleFoldPlan: apply plan") {
    implicit val idGen: IdGen = new SequentialIdGen

    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = Selection(List(True()(pos)), p0)
    val p2 = AllNodesScan(varFor("p2"), Set.empty)
    val p3 = Apply(p1, p2)

    val foldedString =
      LogicalPlans.simpleFoldPlan("")(
        p3,
        (acc, plan) => s"$acc->${id(plan)}"
      )

    foldedString shouldBe "->p0->p1->p2->p3"
  }

  test("LogicalPlans.simpleFoldPlan: cartesian product and applies") {
    implicit val idGen: IdGen = new SequentialIdGen

    /*
     *         p7
     *         |
     *         p6(CartesianProduct)
     *        /         \
     *       p2(Apply)  p5(Apply)
     *      /   \      /    \
     *     p0   p1    p3    p4
     */
    val p0 = AllNodesScan(varFor("p0"), Set.empty)
    val p1 = AllNodesScan(varFor("p1"), Set.empty)
    val p2 = Apply(p0, p1)
    val p3 = AllNodesScan(varFor("p3"), Set.empty)
    val p4 = AllNodesScan(varFor("p4"), Set.empty)
    val p5 = Apply(p3, p4)
    val p6 = CartesianProduct(p2, p5)
    val p7 = Selection(List(True()(pos)), p6)

    val foldedString =
      LogicalPlans.simpleFoldPlan("")(
        p7,
        (acc, plan) => s"$acc->${id(plan)}"
      )

    foldedString shouldBe "->p0->p1->p2->p3->p4->p5->p6->p7"
  }

  private def id(plan: LogicalPlan, uppercase: Boolean = false) = {
    if (uppercase) {
      "P" + plan.id.x
    } else {
      "p" + plan.id.x
    }
  }
}
