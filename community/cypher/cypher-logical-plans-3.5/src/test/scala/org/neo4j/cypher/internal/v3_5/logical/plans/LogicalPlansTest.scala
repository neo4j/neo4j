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
package org.neo4j.cypher.internal.v3_5.logical.plans

import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.attribution.{IdGen, SequentialIdGen}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions.True

import scala.collection.mutable.ArrayBuffer

class LogicalPlansTest extends CypherFunSuite {

  val pos = InputPosition(1,1,1)

  test("LogicalPlans.map") {

    implicit val idGen: IdGen = new SequentialIdGen

   /*
    *         a
    *        / \
    *       b   c
    *      /   / \
    *     d   e   f
    */
    val d = AllNodesScan("d", Set.empty)
    val e = AllNodesScan("e", Set.empty)
    val f = AllNodesScan("f", Set.empty)
    val b = Selection(List(True()(pos)), d)
    val c = Apply(e, f)
    val a = Apply(b, c)

    val idStrings = new IdStrings()
    LogicalPlans.map(a, idStrings)

    idStrings.calls should equal(List(
      "onLeaf Id(2)",
      "onLeaf Id(1)",
      "onTwoChildPlan Id(4) Id(1) Id(2)",
      "onLeaf Id(0)",
      "onOneChildPlan Id(3) Id(0)",
      "onTwoChildPlan Id(5) Id(3) Id(4)"
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

  private def id(plan: LogicalPlan) = "Id("+plan.id.x+")"
}
