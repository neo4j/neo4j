/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.phases.Monitors
import org.neo4j.cypher.internal.ir.v3_4.PlannerQuery
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{FakePipe, Pipe}
import org.neo4j.cypher.internal.util.v3_4.attribution.{Id, SameId}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.time.Clocks

class PipeExecutionPlanBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  abstract class FakePlan extends LogicalPlan(idGen = SameId(Id.INVALID_ID)) {
    private val pq = PlannerQuery.empty

    override def lhs: Option[LogicalPlan] = None

    override val availableSymbols: Set[String] = Set.empty[String]

    override def rhs: Option[LogicalPlan] = None

    override def strictness: Nothing = ???
  }

  abstract class FakeRonjaPipe extends FakePipe(Iterator.empty)

  case class LeafPlan(name: String) extends FakePlan

  case class OneChildPlan(name: String, child: LogicalPlan) extends FakePlan {
    override def lhs = Some(child)
  }

  case class TwoChildPlan(name: String, l: LogicalPlan, r: LogicalPlan) extends FakePlan {
    override def lhs = Some(l)
    override def rhs = Some(r)

  }

  case class LeafPipe(name: String) extends FakeRonjaPipe

  case class OneChildPipe(name: String, src: Pipe) extends FakeRonjaPipe

  case class TwoChildPipe(name: String, l: Pipe, r: Pipe) extends FakeRonjaPipe


  val factory = new PipeBuilderFactory {
    override def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean, expressionConverters: ExpressionConverters)
                      (implicit context: PipeExecutionBuilderContext, planContext: PlanContext) = new PipeBuilder {
      def build(plan: LogicalPlan) = plan match {
        case LeafPlan(n) => LeafPipe(n)
      }

      def build(plan: LogicalPlan, source: Pipe) = plan match {
        case OneChildPlan(name, _) => OneChildPipe(name, source)
      }

      def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe) = plan match {
        case TwoChildPlan(name, _, _) => TwoChildPipe(name, lhs, rhs)
      }
    }
  }

  private val builder = {
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    new PipeExecutionPlanBuilder(Clocks.fakeClock(), mock[Monitors], factory, expressionConverters = converters)
  }
  private val planContext = newMockedPlanContext
  private val pipeContext = mock[PipeExecutionBuilderContext]
  when(pipeContext.readOnlies).thenReturn(new StubReadOnlies)

  test("should handle plan with single leaf node") {
    val plan = LeafPlan("a")
    val expectedPipe = LeafPipe("a")

    val result = builder.build(None, plan)(pipeContext, planContext).pipe
    result should equal(expectedPipe)
  }

  test("should handle plan with single leaf branch") {
    /*
      a
      |
      b
      |
      c */


    val plan =
      OneChildPlan("a",
        OneChildPlan("b",
          LeafPlan("c"))
      )

    val expectedPipe =
      OneChildPipe("a",
        OneChildPipe("b",
          LeafPipe("c")
        )
      )

    val result = builder.build(None, plan)(pipeContext, planContext).pipe
    result should equal(expectedPipe)
  }

  test("should handle plan with two leaf nodes") {
    /*
      a
      |
      b
     / \
    c   d*/


    val plan =
      OneChildPlan("a",
        TwoChildPlan("b",
          LeafPlan("c"),
          LeafPlan("d")
        )
      )

    val expectedPipe =
      OneChildPipe("a",
        TwoChildPipe("b",
          LeafPipe("c"),
          LeafPipe("d")
        )
      )

    val result = builder.build(None, plan)(pipeContext, planContext).pipe
    result should equal(expectedPipe)
  }

  test("should handle plan with three leaf nodes") {
    /*
          a
         / \
        b   c
       /   / \
      d   e   f*/

    val plan = TwoChildPlan("a",
                OneChildPlan("b", LeafPlan("d")),
                TwoChildPlan("c", LeafPlan("e"), LeafPlan("f"))
      )

    val expectedPipe = TwoChildPipe("a",
                        OneChildPipe("b", LeafPipe("d")),
                        TwoChildPipe("c", LeafPipe("e"), LeafPipe("f")))

    val result = builder.build(None, plan)(pipeContext, planContext).pipe
    result should equal(expectedPipe)
  }
}
