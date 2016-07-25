/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.execution

import org.neo4j.cypher.internal.compiler.v3_1.Monitors
import org.neo4j.cypher.internal.compiler.v3_1.pipes.{FakePipe, Pipe, RonjaPipe}
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_1.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.time.FakeClock

class PipeExecutionPlanBuilderTest extends CypherFunSuite {

  abstract class FakePlan extends LogicalPlan {
    private val pq = PlannerQuery.empty

    def solved = CardinalityEstimation.lift(pq, Cardinality(1.0))

    override def lhs: Option[LogicalPlan] = None


    override def availableSymbols = ???

    override def rhs: Option[LogicalPlan] = None

    override def strictness = ???
  }

  abstract class FakeRonjaPipe extends FakePipe(Iterator.empty) with RonjaPipe {
    override def planDescriptionWithoutCardinality = ???

    override def withEstimatedCardinality(estimated: Double) = this

    override def estimatedCardinality = ???
  }


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
    override def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean)
                      (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder = new PipeBuilder {
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

  private val builder = new PipeExecutionPlanBuilder(new FakeClock, mock[Monitors], factory)
  private implicit val planContext = mock[PlanContext]
  private implicit val pipeContext = mock[PipeExecutionBuilderContext]


  test("should handle plan with single leaf node") {
    val plan = LeafPlan("a")
    val expectedPipe = LeafPipe("a")

    val result = builder.build(None, plan).pipe
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

    val result = builder.build(None, plan).pipe
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

    val result = builder.build(None, plan).pipe
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

    val result = builder.build(None, plan).pipe
    result should equal(expectedPipe)
  }
}
