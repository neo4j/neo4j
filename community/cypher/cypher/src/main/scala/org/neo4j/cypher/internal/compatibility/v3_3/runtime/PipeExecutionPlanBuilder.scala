/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.ir.v3_3.PeriodicCommit

import scala.collection.mutable

class PipeExecutionPlanBuilder(clock: Clock,
                               monitors: Monitors,
                               pipeBuilderFactory: PipeBuilderFactory,
                               expressionConverters: ExpressionConverters) {
  def build(periodicCommit: Option[PeriodicCommit], plan: LogicalPlan, idMap: Map[LogicalPlan, Id])
           (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeInfo = {

    val topLevelPipe = buildPipe(plan, idMap)

    val fingerprint = planContext.statistics match {
      case igs: InstrumentedGraphStatistics =>
        Some(PlanFingerprint(clock.millis(), planContext.txIdProvider(), igs.snapshot.freeze))
      case _ =>
        None
    }

    val periodicCommitInfo = periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    PipeInfo(topLevelPipe, !plan.solved.readOnly,
             periodicCommitInfo, fingerprint, context.plannerName)
  }

  /*
  Traverses the logical plan tree structure and builds up the corresponding pipe structure. Given a logical plan such as:

          a
         / \
        b   c
       /   / \
      d   e   f

   populate(a) starts the session, and eagerly adds [a, c, f] to the plan stack. We then immediately pop 'f' from the
   plan stack, we build a pipe for it add it to the pipe stack, and pop 'c' from the plan stack. Since we are coming from
   'f', we add [c, e] to the stack and then pop 'e' out again. This is a leaf, so we build a pipe for it and add it to the
   pipe stack. We now pop 'c' from the plan stack again. This time we are coming from 'e', and so we know we can use
   two pipes from the pipe stack to use when building 'c'. We add this pipe to the pipe stack and pop 'a' from the plan
   stack. Since we are coming from 'a's RHS, we add [a,b,d] to the stack. Next step is to pop 'd' out, and build a pipe
   for it, storing it in the pipe stack. Pop ut 'b' from the plan stack, one pipe from the pipe stack, and build a pipe for 'b'.
   Next we pop out 'a', and this time we are coming from the LHS, and we can now pop two pipes from the pipe stack to
   build the pipe for 'a'. Thanks for reading this far - I didn't think we would make it!
   */
  private def buildPipe(plan: LogicalPlan, idMap: Map[LogicalPlan, Id])(implicit context: PipeExecutionBuilderContext, planContext: PlanContext): Pipe = {
    val pipeBuilder = pipeBuilderFactory(
      monitors = monitors,
      recurse = p => buildPipe(p, idMap),
      readOnly = plan.solved.all(_.queryGraph.readOnly),
      idMap = idMap,
      expressionConverters = expressionConverters)

    val planStack = new mutable.Stack[LogicalPlan]()
    val pipeStack = new mutable.Stack[Pipe]()
    var comingFrom = plan
    def populate(plan: LogicalPlan) = {
      var current = plan
      while (!current.isLeaf) {
        planStack.push(current)
        (current.lhs, current.rhs) match {
          case (Some(_), Some(right)) =>
            current = right

          case (Some(left), None) =>
            current = left
          case _ => throw new InternalException("This must not be!")
        }
      }
      comingFrom = current
      planStack.push(current)
    }

    populate(plan)

    while (planStack.nonEmpty) {
      val current = planStack.pop()

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val newPipe = pipeBuilder
            .build(current)

          pipeStack.push(newPipe)

        case (Some(_), None) =>
          val source = pipeStack.pop()
          val newPipe = pipeBuilder
            .build(current, source)

          pipeStack.push(newPipe)

        case (Some(left), Some(right)) if right == left =>
          throw new InternalException(s"Tried to build pipes from bad logical plan. LHS and RHS must never be the same: op: $current\nfull plan: $plan")

        case (Some(left), Some(_)) if comingFrom == left =>
          val arg1 = pipeStack.pop()
          val arg2 = pipeStack.pop()
          val newPipe = pipeBuilder
            .build(current, arg1, arg2)

          pipeStack.push(newPipe)

        case (Some(left), Some(right)) if comingFrom == right =>
          planStack.push(current)
          populate(left)
      }

      comingFrom = current

    }

    val result = pipeStack.pop()
    assert(pipeStack.isEmpty, "Should have emptied the stack of pipes by now!")

    result
  }
}

object CommunityPipeBuilderFactory extends PipeBuilderFactory {
  def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean, idMap: Map[LogicalPlan, Id], expressionConverters: ExpressionConverters)(implicit context: PipeExecutionBuilderContext, planContext: PlanContext) =
  CommunityPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters)
}

trait PipeBuilder {
  def build(plan: LogicalPlan): Pipe
  def build(plan: LogicalPlan, source: Pipe): Pipe
  def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe
}
