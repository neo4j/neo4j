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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}

import scala.collection.mutable

case class ValueHashJoinPipe(lhsExpression: Expression, rhsExpression: Expression, left: Pipe, right: Pipe)
                            (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(left, pipeMonitor) with RonjaPipe {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    implicit val x = state
    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input)

    if (table.isEmpty)
      return Iterator.empty

    val result = for {context: ExecutionContext <- rhsIterator
                      joinKey = rhsExpression(context) if joinKey != null}
      yield {
        val seq = table.getOrElse(joinKey, mutable.MutableList.empty)
        seq.map(context ++ _)
      }
    result.flatten
  }


  override def planDescriptionWithoutCardinality: InternalPlanDescription = {
    new PlanDescriptionImpl(
      id = id,
      name = "ValueHashJoin",
      children = TwoChildren(left.planDescription, right.planDescription),
      arguments = Seq(Arguments.LegacyExpression(lhsExpression), Arguments.LegacyExpression(rhsExpression)),
      variables
    )
  }

  override def symbols = left.symbols.add(right.symbols.variables)

  override val sources = Seq(left, right)

  override def dup(sources: List[Pipe]): Pipe = {
    val (left :: right :: Nil) = sources
    copy(left = left, right = right)(estimatedCardinality)
  }

  override def localEffects = Effects()

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private def buildProbeTable(input: Iterator[ExecutionContext])(implicit state: QueryState) = {
    val table = new mutable.HashMap[Any, mutable.MutableList[ExecutionContext]]

    for (context <- input;
         joinKey = lhsExpression(context) if joinKey != null) {
      val seq = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      seq += context
    }

    table
  }
}
