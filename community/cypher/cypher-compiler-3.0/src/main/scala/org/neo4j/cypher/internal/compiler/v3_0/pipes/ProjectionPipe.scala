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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects._
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.LegacyExpressions

/*
Projection evaluates expressions and stores their values into new slots in the execution context.
It's an additive operation - nothing is lost in the execution context, the pipe simply adds new key-value pairs.
 */
case class ProjectionPipe(source: Pipe, expressions: Map[String, Expression])(val estimatedCardinality: Option[Double] = None)
                         (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  expressions.values.foreach(_.registerOwningPipe(this))

  val symbols = {
    val newVariables = expressions.map {
      case (name, expression) => name -> expression.getType(source.symbols)
    }

    source.symbols.add(newVariables)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map {
      ctx =>
        expressions.foreach {
          case (name, expression) =>
            val result = expression(ctx)(state)
            ctx.put(name, result)
        }

        ctx
    }
  }

  def planDescriptionWithoutCardinality =
    source.planDescription
      .andThen(this.id, "Projection", variables, LegacyExpressions(expressions))

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  override def localEffects = expressions.effects(symbols)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
