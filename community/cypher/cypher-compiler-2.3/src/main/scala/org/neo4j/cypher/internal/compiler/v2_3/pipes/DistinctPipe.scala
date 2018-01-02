/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

import scala.collection.mutable

case class DistinctPipe(source: Pipe, expressions: Map[String, Expression])(val estimatedCardinality: Option[Double] = None)
                       (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  val keyNames: Seq[String] = expressions.keys.toSeq

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    // Run the return item expressions, and replace the execution context's with their values
    val result = input.map(ctx => {
      val newMap = Eagerly.mutableMapValues(expressions, (expression: Expression) => expression(ctx)(state))
      ctx.copy(m = newMap)
    })

    /*
     * The filtering is done by extracting from the context the values of all return expressions, and keeping them
     * in a set.
     */
    var seen = mutable.Set[NiceHasher]()

    result.filter {
       case ctx =>
         val values = new NiceHasher(keyNames.map(ctx))

         if (seen.contains(values)) {
           false
         } else {
           seen += values
           true
         }
    }
  }

  def planDescriptionWithoutCardinality = source.planDescription.
                        andThen(this.id, "Distinct", identifiers, KeyNames(expressions.keys.toSeq))

  def symbols: SymbolTable = {
    val identifiers = Eagerly.immutableMapValues(expressions, (e: Expression) => e.evaluateType(CTAny, source.symbols))
    SymbolTable(identifiers)
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  override def localEffects = expressions.effects(symbols)
}
