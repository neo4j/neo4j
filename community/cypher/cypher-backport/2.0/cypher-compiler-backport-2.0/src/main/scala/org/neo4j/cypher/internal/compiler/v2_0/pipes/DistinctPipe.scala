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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.Expression
import symbols._
import collection.mutable
import org.neo4j.cypher.internal.helpers._

class DistinctPipe(source: Pipe, expressions: Map[String, Expression]) extends PipeWithSource(source) {

  val keyNames: Seq[String] = expressions.keys.toSeq

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    // Run the return item expressions, and replace the execution context's with their values
    val returnExpressions = input.map(ctx => {
      val newMap = Materialized.mapValues(expressions, (expression: Expression) => expression(ctx)(state))
      ctx.newFrom(newMap)
    })

    /*
     * The filtering is done by extracting from the context the values of all return expressions, and keeping them
     * in a set.
     */
    var seen = mutable.Set[NiceHasher]()

    returnExpressions.filter {
       case ctx =>
         val values = new NiceHasher(keyNames.map(ctx).toSeq)

         if (seen.contains(values)) {
           false
         } else {
           seen += values
           true
         }
    }
  }

  override def executionPlanDescription = source.executionPlanDescription.andThen(this, "Distinct")

  def symbols: SymbolTable = {
    val identifiers = Materialized.mapValues(expressions, (e: Expression) => e.evaluateType(CTAny, source.symbols))
    SymbolTable(identifiers)
  }
}
