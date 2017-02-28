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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Equivalent
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_2.helpers.Eagerly

import scala.collection.mutable

case class DistinctPipe(source: Pipe, expressions: Map[String, Expression])
                       (val id: Id = new Id)
                       (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  val keyNames: Seq[String] = expressions.keys.toIndexedSeq

  expressions.values.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    // Run the return item expressions, and replace the execution context's with their values
    val result = input.map(ctx => {
      val newMap = Eagerly.mutableMapValues(expressions, (expression: Expression) => expression(ctx)(state))
      ctx.copy(m = newMap)
    })

    /*
     * The filtering is done by extracting from the context the values of all return expressions, and keeping them
     * in a set.
     */
    var seen = mutable.Set[Equivalent]()

    result.filter {
       case ctx =>
         val values = Equivalent(keyNames.map(ctx))

         if (seen.contains(values)) {
           false
         } else {
           seen += values
           true
         }
    }
  }
}
