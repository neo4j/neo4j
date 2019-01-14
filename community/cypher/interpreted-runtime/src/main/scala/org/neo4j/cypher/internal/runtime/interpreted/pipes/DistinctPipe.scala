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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.v3_4.Eagerly
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

case class DistinctPipe(source: Pipe, expressions: Map[String, Expression])
                       (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  val keyNames: Seq[String] = expressions.keys.toIndexedSeq

  expressions.values.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    // Run the return item expressions, and replace the execution context's with their values
    val result = input.map(ctx => {
      val newMap = Eagerly.mutableMapValues(expressions, (expression: Expression) => expression(ctx, state))
      executionContextFactory.newExecutionContext(newMap)
    })

    /*
     * The filtering is done by extracting from the context the values of all return expressions, and keeping them
     * in a set.
     */
    var seen = mutable.Set[AnyValue]()

    result.filter { ctx =>
      val values = VirtualValues.list(keyNames.map(ctx): _*)

      if (seen.contains(values)) {
        false
      } else {
        seen += values
        true
      }
    }
  }
}
