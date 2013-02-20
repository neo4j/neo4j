/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.graphdb.Path
import org.neo4j.cypher.internal.commands.values.LabelValue
import org.neo4j.cypher.internal.commands.values.LabelName
import org.neo4j.cypher.internal.helpers.IsCollection

class ResultValueMapperPipe(source: Pipe) extends PipeWithSource(source) {

  def throwIfSymbolsMissing(symbols: SymbolTable) {}


  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    def mapValue(in: Any): Any = in match {
      case p: Path => p
      case l: LabelValue => l.resolveForName(state.query).name
      case IsCollection(coll) => coll.map(mapValue)
      case x => x
    }

    val result: Iterator[ExecutionContext] = source.createResults(state).map {
      (ctx: ExecutionContext) =>
        val newMap = ctx.transform {
          (_, v: Any) => mapValue(v)
        }
        newMap.asInstanceOf[ExecutionContext] // Jetbraaaaiiinz
    }

    result
  }

  def executionPlanDescription() = source.executionPlanDescription.andThen(this, "ResultValueMapper")

  def symbols:SymbolTable = new SymbolTable(source.symbols.identifiers.mapValues { (t: CypherType) =>
    t.rewrite {
      case _: LabelType => StringType()
      case x => x
    }
  })
}