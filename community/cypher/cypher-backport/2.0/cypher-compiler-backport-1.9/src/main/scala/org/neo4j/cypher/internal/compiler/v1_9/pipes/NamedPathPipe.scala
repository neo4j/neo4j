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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import org.neo4j.cypher.internal.compiler.v1_9.commands.NamedPath
import org.neo4j.cypher.internal.compiler.v1_9.data.SimpleVal
import org.neo4j.cypher.internal.compiler.v1_9.symbols.{SymbolTable, PathType}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext

class NamedPathPipe(source: Pipe, path: NamedPath) extends PipeWithSource(source) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = input.map(ctx => {
    ctx += (path.pathName -> path.getPath(ctx))
  })

  val symbols = source.symbols.add(path.pathName, PathType())

  override def executionPlanDescription = {
    val name = SimpleVal.fromStr(path.pathName)
    val pats = SimpleVal.fromIterable(path.pathPattern)

    source.executionPlanDescription.andThen(this, "ExtractPath",  "name" -> name, "patterns" -> pats)
  }
  def throwIfSymbolsMissing(symbols: SymbolTable) { }
}
