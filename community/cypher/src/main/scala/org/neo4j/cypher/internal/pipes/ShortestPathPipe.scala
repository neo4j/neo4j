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

import collection.Seq
import org.neo4j.cypher.internal.symbols._
import org.neo4j.graphdb.Path
import org.neo4j.cypher.internal.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.commands.ShortestPath
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.helpers.CollectionSupport

/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 *
 * It's also the base class for all shortest paths
 */
class ShortestPathPipe(source: Pipe, ast: ShortestPath) extends PipeWithSource(source) with CollectionSupport {
  private def optional = ast.optional
  private def pathName = ast.pathName
  private val expression = ShortestPathExpression(ast)

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState) = input.flatMap(ctx => {
    val result = expression(ctx)(state) match {
      case paths:Stream[Path] => paths
      case null               => Stream()
      case path:Path          => Stream(path)
    }

    if (result.isEmpty) {
      if (optional)
        Seq(ctx.newWith(pathName -> null))
      else
        Seq()
    } else {
      result.map(x => ctx.newWith(pathName -> x))
    }

  })

  val symbols = source.symbols.add(pathName, PathType())

  override def executionPlanDescription =
    source.executionPlanDescription.andThen(this, "ShortestPath", "ast" -> ast)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    ast.throwIfSymbolsMissing(symbols)
  }
}
