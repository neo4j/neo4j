/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.kernel.Traversal
import org.neo4j.cypher.SyntaxException
import java.lang.String
import collection.Seq
import org.neo4j.cypher.internal.symbols.{NodeType, Identifier, PathType}
import collection.mutable.Map
import org.neo4j.cypher.internal.commands.{ShortestPathExpression, ReturnItem, ShortestPath}
import org.neo4j.graphdb.{Path, Expander, DynamicRelationshipType, Node}

/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 *
 * It's also the base class for all shortest paths
 */
class ShortestPathPipe(source: Pipe, ast: ShortestPath) extends PipeWithSource(source) {
  def startName = ast.start

  def endName = ast.end

  def relType = ast.relTypes

  def dir = ast.dir

  def maxDepth = ast.maxDepth

  def optional = ast.optional

  def pathName = ast.pathName

  def returnItems: Seq[ReturnItem] = Seq()

  val expression = ShortestPathExpression(ast)

  def createResults(state: QueryState) = source.createResults(state).flatMap(ctx => {
    val result: Stream[Path] = expression(ctx).asInstanceOf[Stream[Path]]

    if (result.isEmpty) {
      if (optional)
        Seq(ctx.newWith(pathName -> null))
      else
        Seq()
    } else {
      result.map(x => ctx.newWith(pathName -> x))
    }

  })


  def dependencies: Seq[Identifier] = Seq(Identifier(startName, NodeType()), Identifier(endName, NodeType()))

  val symbols = source.symbols.add(Identifier(pathName, PathType()))

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "ShortestPath(" + ast + ")"
}
