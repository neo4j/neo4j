/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1._
import commands._
import commands.expressions.ShortestPathExpression
import symbols._
import org.neo4j.cypher.internal.helpers._
import org.neo4j.graphdb.{Relationship, Path}
import org.neo4j.cypher.internal.compiler.v2_1.PlanDescription.Arguments.IntroducedIdentifier
import collection.JavaConverters._
/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
class ShortestPathPipe(source: Pipe, ast: ShortestPath)
                      (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with CollectionSupport {
  private def pathName = ast.pathName
  private val expression = ShortestPathExpression(ast)

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState) = input.flatMap(ctx => {
    val result: Stream[Path] = expression(ctx)(state) match {
      case in: Stream[_] => CastSupport.castOrFail[Stream[Path]](in)
      case null          => Stream()
      case path: Path    => Stream(path)
    }

    result.map {
      (path: Path) =>
        val newValues: Seq[(String, Any)] = Seq(pathName -> path) ++ getRelIterator(path)
        ctx.newWith(newValues)
    }
  })

  val symbols = {
    val withPath = source.symbols.add(pathName, CTPath)
    ast.relIterator match {
      case None    => withPath
      case Some(x) => withPath.add(x, CTCollection(CTRelationship))
    }
  }

  private def getRelIterator(p: Path): TraversableOnce[(String, Seq[Relationship])] = {
    ast.relIterator.map {
      relName => relName -> p.relationships().asScala.toSeq
    }
  }

  override def planDescription =
    source.planDescription.andThen(this, "ShortestPath", IntroducedIdentifier(ast.pathName))

}
