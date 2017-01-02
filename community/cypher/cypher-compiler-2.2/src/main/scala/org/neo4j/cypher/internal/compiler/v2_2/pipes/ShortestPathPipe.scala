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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{Effects, ReadsNodes, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_2.helpers.{CollectionSupport, CastSupport}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.Path

import scala.collection.JavaConverters._
/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
case class ShortestPathPipe(source: Pipe, ast: ShortestPath)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with CollectionSupport with RonjaPipe {
  private def pathName = ast.pathName
  private val expression = ShortestPathExpression(ast)

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState) = input.flatMap(ctx => {
    val result: Stream[Path] = expression(ctx)(state) match {
      case in: Stream[_] => CastSupport.castOrFail[Stream[Path]](in)
      case null          => Stream()
      case path: Path    => Stream(path)
    }

    ast.relIterator match {
      case Some(relName) =>
        result.map { (path: Path) => ctx.newWith2(pathName, path, relName, path.relationships().asScala.toSeq) }
      case None =>
        result.map { (path: Path) => ctx.newWith1(pathName, path) }
    }
  })

  val symbols = {
    val withPath = source.symbols.add(pathName, CTPath)
    ast.relIterator match {
      case None    => withPath
      case Some(x) => withPath.add(x, CTCollection(CTRelationship))
    }
  }

  override def planDescription =
    source.planDescription.andThen(this, "ShortestPath", identifiers)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def localEffects = Effects(ReadsNodes, ReadsRelationships)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
