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
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, Effects, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.LegacyExpression
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Path

import scala.collection.JavaConverters._
/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
case class ShortestPathPipe(source: Pipe, shortestPathCommand: ShortestPath, predicates: Seq[Predicate] = Seq.empty)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with CollectionSupport with RonjaPipe {
  private def pathName = shortestPathCommand.pathName
  private val shortestPathExpression = ShortestPathExpression(shortestPathCommand, predicates)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = input.flatMap(ctx => {
    val result: Stream[Path] = shortestPathExpression(ctx)(state) match {
      case in: Stream[_] => CastSupport.castOrFail[Stream[Path]](in)
      case null => Stream()
      case path: Path => Stream(path)
      case in: Iterator[_] => CastSupport.castOrFail[Stream[Path]](in.toStream)
    }

    shortestPathCommand.relIterator match {
      case Some(relName) =>
        result.map { (path: Path) => ctx.newWith2(pathName, path, relName, path.relationships().asScala.toSeq) }
      case None =>
        result.map { (path: Path) => ctx.newWith1(pathName, path) }
    }
  })

  val symbols = {
    val withPath = source.symbols.add(pathName, CTPath)
    shortestPathCommand.relIterator match {
      case None    => withPath
      case Some(x) => withPath.add(x, CTCollection(CTRelationship))
    }
  }

  override def planDescriptionWithoutCardinality = {
    val args = predicates.map { p =>
      LegacyExpression(p)
    }
    source.planDescription.andThen(this.id, "ShortestPath", identifiers, args:_*)
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def localEffects = Effects(ReadsAllNodes, ReadsRelationships)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
