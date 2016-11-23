/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v3_2.commands._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{CastSupport, ListSupport}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.LegacyExpressions
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.graphdb.Path

import scala.collection.JavaConverters._
/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 */
case class ShortestPathPipe(source: Pipe, shortestPathCommand: ShortestPath, predicates: Seq[Predicate] = Seq.empty,
                            withFallBack: Boolean = false)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with ListSupport with RonjaPipe {
  private def pathName = shortestPathCommand.pathName
  private val shortestPathExpression = ShortestPathExpression(shortestPathCommand, predicates, withFallBack)

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState) =
    input.flatMap(ctx => {
      val result: Stream[Path] = shortestPathExpression(ctx)(state) match {
        case in: Stream[_] => CastSupport.castOrFail[Stream[Path]](in)
        case null          => Stream()
        case path: Path    => Stream(path)
        case in: Iterator[_] => CastSupport.castOrFail[Stream[Path]](in.toStream)
      }

      shortestPathCommand.relIterator match {
        case Some(relName) =>
          result.map { (path: Path) => ctx.newWith2(pathName, path, relName, path.relationships().asScala.toIndexedSeq) }
        case None =>
          result.map { (path: Path) => ctx.newWith1(pathName, path) }
      }
    })

  val symbols = {
    val withPath = source.symbols.add(pathName, CTPath)
    shortestPathCommand.relIterator match {
      case None    => withPath
      case Some(x) => withPath.add(x, CTList(CTRelationship))
    }
  }

  override def planDescriptionWithoutCardinality = {
    val args = predicates.zipWithIndex.map { case (p, idx) => s"p$idx" -> p }
    source.planDescription.andThen(this.id, "ShortestPath", variables, LegacyExpressions(args.toMap))
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def localEffects = Effects(ReadsAllNodes, ReadsAllRelationships)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
