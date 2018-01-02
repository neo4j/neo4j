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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Path, PropertyContainer}

import scala.collection.JavaConverters._

case class NamedPathPipe(source: Pipe, pathName: String, entities: Seq[AbstractPattern])
                        (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    input.map {
      ctx => ctx += (pathName -> getPath(ctx))
    }

  // TODO: This is duplicated with PathExtractor
  private def getPath(ctx: ExecutionContext): Path = {
    def get(x: String): PropertyContainer = ctx(x).asInstanceOf[PropertyContainer]

    val p: Seq[PropertyContainer] = entities.foldLeft(get(firstNode) :: Nil)((soFar, p) => p match {
      case e: ParsedEntity           => soFar
      case r: ParsedRelation         => soFar :+ get(r.name) :+ get(r.end.name)
      case path: PatternWithPathName => getPath(ctx, path.pathName, soFar)
    })

    buildPath(p)
  }

  private def firstNode: String =
    entities.head.start.name

  private def buildPath(pieces: Seq[PropertyContainer]): Path =
    if (pieces.contains(null))
      null
    else
      new PathImpl(pieces: _*)

  //WARNING: This method can return NULL
  private def getPath(m: ExecutionContext, key: String, soFar: List[PropertyContainer]): List[PropertyContainer] = {
    val m1 = m(key)

    if (m1 == null)
      return List(null)

    val path = m1.asInstanceOf[Path].iterator().asScala.toList

    val pathTail = if (path.head == soFar.last) {
      path.tail
    } else {
      path.reverse.tail
    }

    soFar ++ pathTail
  }

  val symbols = source.symbols.add(pathName, CTPath)

  override def planDescription =
    source.planDescription.andThen(this.id, "ExtractPath", identifiers)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }

  override def localEffects = Effects()
}
