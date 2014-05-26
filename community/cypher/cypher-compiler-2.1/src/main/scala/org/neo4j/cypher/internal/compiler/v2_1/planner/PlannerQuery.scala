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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.pprint.{PrettyToString, GeneratedPretty, Pretty, pformat}

trait PlannerQuery {
  def graph: QueryGraph
  def projection: QueryProjection
  def tail: Option[PlannerQuery]

  def withTail(newTail: PlannerQuery): PlannerQuery
  def withProjection(projection: QueryProjection): PlannerQuery
  def withGraph(graph: QueryGraph): PlannerQuery

  def updateGraph(f: QueryGraph => QueryGraph): PlannerQuery = withGraph(f(graph))
  def updateProjections(f: QueryProjection => QueryProjection): PlannerQuery = withProjection(f(projection))
  def updateTail(f: PlannerQuery => PlannerQuery): PlannerQuery

  def updateTailOrSelf(f: PlannerQuery => PlannerQuery): PlannerQuery = tail match {
    case None            => f(this)
    case Some(tailQuery) => this.updateTail(_.updateTailOrSelf(f))
  }

  def exists(f: PlannerQuery => Boolean): Boolean =
    f(this) || tail.exists(_.exists(f))

  def ++(other: PlannerQuery): PlannerQuery =
    PlannerQuery(
      graph = graph ++ other.graph,
      projection = projection ++ other.projection,
      tail = either(tail, other.tail)
    )

  private def either[T](a: Option[T], b: Option[T]): Option[T] = (a, b) match {
    case (Some(_), Some(_)) => throw new InternalException("Can't join two query graphs with different SKIP")
    case (s@Some(_), None) => s
    case (None, s) => s
  }
}

object PlannerQuery {
  def apply(graph: QueryGraph = QueryGraph.empty,
            projection: QueryProjection = QueryProjection.empty,
            tail: Option[PlannerQuery] = None): PlannerQuery = PlannerQueryImpl(graph, projection, tail)

  val empty = PlannerQuery()

  def coveredIdsForPatterns(patternNodeIds: Set[IdName], patternRels: Set[PatternRelationship]) = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }
}

case class PlannerQueryImpl(graph: QueryGraph,
                            projection: QueryProjection,
                            tail: Option[PlannerQuery] = None)
  extends PlannerQuery with GeneratedPretty with PrettyToString {

  def withTail(newTail: PlannerQuery): PlannerQuery = tail match {
    case None => copy(tail = Some(newTail))
    case Some(_) => throw new InternalException("Attempt to set a second tail on a query graph")
  }

  def updateTail(f: PlannerQuery => PlannerQuery) = tail match {
    case None            => this
    case Some(tailQuery) => copy(tail = Some(f(tailQuery)))
  }

  def withProjection(projection: QueryProjection): PlannerQuery = copy(projection = projection)

  def withGraph(graph: QueryGraph): PlannerQuery = copy(graph = graph)
}

