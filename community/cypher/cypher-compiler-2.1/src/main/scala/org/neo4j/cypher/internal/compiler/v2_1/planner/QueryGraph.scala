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

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.graphdb.Direction

case class QueryGraph(maxId: Id,
                      edges: Seq[QueryEdge],
                      selections: Seq[(Id, Selection)],
                      projection: Seq[(Expression, Int)]) {

  // build bitmap of connected nodes

  val selectionsByNode: Map[Id, Seq[Selection]] =
    selections.groupBy(_._1).mapValues(_.map(_._2)).withDefault(_ => Seq.empty)

  val graphRelsById: Map[Id, Seq[GraphRelationship]] =
    edges.collect {
      case edge: GraphRelationship if edge.start == edge.end =>
        Seq(edge.start -> edge)
      case edge: GraphRelationship =>
        Seq(edge.start -> edge, edge.end -> edge)
    }.flatten.groupBy(_._1).mapValues(_.map(_._2))
}

object QueryEdge {
  case class Key(start: Id, end: Id)
}

object QueryEdgeKey {
  def apply(start: Id, end: Id): QueryEdge.Key =
    if (start <= end) QueryEdge.Key(start, end) else QueryEdge.Key(end, start)
}

trait QueryEdge {
  def start: Id
  def end: Id
  
  val key = QueryEdgeKey(start, end)

  def other(id: Id) =
    if (start == id)
      end
    else if (end == id)
      start
    else
      throw new IllegalArgumentException("Provided Id is neither the start nor the end Id")
}

trait Comparison
case object Equal extends Comparison
case object NotEqual extends Comparison
case object LargerThan extends Comparison
case object LargerThanOrEqual extends Comparison

case class PropertyJoin(start: Id, end: Id, startPropertyId: PropertyKey, endPropertyId: PropertyKey, comparison: Comparison) extends QueryEdge
case class GraphRelationship(start: Id, end: Id, direction: Direction, types: Seq[RelationshipType]) extends QueryEdge

trait Selection
case class NodePropertySelection(property: PropertyKey, value: Option[Any], comparison: Comparison) extends Selection
case class NodeLabelSelection(label: Label) extends Selection

case class Id(id: Int) extends AnyVal {
  def <=(other: Id) = id < other.id
  def allIncluding: Seq[Id] = 0.to(id).map(Id)
}

case class PropertyKey(name: String) extends AnyVal
case class RelationshipType(name: String) extends AnyVal
case class Label(name: String) extends AnyVal
case class Token(value: Int)
