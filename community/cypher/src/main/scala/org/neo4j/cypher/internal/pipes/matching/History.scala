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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb.Node

/**
 * This class is responsible for keeping track of the already visited parts of the pattern, and the matched
 * entities corresponding to the pattern items.
 *
 * It's also used to emit the subgraph when the whole pattern has been matched (that's the toMap method)
 */
class History(source:Map[String,Any], seen: Set[MatchingPair]=Set()) {
  def filter(relationships: Set[PatternRelationship]): Set[PatternRelationship] = relationships.filterNot(r => seen.exists(_.matches(r)))

  def filter(relationships: Seq[GraphRelationship]): Seq[GraphRelationship] = relationships.filterNot(gr => gr match {
    case SingleGraphRelationship(r) => seen.exists(h => h.matches(r))
    case VariableLengthGraphRelationship(p) => seen.exists(h => h.matches(p))
  }).toSeq

  def add(pair: MatchingPair): History = new History(source, seen ++ Seq(pair))

  def toMap: Map[String, Any] = source ++ seen.flatMap(_ match {
      case MatchingPair(pe: PatternNode, entity: Node) => Seq(pe.key -> entity)
      case MatchingPair(pe: PatternRelationship, entity: SingleGraphRelationship) => Seq(pe.key -> entity.rel)
      case MatchingPair(pe: VariableLengthPatternRelationship, null) => Seq(pe.key -> null) ++ pe.relIterable.map( _ -> null)
      case MatchingPair(pe: PatternRelationship, null) => Seq(pe.key -> null)
      case MatchingPair(pe: VariableLengthPatternRelationship, entity: VariableLengthGraphRelationship) => Seq(pe.key -> entity.path) ++ relationshipIterable(pe, entity)
  }).toMap

  private def relationshipIterable(pe: VariableLengthPatternRelationship, entity: VariableLengthGraphRelationship):Option[(String, Any)] = pe.relIterable match {
    case None => None
    case Some(relIterable) => Some(relIterable -> entity.relationships)
  }

  override def toString: String = "History(%s)".format(seen.mkString("[", "], [", "]"))
}