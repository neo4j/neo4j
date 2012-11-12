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
package org.neo4j.cypher.pipes.matching

import org.neo4j.graphdb.Node

object History {
  def apply(): History = new History(Set())
}

class History(seen: Set[MatchingPair]) {
  def filter(relationships: Set[PatternRelationship]): Set[PatternRelationship] = relationships.filterNot(r => seen.exists(_.matches(r)))

  def filter(relationships: Seq[GraphRelationship]): Seq[GraphRelationship] = relationships.filterNot(gr => gr match {
    case SingleGraphRelationship(r) => seen.exists(h => h.matches(r))
    case VariableLengthGraphRelationship(p) => seen.exists(h => h.matches(p))
  }).toSeq

  def add(pair: MatchingPair): History = new History(seen ++ Seq(pair))

  def toMap: Map[String, Any] = seen.flatMap(_ match {
    case MatchingPair(p, e) => (p, e) match {
      case (pe: PatternNode, entity: Node) => Seq(pe.key -> entity)
      case (pe: PatternRelationship, entity: SingleGraphRelationship) => Seq(pe.key -> entity.rel)
      case (pe: PatternRelationship, null) => Seq(pe.key -> null)
      case (pe: VariableLengthPatternRelationship, entity: VariableLengthGraphRelationship) => Seq(pe.key -> entity.path)
    }
  }).toMap

  override def toString: String = "History(%s)".format(seen.mkString("[", "], [", "]"))
}