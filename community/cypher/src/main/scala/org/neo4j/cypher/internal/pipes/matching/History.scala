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

import collection.Map
import collection.Set
import collection.mutable.{Map => MutableMap}
import org.neo4j.graphdb.{Relationship, Path, Node}
import scala.Option

/**
 * This class is responsible for keeping track of the already visited parts of the pattern, and the matched
 * entities corresponding to the pattern items.
 *
 * It's also used to emit the subgraph when the whole pattern has been matched (that's the toMap method)
 */
abstract class History {
  val seen : Set[MatchingPair]

  def filter(relationships: Set[PatternRelationship]): Set[PatternRelationship] = relationships.filterNot(r => matches(r))

  def filter(relationships: Seq[GraphRelationship]): Seq[GraphRelationship] = relationships.filterNot(gr => gr match {
    case SingleGraphRelationship(r) => matches(r)
    case VariableLengthGraphRelationship(p) => matches(p)
  })

  def matches(p : Any) : Boolean

  def add(pair: MatchingPair): History

  val toMap: Map[String, Any]

  def contains(p : MatchingPair) : Boolean
  override def toString: String = "History(%s)".format(seen.mkString("[", "], [", "]"))
}

class InitialHistory(source : Map[String,Any]) extends History {
  lazy val seen = Set[MatchingPair]()

  def matches(p: Any) = false

  def contains(p : MatchingPair) = false

  def add(pair: MatchingPair) = new AddedHistory(this,pair)

  val toMap = source
}

class AddedHistory(val parent : History, val pair : MatchingPair) extends History {
  lazy val seen = parent.seen + pair

  def matches(p: Any) = pair.matches(p) || parent.matches(p)

  def contains(p : MatchingPair) = pair == p || parent.contains(p)

  def add(pair: MatchingPair) = if (contains(pair)) this else new AddedHistory(this,pair)

  lazy val toMap = {
    parent.toMap ++ toSeq(pair)
  }

  def toSeq(p: MatchingPair) : Seq[(String,Any)] = {
    p match {
      case MatchingPair(pe: PatternNode, entity: Node) => Seq(pe.key -> entity)
      case MatchingPair(pe: PatternRelationship, entity: SingleGraphRelationship) => Seq(pe.key -> entity.rel)
      case MatchingPair(pe: PatternRelationship, null) => Seq(pe.key -> null)
      case MatchingPair(pe: VariableLengthPatternRelationship, entity: VariableLengthGraphRelationship) => {
        relationshipIterable(pe, entity) match {
          case Some(aPair) => Seq(pe.key -> entity.path, aPair)
          case None => Seq(pe.key -> entity.path)
        }

      }
    }
  }

  private def relationshipIterable(pe: VariableLengthPatternRelationship, entity: VariableLengthGraphRelationship):Option[(String, Any)] = pe.relIterable.map(_->entity.relationships)
}
