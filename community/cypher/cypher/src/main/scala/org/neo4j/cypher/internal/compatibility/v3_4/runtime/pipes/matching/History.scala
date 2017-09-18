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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.matching

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{EdgeValue, ListValue, NodeValue}

import scala.collection.Set

/**
  * This class is responsible for keeping track of the already visited parts of the pattern, and the matched
  * entities corresponding to the pattern items.
  *
  * It's also used to emit the subgraph when the whole pattern has been matched (that's the toMap method)
  */
abstract class History {

  def removeSeen(relationships: Set[PatternRelationship]): Set[PatternRelationship] =
    relationships.filterNot(r => hasSeen(r))

  def removeSeen(relationships: Seq[GraphRelationship]): Seq[GraphRelationship] = relationships.filterNot {
    case SingleGraphRelationship(r) => hasSeen(r)
    case VariableLengthGraphRelationship(p) => hasSeen(p)
  }

  def hasSeen(p: Any): Boolean

  def add(pair: MatchingPair): History

  val toMap: ExecutionContext

  def contains(p: MatchingPair): Boolean

}

class InitialHistory(source: ExecutionContext, alreadySeen: Seq[EdgeValue]) extends History {

  def hasSeen(p: Any) = p match {
    case r: EdgeValue => alreadySeen.contains(r)
    case _ => false
  }

  def contains(p: MatchingPair) = false

  def add(pair: MatchingPair) = new AddedHistory(this, pair)

  val toMap = source
}

class AddedHistory(val parent: History, val pair: MatchingPair) extends History {

  def hasSeen(p: Any) = pair.matches(p) || parent.hasSeen(p)

  def contains(p: MatchingPair) = pair == p || parent.contains(p)

  def add(pair: MatchingPair) = if (contains(pair)) this else new AddedHistory(this, pair)

  lazy val toMap = {
    parent.toMap.newWith(toIndexedSeq(pair))
  }

  def toIndexedSeq(p: MatchingPair): Seq[(String, AnyValue)] = {
    p match {
      case MatchingPair(pe: PatternNode, entity: NodeValue) => Seq(pe.key -> entity)
      case MatchingPair(pe: PatternRelationship, entity: SingleGraphRelationship) => Seq(pe.key -> entity.rel)
      case MatchingPair(pe: VariableLengthPatternRelationship, null) => Seq(pe.key -> Values.NO_VALUE) ++ pe.relIterable
        .map(_ -> Values.NO_VALUE)
      case MatchingPair(pe: PatternRelationship, null) => Seq(pe.key -> Values.NO_VALUE)
      case MatchingPair(pe: VariableLengthPatternRelationship, entity: VariableLengthGraphRelationship) => {
        relationshipIterable(pe, entity) match {
          case Some((key, rels)) => Seq(pe.key -> entity.path, (key, rels))
          case None => Seq(pe.key -> entity.path)
        }

      }
    }
  }

  private def relationshipIterable(pe: VariableLengthPatternRelationship,
                                   entity: VariableLengthGraphRelationship): Option[(String, ListValue)]
  = pe.relIterable.map(_ -> entity.relationships)
}
