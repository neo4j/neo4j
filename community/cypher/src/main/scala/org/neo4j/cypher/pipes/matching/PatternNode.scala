/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import scala.collection.JavaConverters._
import org.neo4j.graphdb.{DynamicRelationshipType, Relationship, Direction, Node}

class PatternNode(key: String) extends PatternElement(key) with PinnablePatternElement[Node] {
  val relationships = scala.collection.mutable.Set[PatternRelationship]()

  def getPRels(history: Seq[MatchingPair]): Seq[PatternRelationship] = relationships.filterNot( r => history.exists(_.matches(r)) ).toSeq

  def getRealRelationships(node: Node, pRel: PatternRelationship, history:Seq[MatchingPair]): Seq[Relationship] = {
    val relationships = pRel.getRealRelationships(this, node)
    relationships.asScala.filterNot( r => history.exists(_.matches(r)) ).toSeq
  }

  def relateTo(key: String, other: PatternNode, relType: Option[String], dir: Direction): PatternRelationship = {
    val rel = new PatternRelationship(key, this, other, relType, dir)
    relationships.add(rel)
    other.relationships.add(rel)

    rel
  }
}
