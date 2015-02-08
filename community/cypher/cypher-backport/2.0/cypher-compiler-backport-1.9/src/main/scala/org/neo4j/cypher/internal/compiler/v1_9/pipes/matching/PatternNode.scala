/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes.matching

import org.neo4j.graphdb.{Direction, Node}
import org.neo4j.cypher.internal.compiler.v1_9.commands.Predicate
import org.neo4j.cypher.internal.compiler.v1_9.spi.QueryContext

class PatternNode(key: String) extends PatternElement(key) {

  val relationships = scala.collection.mutable.Set[PatternRelationship]()

  def getPRels(history: Seq[MatchingPair]): Seq[PatternRelationship] = relationships.filterNot(r => history.exists(_.matches(r))).toSeq

  def getGraphRelationships(node: Node, pRel: PatternRelationship, ctx:QueryContext): Seq[GraphRelationship] = pRel.getGraphRelationships(this, node, ctx)

  def relateTo(key: String, other: PatternNode, relType: Seq[String], dir: Direction, optional: Boolean, predicate: Predicate): PatternRelationship = {
    val rel = new PatternRelationship(key, this, other, relType, dir, optional, predicate)
    relationships.add(rel)
    other.relationships.add(rel)
    rel
  }

  def relateViaVariableLengthPathTo(pathName: String,
                                    end: PatternNode,
                                    minHops: Option[Int],
                                    maxHops: Option[Int],
                                    relType: Seq[String],
                                    dir: Direction,
                                    collectionOfRels: Option[String],
                                    optional: Boolean,
                                    predicate: Predicate): PatternRelationship = {
    val rel = new VariableLengthPatternRelationship(pathName, this, end, collectionOfRels, minHops, maxHops, relType, dir, optional, predicate)
    relationships.add(rel)
    end.relationships.add(rel)
    rel
  }

  override def toString = String.format("PatternNode[key=%s]", key)

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T,
                  path: Seq[PatternElement]) {
    if (!path.contains(this)) {
      val moreData = visitNode(this, data)

      relationships.
        filter(shouldFollow).
        foreach(r => r.traverse(shouldFollow, visitNode, visitRelationship, moreData, this, path :+ this))
    }
  }
}
