package org.neo4j.cypher.pipes.matching

import org.neo4j.graphdb.{Node, PropertyContainer}

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

case class MatchingPair(patternElement: PatternElement, entity: Any) {
  def matches(x: Any) = entity == x || patternElement == x

  override def toString = {
    val value = entity match {
      case propC: PropertyContainer => propC.getProperty("name").toString
      case null => "null"
      case x => x.toString
    }

    patternElement.key + "/" + value
  }

  def getGraphRelationships(pRel:PatternRelationship, history:Seq[MatchingPair]) = patternElement.asInstanceOf[PatternNode].getGraphRelationships(entity.asInstanceOf[Node], pRel, history)

  def getPatternAndGraphPoint: (PatternNode, Node) = (patternElement.asInstanceOf[PatternNode], entity.asInstanceOf[Node])

}