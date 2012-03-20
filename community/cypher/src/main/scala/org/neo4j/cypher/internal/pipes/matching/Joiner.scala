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

import scala.collection.JavaConverters._
import org.neo4j.graphdb.{DynamicRelationshipType, Direction, Node}
import collection.{Iterable, Traversable}
import collection.Map
import org.neo4j.cypher.internal.commands.Predicate
import org.neo4j.helpers.ThisShouldNotHappenError

/*
This class performs simpler join operations, but faster than the full matcher.

By linking together a number of these joiners, the whole pattern can be matched.
Joiners can't handle loops or optional elements. Right now, they can't handle
variable length relationships, but there's nothing stopping that, in theory.
 */
class Joiner(source: Linkable,
             start: String,
             dir: Direction,
             end: String,
             relType: Option[String],
             relName: String,
             predicate: Predicate)
  extends Linkable {

  def getResult(m: Map[String, Any]): Traversable[Map[String, Any]] = source.getResult(m).flatMap(getSingleResult)

  def getSingleResult(m: Map[String, Any]): Iterable[Map[String, Any]] = {
    val startNode = m.get(start) match {
      case Some(x: Node) => x
      case _ => throw new ThisShouldNotHappenError("Andres Taylor", "The start node has to come from the underlying pipe!")
    }

    val rels = (relType match {
      case None => startNode.getRelationships(dir)
      case Some(x) => startNode.getRelationships(DynamicRelationshipType.withName(x), dir)
    }).asScala

    val between = rels.flatMap(rel => {
      val otherNode = rel.getOtherNode(startNode)

      val otherAlreadyFound = m.exists {
        case (key, value) => key != start && value == otherNode
      }

      if (otherAlreadyFound) {
        None
      }
      else {
        val product = Map(relName -> rel, end -> otherNode)
        Some(m ++ product)
      }
    })

    between.filter(predicate.isMatch)
  }

  def providesKeys(): Seq[String] = source.providesKeys() ++ Seq(relName, end)
}

class Start(val providesKeys: Seq[String]) extends Linkable {
  def getResult(m: Map[String, Any]): Traversable[Map[String, Any]] = Seq(m)
}

trait Linkable {
  def getResult(m: Map[String, Any]): Traversable[Map[String, Any]]

  def providesKeys(): Seq[String]
}