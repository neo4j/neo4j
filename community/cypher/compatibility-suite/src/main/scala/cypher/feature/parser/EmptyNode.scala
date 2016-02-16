/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.parser

import java.lang.Iterable
import java.util

import org.neo4j.graphdb._

/**
  * Supported functions:
  * - getAllProperties()
  * - getLabels()
  */
trait EmptyNode extends Node {

  override def getId: Long = ???

  override def addLabel(label: Label): Unit = ???

  override def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = ???

  override def removeLabel(label: Label): Unit = ???

  override def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = ???

  override def getRelationshipTypes: Iterable[RelationshipType] = ???

  override def delete(): Unit = ???

  override def getRelationships: Iterable[Relationship] = ???

  override def getRelationships(types: RelationshipType*): Iterable[Relationship] = ???

  override def getRelationships(direction: Direction, types: RelationshipType*): Iterable[Relationship] = ???

  override def getRelationships(dir: Direction): Iterable[Relationship] = ???

  override def getRelationships(`type`: RelationshipType, dir: Direction): Iterable[Relationship] = ???

  override def hasRelationship: Boolean = ???

  override def hasRelationship(types: RelationshipType*): Boolean = ???

  override def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = ???

  override def hasRelationship(dir: Direction): Boolean = ???

  override def hasRelationship(`type`: RelationshipType, dir: Direction): Boolean = ???

  override def getLabels: Iterable[Label]

  override def getDegree: Int = ???

  override def getDegree(`type`: RelationshipType): Int = ???

  override def getDegree(direction: Direction): Int = ???

  override def getDegree(`type`: RelationshipType, direction: Direction): Int = ???

  override def hasLabel(label: Label): Boolean = ???

  override def getProperty(key: String): AnyRef = ???

  override def getProperty(key: String, defaultValue: scala.Any): AnyRef = ???

  override def setProperty(key: String, value: scala.Any): Unit = ???

  override def getPropertyKeys: Iterable[String] = ???

  override def hasProperty(key: String): Boolean = ???

  override def getGraphDatabase: GraphDatabaseService = ???

  override def removeProperty(key: String): AnyRef = ???

  override def getProperties(keys: String*): util.Map[String, AnyRef] = ???

  override def getAllProperties: util.Map[String, AnyRef]
}

object EmptyNode {

  def newWith(labels: Iterable[Label], properties: util.Map[String, AnyRef]): Node = {
    new EmptyNode {
      override def getLabels: Iterable[Label] = labels

      override def getAllProperties: util.Map[String, AnyRef] = properties
    }
  }
}

