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
package org.neo4j.cypher.internal.compiler.v3_1.commands.expressions

import java.util
import java.lang.Iterable

import org.neo4j.graphdb._

trait FakeEntityTestSupport {

  class FakeRel(start: Node, end: Node, typ: RelationshipType) extends Relationship {

    def getId: Long = 0L

    def delete() {}

    def getStartNode: Node = start

    def getEndNode: Node = end

    def getOtherNode(node: Node): Node = null

    def getNodes: Array[Node] = null

    def getType: RelationshipType = typ

    def isType(`type`: RelationshipType): Boolean = false

    def getGraphDatabase: GraphDatabaseService = null

    def hasProperty(key: String): Boolean = false

    def getProperty(key: String): AnyRef = null

    def getProperty(key: String, defaultValue: AnyRef): AnyRef = null

    def setProperty(key: String, value: AnyRef) {}

    def removeProperty(key: String): AnyRef = null

    def getPropertyKeys: Iterable[String] = null

    def getProperties(keys: String*): util.Map[String, AnyRef] = null

    def getAllProperties: util.Map[String, AnyRef] = null

    override def toString: String = "Rel"
  }

  class FakeNode extends Node {

    def getId: Long = 0L

    def getRelationships(types: RelationshipType*): Iterable[Relationship] = null

    def delete() {}

    def getRelationships: Iterable[Relationship] = null

    def hasRelationship: Boolean = false

    def getRelationships(direction: Direction, types: RelationshipType*): Iterable[Relationship] = null

    def hasRelationship(types: RelationshipType*): Boolean = false

    def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = false

    def getRelationships(dir: Direction): Iterable[Relationship] = null

    def hasRelationship(dir: Direction): Boolean = false

    def getRelationships(`type`: RelationshipType, dir: Direction): Iterable[Relationship] = null

    def hasRelationship(`type`: RelationshipType, dir: Direction): Boolean = false

    def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = null

    def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = null

    def getGraphDatabase: GraphDatabaseService = null

    def hasProperty(key: String): Boolean = false

    def getProperty(key: String): AnyRef = null

    def getProperty(key: String, defaultValue: AnyRef): AnyRef = null

    def setProperty(key: String, value: AnyRef) {}

    def removeProperty(key: String): AnyRef = null

    def getPropertyKeys: Iterable[String] = null

    def getProperties(keys: String*): util.Map[String, AnyRef] = null

    def getAllProperties: util.Map[String, AnyRef] = null

    override def toString: String = "Node"

    def addLabel(label: Label) {
      ???
    }

    def removeLabel(label: Label) {
      ???
    }

    def hasLabel(label: Label) = ???

    def getLabels = ???

    def getRelationshipTypes = ???

    def getDegree: Int = ???

    def getDegree(direction: Direction): Int = ???

    def getDegree(relType: RelationshipType): Int = ???

    def getDegree(relType: RelationshipType, direction: Direction): Int = ???
  }

}
