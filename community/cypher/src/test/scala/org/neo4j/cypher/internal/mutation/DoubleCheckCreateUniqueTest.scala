/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.test.ImpermanentGraphDatabase
import java.lang.Iterable
import org.neo4j.graphdb.Traverser.Order
import org.neo4j.graphdb._
import org.neo4j.cypher.internal.pipes.{QueryState}
import collection.JavaConverters._
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext
import org.neo4j.cypher.internal.ExecutionContext

/*
This test tries to set up a situation where CREATE UNIQUE would fail, unless we guard with locks to prevent creating
multiple relationships.

It does so by using a decorator around ImpermanentGraphDatabase, so directly after CREATE UNIQUE has done
getRelationships on a node, we'll create a new relationship.
*/

class DoubleCheckCreateUniqueTest extends Assertions {
  var done = false
  val db = new ImpermanentGraphDatabase() with TripIt

  @Test def double_check_unique() {

    db.afterGetRelationship = createRel

    val tx = db.beginTx

    val a = try {
      val a = db.createNode()

      relateAction.exec(createExecutionContext(a, tx), createQueryState(tx))

      tx.success()
      a
    } finally {
      tx.finish()
    }

    assert(a.getRelationships.asScala.size === 1)
  }

  val relateAction = CreateUniqueAction(UniqueLink("a", "b", "r", "X", Direction.OUTGOING))


  private def createExecutionContext(a: Node, tx: Transaction): ExecutionContext = {
    ExecutionContext(state = createQueryState(tx)).newWith(Map("a" -> a))
  }

  private def createQueryState(tx: Transaction): QueryState = {
    new QueryState(db, new GDSBackedQueryContext(db), Map.empty, Some(tx))
  }

  private def createRel(node:Node) {
    if (!done) {
      done = true
      val x = db.createNode()
      node.createRelationshipTo(x, DynamicRelationshipType.withName("X"))
    }

  }
}

trait TripIt extends GraphDatabaseService {
  var afterGetRelationship: Node => Unit = (n) => {}

  abstract override def createNode(): Node = {
    val n = super.createNode()
    new PausingNode(n, afterGetRelationship)
  }
}

class PausingNode(n: Node, afterGetRelationship: Node => Unit) extends Node {
  def getId: Long = n.getId

  def delete() {
    ???
  }

  def getRelationships: Iterable[Relationship] = {
    val rels = n.getRelationships.asScala.toList
    afterGetRelationship(n)
    rels.toIterable.asJava
  }

  def hasRelationship: Boolean = ???

  def getRelationships(types: RelationshipType*): Iterable[Relationship] = ???

  def getRelationships(direction: Direction, types: RelationshipType*): Iterable[Relationship] = getRelationships

  def hasRelationship(types: RelationshipType*): Boolean = ???

  def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = ???

  def getRelationships(dir: Direction): Iterable[Relationship] = ???

  def hasRelationship(dir: Direction): Boolean = ???

  def getRelationships(`type`: RelationshipType, dir: Direction): Iterable[Relationship] = {
    val rels =  n.getRelationships(`type`, dir).asScala.toList
    afterGetRelationship(n)
    rels.toIterable.asJava
  }


  def hasRelationship(`type`: RelationshipType, dir: Direction): Boolean = ???

  def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = ???

  def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = {
    n.createRelationshipTo(otherNode, `type`)
  }

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, relationshipType: RelationshipType, direction: Direction): Traverser = ???

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, firstRelationshipType: RelationshipType, firstDirection: Direction, secondRelationshipType: RelationshipType, secondDirection: Direction): Traverser = ???

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, relationshipTypesAndDirections: AnyRef*): Traverser = ???

  def getGraphDatabase: GraphDatabaseService = ???

  def hasProperty(key: String): Boolean = ???

  def getProperty(key: String): AnyRef = ???

  def getProperty(key: String, defaultValue: Any): AnyRef = ???

  def setProperty(key: String, value: Any) {
    ???
  }

  def removeProperty(key: String): AnyRef = ???

  def getPropertyKeys: Iterable[String] = ???

  def getPropertyValues: Iterable[AnyRef] = ???

  def addLabel(label: Label) { ??? }

  def hasLabel(label: Label) = ???
}