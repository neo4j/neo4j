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
package org.neo4j.cypher.internal.spi

import org.junit.Ignore
import gdsimpl.TransactionBoundQueryContext
import org.hamcrest.CoreMatchers.is
import org.junit.Assert.assertThat
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.kernel.impl.api.LockHolder
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.Transaction
import org.neo4j.test.ImpermanentGraphDatabase

class RepeatableReadQueryContextContract {
  private var database: ImpermanentGraphDatabase = null
  private var innerContext: QueryContext = null
  private var node: Node = null
  private var locker: Locker = null

  @Before def init() {
    database = new ImpermanentGraphDatabase
    locker = mock(classOf[Locker])
    innerContext = new TransactionBoundQueryContext(database)
    val tx: Transaction = database.beginTx
    node = database.createNode
    val b: Node = database.createNode
    val c: Node = database.createNode
    node.createRelationshipTo(b, withName("R"))
    node.createRelationshipTo(c, withName("R"))
    tx.success()
    tx.finish()
  }

  @Test def has_property_locks_node() {
    val node: Node = createNode
    val lockingContext = new RepeatableReadQueryContext(innerContext, locker)
    lockingContext.nodeOps.hasProperty(node, "foo")
    verify(locker).readLock(node)
  }

  @Test def close_releases_locks() {
    val node: Node = createNode
    val lockingContext = new RepeatableReadQueryContext(innerContext, locker)
    lockingContext.nodeOps.hasProperty(node, "foo")
    lockingContext.close(success = true)
    verify(locker).readLock(node)
    verify(locker).releaseAllReadLocks()
  }

  @Test def get_relationships_locks_node_and_relationships() {
    val lockingContext: RepeatableReadQueryContext = new RepeatableReadQueryContext(innerContext, locker)
    val rels = lockingContext.getRelationshipsFor(node, Direction.OUTGOING, Seq())
    val count_the_matching_rows: Int = rels.size
    lockingContext.close(true)

    verify(locker).readLock(node)

    for (rel <- rels) {
      verify(locker).readLock(rel)
    }
    verify(locker).releaseAllReadLocks()

    assertThat(count_the_matching_rows, is(2))
  }

  private def createNode: Node = {
    val tx = database.beginTx
    val node = database.createNode
    tx.success()
    tx.finish()
    node
  }
}