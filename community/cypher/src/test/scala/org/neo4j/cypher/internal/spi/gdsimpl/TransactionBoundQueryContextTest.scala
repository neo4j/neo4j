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

import org.junit.Test
import gdsimpl.TransactionBoundQueryContext
import org.neo4j.graphdb.Transaction
import org.neo4j.test.ImpermanentGraphDatabase
import org.scalatest.Assertions
import org.mockito.Mockito
import org.scalatest.junit.JUnitSuite

class TransactionBoundQueryContextTest extends JUnitSuite with Assertions {
  @Test def should_mark_transaction_successful_if_successful() {
    // GIVEN
    val graph = new ImpermanentGraphDatabase
    val outerTx = Mockito.mock( classOf[Transaction] )
    Mockito.when(outerTx.failure()).thenThrow( new AssertionError( "Shouldn't be called" ) )
    val context = new TransactionBoundQueryContext(graph) {
      override val tx = outerTx
    }

    // WHEN
    context.close(true)

    // THEN
    Mockito.verify(outerTx).success()
    Mockito.verify(outerTx).finish()
    Mockito.verifyNoMoreInteractions(outerTx)
  }

  @Test def should_mark_transaction_failed_if_not_successful() {
    // GIVEN
    val graph = new ImpermanentGraphDatabase
    val outerTx = Mockito.mock( classOf[Transaction] )
    Mockito.when(outerTx.success()).thenThrow( new AssertionError( "Shouldn't be called" ) )
    val context = new TransactionBoundQueryContext(graph) {
      override val tx = outerTx
    }

    // WHEN
    context.close(false)

    // THEN
    Mockito.verify(outerTx).failure()
    Mockito.verify(outerTx).finish()
    Mockito.verifyNoMoreInteractions(outerTx)
  }
}
