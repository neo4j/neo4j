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
package org.neo4j.kernel.api;

import org.junit.Test;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KernelTransactionImplementationTest
{
    @Test
    public void shouldBeAbleToRollbackTransactionThatFailsToCommit() throws Exception
    {
        // given
        AbstractTransactionManager transactionManager = mock( AbstractTransactionManager.class );
        when( transactionManager.getTransactionState()).thenReturn( mock( TransactionState.class ) );
        doThrow( new TransactionFailureException( "asd" ) ).when( transactionManager ).commit();

        KernelTransactionImplementation tx = new KernelTransactionImplementation( null, null, false, null, null, null,
                null, transactionManager, null, null, null, null, null, mock( NeoStore.class ));
        // when
        try
        {
            tx.commit();
            fail( "expected exception" );
        }
        catch ( TransactionFailureException e )
        {
            // ok
        }

        // then (no exception)
        tx.rollback();
    }
}
