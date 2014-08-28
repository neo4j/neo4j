/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppendException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppender;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.Exceptions.contains;

public class TransactionRepresentationCommitProcessTest
{
    @Test
    public void shouldCloseTransactionThatFailsToAppendToLogButDidGenerateTransactionId() throws Exception
    {
        // GIVEN
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        TransactionAppender appender = mock( TransactionAppender.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        long txId = 11;
        IOException rootCause = new IOException( "Mock exception" );
        doThrow( new TransactionAppendException( rootCause, txId ) )
                .when( appender ).append( any( TransactionRepresentation.class ) );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                logicalTransactionStore, kernelHealth, transactionIdStore, storeApplier, false );
        
        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        try
        {
            commitProcess.commit( transaction );
            fail( "Should have failed, something is wrong with the mocking in this test" );
        }
        catch ( TransactionFailureException e )
        {
            assertTrue( contains( e, rootCause.getMessage(), rootCause.getClass() ) );
        }
        
        verify( transactionIdStore, times( 1 ) ).transactionClosed( txId );
    }
}
