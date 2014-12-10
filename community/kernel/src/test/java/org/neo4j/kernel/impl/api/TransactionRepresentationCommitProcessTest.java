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

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart.checksum;

public class TransactionRepresentationCommitProcessTest
{
    @Test
    public void shouldNotIncrementLastCommittedTxIdIfAppendFails() throws Exception
    {
        // GIVEN
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        TransactionAppender appender = mock( TransactionAppender.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        long txId = 11;
        IOException rootCause = new IOException( "Mock exception" );
        doThrow( new IOException( rootCause ) ).when( appender ).append( any( TransactionRepresentation.class ) );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                logicalTransactionStore, kernelHealth, transactionIdStore, storeApplier, INTERNAL );

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            commitProcess.commit( mockedTransaction(), locks );
            fail( "Should have failed, something is wrong with the mocking in this test" );
        }
        catch ( TransactionFailureException e )
        {
            assertThat( e.getMessage(), containsString( "Could not append transaction representation to log" ) );
            assertTrue( contains( e, rootCause.getMessage(), rootCause.getClass() ) );
        }

        verify( transactionIdStore, times( 0 ) ).transactionCommitted( txId, 0 );
    }

    @Test
    public void shouldCloseTransactionRegardlessOfWhetherOrNotItAppliedCorrectly() throws Exception
    {
        // GIVEN
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        TransactionAppender appender = mock( TransactionAppender.class );
        long txId = 11;
        when( appender.append( any( TransactionRepresentation.class ) ) ).thenReturn( txId );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        IOException rootCause = new IOException( "Mock exception" );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );
        doThrow( new IOException( rootCause ) ).when( storeApplier ).apply(
                any( TransactionRepresentation.class ), any( LockGroup.class ), eq( txId ), eq( INTERNAL ) );
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                logicalTransactionStore, kernelHealth, transactionIdStore, storeApplier, INTERNAL );
        TransactionRepresentation transaction = mockedTransaction();

        // WHEN
        try ( LockGroup locks = new LockGroup() )
        {
            commitProcess.commit( transaction, locks );
        }
        catch ( TransactionFailureException e )
        {
            assertThat( e.getMessage(), containsString( "Could not apply the transaction to the store" ) );
            assertTrue( contains( e, rootCause.getMessage(), rootCause.getClass() ) );
        }

        // THEN
        verify( transactionIdStore, times( 1 ) ).transactionCommitted( txId,
                checksum( transaction.additionalHeader(), transaction.getMasterId(), transaction.getAuthorId() ) );
        verify( transactionIdStore, times( 1 ) ).transactionClosed( txId );
        verifyNoMoreInteractions( transactionIdStore );
    }

    private TransactionRepresentation mockedTransaction()
    {
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        return transaction;
    }
}
