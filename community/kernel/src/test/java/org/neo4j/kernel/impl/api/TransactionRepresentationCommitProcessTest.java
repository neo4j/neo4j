/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TestableTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

public class TransactionRepresentationCommitProcessTest
{
    private final CommitEvent commitEvent = CommitEvent.NULL;

    @Test
    public void shouldFailWithProperMessageOnAppendException() throws Exception
    {
        // GIVEN
        TransactionAppender appender = mock( TransactionAppender.class );
        IOException rootCause = new IOException( "Mock exception" );
        doThrow( new IOException( rootCause ) ).when( appender ).append( any( TransactionToApply.class ),
                any( LogAppendEvent.class ) );
        StorageEngine storageEngine = mock( StorageEngine.class );
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                appender,
                storageEngine );

        // WHEN
        try
        {
            commitProcess.commit( mockedTransaction(), commitEvent, INTERNAL );
            fail( "Should have failed, something is wrong with the mocking in this test" );
        }
        catch ( TransactionFailureException e )
        {
            assertThat( e.getMessage(), containsString( "Could not append transaction representation to log" ) );
            assertTrue( contains( e, rootCause.getMessage(), rootCause.getClass() ) );
        }
    }

    @Test
    public void shouldCloseTransactionRegardlessOfWhetherOrNotItAppliedCorrectly() throws Exception
    {
        // GIVEN
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionAppender appender = new TestableTransactionAppender( transactionIdStore );
        long txId = 11;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        IOException rootCause = new IOException( "Mock exception" );
        StorageEngine storageEngine = mock( StorageEngine.class );
        doThrow( new IOException( rootCause ) ).when( storageEngine ).apply(
                any( TransactionToApply.class ), any( TransactionApplicationMode.class ) );
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                appender,
                storageEngine );
        TransactionToApply transaction = mockedTransaction();

        // WHEN
        try
        {
            commitProcess.commit( transaction, commitEvent, INTERNAL );
        }
        catch ( TransactionFailureException e )
        {
            assertThat( e.getMessage(), containsString( "Could not apply the transaction to the store" ) );
            assertTrue( contains( e, rootCause.getMessage(), rootCause.getClass() ) );
        }

        // THEN
        // we can't verify transactionCommitted since that's part of the TransactionAppender, which we have mocked
        verify( transactionIdStore, times( 1 ) ).transactionClosed( eq( txId ), anyLong(), anyLong() );
    }

    @Test
    public void shouldSuccessfullyCommitTransactionWithNoCommands() throws Exception
    {
        // GIVEN
        long txId = 11;
        long commitTimestamp = System.currentTimeMillis();
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionAppender appender = new TestableTransactionAppender( transactionIdStore );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );

        StorageEngine storageEngine = mock( StorageEngine.class );

        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                appender, storageEngine );
        PhysicalTransactionRepresentation noCommandTx = new PhysicalTransactionRepresentation( Collections.emptyList() );
        noCommandTx.setHeader( new byte[0], -1, -1, -1, -1, -1, -1 );

        // WHEN

        commitProcess.commit( new TransactionToApply( noCommandTx ), commitEvent, INTERNAL );

        verify( transactionIdStore ).transactionCommitted( txId, FakeCommitment.CHECKSUM, FakeCommitment.TIMESTAMP );
    }

    private TransactionToApply mockedTransaction()
    {
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        return new TransactionToApply( transaction );
    }
}
