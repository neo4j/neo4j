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
package org.neo4j.kernel.impl.transaction.state;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.mockito.Matchers;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.state.RecoveryVisitor;
import org.neo4j.kernel.impl.transaction.state.RecoveryVisitor.Monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.RECOVERY;

public class RecoveryVisitorTest
{
    private final TransactionIdStore store = mock( TransactionIdStore.class );
    private final TransactionRepresentationStoreApplier storeApplier =
            mock( TransactionRepresentationStoreApplier.class );

    private final AtomicInteger recoveredCount = new AtomicInteger();
    private final LogEntryStart startEntry = null;
    private final LogEntryCommit commitEntry = new OnePhaseCommit( 42, 0 );

    @Test
    public void shouldNotSetLastCommittedAndClosedTransactionIdWhenNoRecoveryHappened() throws IOException
    {
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storeApplier, recoveredCount,
                mock( RecoveryVisitor.Monitor.class ) );

        visitor.close();

        verify( store, never() ).setLastCommittedAndClosedTransactionId( anyLong() );
    }

    @Test
    public void shouldApplyVisitedTransactionToTheStoreAndSetLastCommittedAndClosedTransactionId() throws IOException
    {
        Monitor monitor = mock( RecoveryVisitor.Monitor.class );
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storeApplier, recoveredCount,
                monitor );

        final TransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.<Command>emptySet() );

        final CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( startEntry, representation, commitEntry );

        final boolean result = visitor.visit( transaction );

        assertTrue( result );
        verify( storeApplier, times( 1 ) ).apply( eq( representation ), any( LockGroup.class ),
                eq( commitEntry.getTxId() ), eq( RECOVERY ) );
        assertEquals( 1l, recoveredCount.get() );
        verify( monitor ).transactionRecovered( commitEntry.getTxId() );

        visitor.close();

        verify( store, times( 1 ) ).setLastCommittedAndClosedTransactionId( commitEntry.getTxId() );
    }

    @Test
    public void shouldNotApplyTransactionsThatAreKnownToBeClosed() throws IOException
    {
        // Given
           //
        long lastAppliedTxId = 43l;
        Monitor monitor = mock( RecoveryVisitor.Monitor.class );
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storeApplier, recoveredCount,
                monitor );

        LogEntryCommit commitEntry1 = new OnePhaseCommit( lastAppliedTxId - 1, 0 );
        LogEntryCommit commitEntry2 = new OnePhaseCommit( lastAppliedTxId, 0 );
        LogEntryCommit commitEntry3 = new OnePhaseCommit( lastAppliedTxId + 1, 0 );

        final TransactionRepresentation representation1 =
                new PhysicalTransactionRepresentation( Collections.<Command>emptySet() );
        final TransactionRepresentation representation2 =
                new PhysicalTransactionRepresentation( Collections.<Command>emptySet() );
        final TransactionRepresentation representation3 =
                new PhysicalTransactionRepresentation( Collections.<Command>emptySet() );


        final CommittedTransactionRepresentation transaction1 =
                new CommittedTransactionRepresentation( startEntry, representation1, commitEntry1 );
        final CommittedTransactionRepresentation transaction2 =
                new CommittedTransactionRepresentation( startEntry, representation2, commitEntry2 );
        final CommittedTransactionRepresentation transaction3 =
                new CommittedTransactionRepresentation( startEntry, representation3, commitEntry3 );

        when( store.getLastClosedTransactionId() ).thenReturn( lastAppliedTxId );

        boolean applicationResult;

        applicationResult = visitor.visit( transaction1 );
        assertTrue( applicationResult );
        verifyZeroInteractions( storeApplier );
        applicationResult = visitor.visit( transaction2 );
        assertTrue( applicationResult );
        verifyZeroInteractions( storeApplier );
        applicationResult = visitor.visit( transaction3 );
        assertTrue( applicationResult );
        verify( storeApplier, times( 1 ) ).apply( Matchers.<TransactionRepresentation>any(), Matchers.<LockGroup>any(), anyLong(), Matchers.<TransactionApplicationMode>any() );

        visitor.close();

        verify( store, times( 1 ) ).setLastCommittedAndClosedTransactionId( commitEntry3.getTxId() );
    }
}
