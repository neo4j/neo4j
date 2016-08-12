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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import java.util.Collections;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;

public class RecoveryVisitorTest
{
    private final TransactionIdStore store = mock( TransactionIdStore.class );
    private final StorageEngine storageEngine = mock( StorageEngine.class );
    private final RecoveryVisitor.Monitor monitor = mock( RecoveryVisitor.Monitor.class );
    private final LogEntryStart startEntry = new LogEntryStart( 1, 2, 123, 456, "tx".getBytes(),
            new LogPosition( 1, 198 ) );
    private final LogEntryCommit commitEntry = new OnePhaseCommit( 42, 0 );

    @Test
    public void shouldNotSetLastCommittedAndClosedTransactionIdWhenNoRecoveryHappened() throws Exception
    {
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storageEngine, monitor );

        visitor.close();

        verify( store, never() ).setLastCommittedAndClosedTransactionId( anyLong(), anyLong(), anyLong(), anyLong(), anyLong() );
    }

    @Test
    public void shouldApplyVisitedTransactionToTheStoreAndSetLastCommittedAndClosedTransactionId() throws Exception
    {
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storageEngine, monitor );

        final TransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.<StorageCommand>emptySet() );

        final CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( startEntry, representation, commitEntry );

        final LogPosition logPosition = new LogPosition( 0, 42 );
        final boolean result = visitor.visit( new RecoverableTransaction()
        {
            @Override
            public CommittedTransactionRepresentation representation()
            {
                return transaction;
            }

            @Override
            public LogPosition positionAfterTx()
            {
                return logPosition;
            }
        } );
        visitor.close();

        assertFalse( result );
        // TODO Perhaps we'd like to verify that it's the exact tx representation that we get to apply, no?
        verify( storageEngine, times( 1 ) ).apply( any( TransactionToApply.class ), eq( RECOVERY ) );
        verify( monitor ).transactionRecovered( commitEntry.getTxId() );

        verify( store, times( 1 ) ).setLastCommittedAndClosedTransactionId(
                commitEntry.getTxId(),
                LogEntryStart.checksum( startEntry ), commitEntry.getTimeWritten(),
                logPosition.getByteOffset(),
                logPosition.getLogVersion() );
    }
}
