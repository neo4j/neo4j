/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;

import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.RECOVERY;

public class RecoveryVisitorTest
{
    private final TransactionIdStore store = mock( TransactionIdStore.class );
    private final TransactionRepresentationStoreApplier storeApplier =
            mock( TransactionRepresentationStoreApplier.class );
    private final IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class, RETURNS_MOCKS );
    private final RecoveryVisitor.Monitor monitor = mock( RecoveryVisitor.Monitor.class );

    private final LogEntryStart startEntry = new LogEntryStart( 1, 2, 123, 456, "tx".getBytes(),
            new LogPosition( 1, 198 ) );
    private final LogEntryCommit commitEntry = new OnePhaseCommit( 42, 0 );

    @Test
    public void shouldNotSetLastCommittedAndClosedTransactionIdWhenNoRecoveryHappened() throws IOException
    {
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storeApplier, indexUpdatesValidator, monitor );

        visitor.close();

        verify( store, never() ).setLastCommittedAndClosedTransactionId( anyLong(), anyLong() );
    }

    @Test
    public void shouldApplyVisitedTransactionToTheStoreAndSetLastCommittedAndClosedTransactionId() throws IOException
    {
        final RecoveryVisitor visitor = new RecoveryVisitor( store, storeApplier, indexUpdatesValidator, monitor );

        final TransactionRepresentation representation =
                new PhysicalTransactionRepresentation( Collections.<Command>emptySet() );

        final CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( startEntry, representation, commitEntry );

        final boolean result = visitor.visit( transaction );

        assertFalse( result );
        verify( storeApplier, times( 1 ) ).apply( eq( representation ), any( ValidatedIndexUpdates.class ),
                any( LockGroup.class ), eq( commitEntry.getTxId() ), eq( RECOVERY ) );
        verify( monitor ).transactionRecovered( commitEntry.getTxId() );

        visitor.close();

        verify( store, times( 1 ) ).setLastCommittedAndClosedTransactionId( commitEntry.getTxId(),
                LogEntryStart.checksum( startEntry ) );
    }
}
