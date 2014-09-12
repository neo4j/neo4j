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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.RecoveryVisitor.Monitor;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        verify( storeApplier, times( 1 ) ).apply( same( representation ), any( LockGroup.class ),
                                                  eq( commitEntry.getTxId() ), eq( true ) );
        assertEquals( 1l, recoveredCount.get() );
        verify( monitor ).transactionRecovered( commitEntry.getTxId() );

        visitor.close();

        verify( store, times( 1 ) ).setLastCommittedAndClosedTransactionId( commitEntry.getTxId() );
    }
}
