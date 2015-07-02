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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CheckPointerImplTest
{
    private static final NullLogProvider NULL_LOG_PROVIDER = NullLogProvider.getInstance();

    private final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
    private final CheckPointThreshold threshold = mock( CheckPointThreshold.class );
    private final StoreFlusher flusher = mock( StoreFlusher.class );
    private final LogPruning logPruning = mock( LogPruning.class );
    private final TransactionAppender appender = mock( TransactionAppender.class );
    private final KernelHealth health = mock( KernelHealth.class );
    private final CheckPointTracer tracer = mock( CheckPointTracer.class, RETURNS_MOCKS );

    @Test
    public void shouldNotFlushIfItIsNotNeeded() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, appender, health,
                NULL_LOG_PROVIDER, tracer );
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( false );

        checkPointing.start();

        // When
        checkPointing.checkPointIfNeeded();

        // Then
        verifyZeroInteractions( flusher );
        verifyZeroInteractions( tracer );
        verifyZeroInteractions( appender );
    }

    @Test
    public void shouldFlushIfItIsTime() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, appender, health,
                NULL_LOG_PROVIDER, tracer );
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( true, false );
        LogPosition logPosition = new LogPosition( 16l, 233l );
        long initialTransactionId = 2l;
        long transactionId = 42l;
        long[] triggerCommittedTransaction = {transactionId, logPosition.getLogVersion(), logPosition.getByteOffset()};
        when( txIdStore.getLastClosedTransaction() ).thenReturn( triggerCommittedTransaction );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( initialTransactionId, transactionId, transactionId );

        checkPointing.start();

        // When
        checkPointing.checkPointIfNeeded();

        // Then
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, times( 1 ) ).isCheckPointingNeeded( transactionId );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verify( tracer, times( 1 ) ).beginCheckPoint();
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }
}
