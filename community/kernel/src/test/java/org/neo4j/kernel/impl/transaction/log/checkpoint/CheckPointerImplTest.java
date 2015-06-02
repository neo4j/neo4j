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
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CheckPointerImplTest
{
    private static final NullLogProvider NULL = NullLogProvider.getInstance();

    private final LogFile logFile = mock( LogFile.class );
    private final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
    private final CheckPointThreshold enabler = mock( CheckPointThreshold.class );
    private final StoreFlusher flusher = mock( StoreFlusher.class );
    private final LogPruning logPruning = mock( LogPruning.class );
    private final KernelHealth health = mock( KernelHealth.class );
    private final LogAppendEvent logAppendEvent = mock( LogAppendEvent.class, RETURNS_MOCKS );

    @Test
    public void shouldNotFlushIfItIsNotNeeded() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( logFile, txIdStore, enabler, flusher,
                logPruning, health, NULL );
        checkPointing.start();
        when( enabler.isCheckPointingNeeded() ).thenReturn( false );

        // When
        checkPointing.checkPointIfNeeded( logAppendEvent );

        // Then
        verifyZeroInteractions( flusher );
    }

    @Test
    public void shouldNotFlushIfSomeOneFlushedBeforeUs() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( logFile, txIdStore, enabler, flusher,
                logPruning, health, NULL );
        checkPointing.start();
        when( enabler.isCheckPointingNeeded() ).thenReturn( true, false );

        // When
        checkPointing.checkPointIfNeeded( logAppendEvent );

        // Then
        verifyZeroInteractions( flusher );
    }

    @Test
    public void shouldFlushIfItIsTimeAndNoOneElseDidItBeforeUs() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( logFile, txIdStore, enabler, flusher,
                logPruning, health, NULL );
        WritableLogChannel channel = spy( new InMemoryLogChannel() );
        when( logFile.getWriter() ).thenReturn( channel );
        when( logFile.currentLogVersion() ).thenReturn( 17l );
        checkPointing.start();
        when( enabler.isCheckPointingNeeded() ).thenReturn( true, true, false );
        when( txIdStore.getLastCommittedTransaction() ).thenReturn( new long[]{42l, 0l, 16l, 233l} );

        // When
        checkPointing.checkPointIfNeeded( logAppendEvent );

        // Then
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( channel, atLeastOnce() ).put( anyByte() );
        verify( channel, atLeastOnce() ).putLong( anyLong() );
        verify( channel, times( 1 ) ).emptyBufferIntoChannelAndClearIt();
        verify( channel, times( 1 ) ).force();
        verify( enabler, times(1) ).checkPointHappened();
        verify( enabler, times(2) ).isCheckPointingNeeded();
        verify( logPruning, times(1)).pruneLogs( 16l );
        verifyNoMoreInteractions( flusher, health, channel, enabler );
    }
}
