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
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CheckPointerImplTest
{
    private static final NullLogProvider NULL_LOG_PROVIDER = NullLogProvider.getInstance();

    private final LogFile logFile = mock( LogFile.class );
    private final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
    private final CheckPointThreshold threshold = mock( CheckPointThreshold.class );
    private final StoreFlusher flusher = mock( StoreFlusher.class );
    private final LogPruning logPruning = mock( LogPruning.class );
    private final KernelHealth health = mock( KernelHealth.class );
    private final CheckPointTracer tracer = mock( CheckPointTracer.class, RETURNS_MOCKS );

    @Test
    public void shouldNotFlushIfItIsNotNeeded() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, health,
                NULL_LOG_PROVIDER, tracer, logFile );
        when( threshold.isCheckPointingNeeded() ).thenReturn( false );

        checkPointing.start();

        // When
        checkPointing.checkPointIfNeeded();

        // Then
        verifyZeroInteractions( flusher );
        verifyZeroInteractions( tracer );
    }

    @Test
    public void shouldFlushIfItIsTimeAndNoOneElseDidItBeforeUs() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, health,
                NULL_LOG_PROVIDER, tracer, logFile );
        WritableLogChannel channel = spy( new InMemoryLogChannel() );
        when( logFile.getWriter() ).thenReturn( channel );
        when( logFile.currentLogVersion() ).thenReturn( 17l );
        when( threshold.isCheckPointingNeeded() ).thenReturn( true, false );
        when( txIdStore.getLastCommittedTransaction() ).thenReturn( new long[]{42l, 0l, 16l, 233l} );

        checkPointing.start();

        // When
        checkPointing.checkPointIfNeeded();

        // Then
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( channel, atLeastOnce() ).put( anyByte() );
        verify( channel, atLeastOnce() ).putLong( anyLong() );
        verify( channel, times( 1 ) ).emptyBufferIntoChannelAndClearIt();
        verify( channel, times( 1 ) ).force();
        verify( threshold, times( 1 ) ).checkPointHappened( 42l );
        verify( threshold, times( 1 ) ).isCheckPointingNeeded();
        verify( logPruning, times( 1 ) ).pruneLogs( 16l );
        verify( tracer, times( 1 ) ).beginCheckPoint();
        verifyNoMoreInteractions( flusher, health, channel, threshold, tracer );
    }

    @Test
    public void shouldKernelPanicIfNotAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, health,
                NULL_LOG_PROVIDER, tracer, logFile );
        WritableLogChannel channel = mock( WritableLogChannel.class, RETURNS_MOCKS );
        when( logFile.getWriter() ).thenReturn( channel );
        when( logFile.currentLogVersion() ).thenReturn( 17l );
        when( threshold.isCheckPointingNeeded() ).thenReturn( true, false );
        when( txIdStore.getLastCommittedTransaction() ).thenReturn( new long[]{42l, 0l, 16l, 233l} );
        IOException ioex = new IOException( "boom!" );
        doThrow( ioex ).when( channel ).force();

        checkPointing.start();

        // When
        try
        {
            checkPointing.checkPointIfNeeded();
            fail( "should have thrown " );
        }
        catch ( IOException ex )
        {
            assertEquals( ioex, ex );
        }

        // Then
        verify( health, times( 1 ) ).panic( ioex );
    }
}
