/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup;

import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeoutLoopTest
{
    @Test
    public void shouldReturnImmediatelyIfFutureIsAlreadyComplete() throws Exception
    {
        // given
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.complete( 12L );
        Supplier<Optional<Long>> lastResponseSupplier = () -> Optional.of( 1L );

        // when
        long value = TimeoutLoop.<Long>waitForCompletion( future, "", lastResponseSupplier, 2, NullLog.getInstance() );

        // then
        assertEquals( 12L, value );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTimeoutIfNoActivity() throws Exception
    {
        // given
        CompletableFuture<Long> future = mock( CompletableFuture.class );
        when( future.get( anyLong(), any( TimeUnit.class ) ) ).thenThrow( TimeoutException.class ).thenReturn( 12L );

        Supplier<Optional<Long>> lastResponseSupplier = Optional::empty;

        try
        {
            // when
            TimeoutLoop.<Long>waitForCompletion( future, "", lastResponseSupplier, 1, NullLog.getInstance() );
            fail( "Should have timed out" );
        }
        catch ( CatchUpClientException e )
        {
            // then
            // expected
            verify( future ).cancel( true );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTimeoutIfNoContinuedActivity() throws Exception
    {
        // given
        CompletableFuture<Long> future = mock( CompletableFuture.class );
        when( future.get( anyLong(), any( TimeUnit.class ) ) ).thenThrow( TimeoutException.class ).thenReturn( 12L );

        Supplier<Optional<Long>> lastResponseSupplier = () -> Optional.of( 5L );

        try
        {
            // when
            TimeoutLoop.<Long>waitForCompletion( future, "", lastResponseSupplier, 1, NullLog.getInstance() );
            fail( "Should have timed out" );
        }
        catch ( CatchUpClientException e )
        {
            // then
            // expected
            verify( future ).cancel( true );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldKeepWaitingIfThereIsSomeActivity() throws Exception
    {
        // given
        CompletableFuture<Long> future = mock( CompletableFuture.class );
        when( future.get( anyLong(), any( TimeUnit.class ) ) ).thenThrow( TimeoutException.class ).thenReturn( 12L );

        Supplier<Optional<Long>> lastResponseSupplier = () -> Optional.of( 1L );

        // when
        long value = TimeoutLoop.<Long>waitForCompletion( future, "", lastResponseSupplier, 2, NullLog.getInstance() );

        // then
        assertEquals( 12L, value );
    }

    @Test
    public void shouldTranslateExecutionExceptionToCatchUpClientException()
    {
        // given
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.completeExceptionally( new RuntimeException( "I failed to execute, sorry." ) );

        // when
        try
        {
            TimeoutLoop.<Long>waitForCompletion( future, "", () -> Optional.of( 1L ), 2, NullLog.getInstance() );
            fail( "Should have thrown exception" );
        }
        catch ( CatchUpClientException e )
        {
            // expected
        }
    }
}
