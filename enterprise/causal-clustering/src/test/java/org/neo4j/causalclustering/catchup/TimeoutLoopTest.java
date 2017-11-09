/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup;

import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.logging.NullLog;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
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
    public void shouldTranslateExecutionExceptionToCatchUpClientException() throws Exception
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
