/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimeoutLoopTest
{
    @Test
    public void shouldReturnImmediatelyIfFutureIsAlreadyComplete() throws Exception
    {
        // given
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.complete( 12L );
        Supplier<Long> lastResponseSupplier = () -> 1L;

        // when
        long value = TimeoutLoop.<Long>waitForCompletion( future, lastResponseSupplier, 2, MILLISECONDS );

        // then
        assertEquals( 12L, value );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTimeoutIfNoActivity() throws Exception
    {
        // given
        CompletableFuture<Long> future = mock( CompletableFuture.class );
        when( future.get( anyLong(), any( TimeUnit.class ) ) ).thenThrow( TimeoutException.class ).thenReturn( 12L );

        Supplier<Long> lastResponseSupplier = () -> 5L;

        try
        {
            // when
            TimeoutLoop.<Long>waitForCompletion( future, lastResponseSupplier, 1, MILLISECONDS );
            fail( "Should have timed out" );
        }
        catch ( CatchUpClientException e )
        {
            // then
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldKeepWaitingIfThereIsSomeActivity() throws Exception
    {
        // given
        CompletableFuture<Long> future = mock( CompletableFuture.class );
        when( future.get( anyLong(), any( TimeUnit.class ) ) ).thenThrow( TimeoutException.class ).thenReturn( 12L );

        Supplier<Long> lastResponseSupplier = () -> 1L;

        // when
        long value = TimeoutLoop.<Long>waitForCompletion( future, lastResponseSupplier, 2, MILLISECONDS );

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
            TimeoutLoop.<Long>waitForCompletion( future, () -> 1L, 2, MILLISECONDS );
            fail( "Should have thrown exception" );
        }
        catch ( CatchUpClientException e )
        {
            // expected
        }
    }
}
