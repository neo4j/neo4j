/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.concurrent;

import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.fail;

public class CompletableFutureTest
{
    @Test
    public void shouldBeInitiallyUnresolved()
    {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        assertThat( future.isDone(), is( false ) );
        assertThat( future.isCancelled(), is( false ) );
    }

    @Test
    public void shouldResolveWithValue() throws Exception
    {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        future.complete( 1 );

        assertThat( future.isDone(), is( true ) );
        assertThat( future.isCancelled(), is( false ) );
        assertThat( future.get(), is( 1 ) );
    }

    @Test
    public void shouldResolveWithException() throws Exception
    {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        Throwable ex = new Exception();
        future.completeExceptionally( ex );

        assertThat( future.isDone(), is( true ) );
        assertThat( future.isCancelled(), is( false ) );
        try
        {
            future.get();
            fail( "expected exception not thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), sameInstance( ex ) );
        }
    }

    @Test
    public void shouldCancel() throws Exception
    {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        future.cancel( false );

        assertThat( future.isDone(), is( true ) );
        assertThat( future.isCancelled(), is( true ) );
        try
        {
            future.get();
            fail( "expected exception not thrown" );
        }
        catch ( CancellationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldWaitUntilResolved() throws Exception
    {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        final CountDownLatch startedLatch = new CountDownLatch( 1 );

        final Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                startedLatch.countDown();
                try
                {
                    future.get();
                }
                catch ( InterruptedException | ExecutionException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
        thread.start();

        startedLatch.await();
        Thread.sleep( 100 );
        assertThat( thread.isAlive(), is( true ) );

        future.complete( 1 );
        thread.join( 10000 );
        assertThat( thread.isAlive(), is( false ) );
    }

    @Test
    public void shouldThrowTimeoutException() throws Exception
    {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        try
        {
            future.get( 1, MILLISECONDS );
            fail( "expected exception not thrown" );
        }
        catch ( TimeoutException e )
        {
            // expected
        }
    }
}
