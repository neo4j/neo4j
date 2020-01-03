/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.test.ThreadTestUtils;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class LatchMapTest
{
    private LatchMap latches = new LatchMap();

    @Test
    void takeOrAwaitLatchMustReturnLatchIfAvailable()
    {
        BinaryLatch latch = latches.takeOrAwaitLatch( 0 );
        assertThat( latch, is( notNullValue() ) );
        latch.release();
    }

    @Test
    void takeOrAwaitLatchMustAwaitExistingLatchAndReturnNull() throws Exception
    {
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        BinaryLatch latch = latches.takeOrAwaitLatch( 42 );
        assertThat( latch, is( notNullValue() ) );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<BinaryLatch> future = executor.submit( () ->
        {
            threadRef.set( Thread.currentThread() );
            return latches.takeOrAwaitLatch( 42 );
        } );
        Thread th;
        do
        {
            th = threadRef.get();
        }
        while ( th == null );
        ThreadTestUtils.awaitThreadState( th, 10_000, Thread.State.WAITING );
        latch.release();
        assertThat( future.get( 1, TimeUnit.SECONDS ), is( nullValue() ) );
    }

    @Test
    void takeOrAwaitLatchMustNotLetUnrelatedLatchesConflictTooMuch() throws Exception
    {
        BinaryLatch latch = latches.takeOrAwaitLatch( 42 );
        assertThat( latch, is( notNullValue() ) );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<BinaryLatch> future = executor.submit( () -> latches.takeOrAwaitLatch( 33 ) );
        assertThat( future.get( 1, TimeUnit.SECONDS ), is( notNullValue() ) );
        latch.release();
    }

    @Test
    void latchMustBeAvailableAfterRelease()
    {
        latches.takeOrAwaitLatch( 42 ).release();
        latches.takeOrAwaitLatch( 42 ).release();
    }
}
