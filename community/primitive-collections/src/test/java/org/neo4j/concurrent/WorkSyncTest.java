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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WorkSyncTest
{
    private static void usleep( long micros )
    {
        long deadline = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos( micros );
        long now;
        do
        {
            now = System.nanoTime();
        }
        while ( now < deadline );
    }

    private static class AddWork implements Work<Adder, AddWork>
    {
        private int delta;

        private AddWork( int delta )
        {
            this.delta = delta;
        }

        @Override
        public AddWork combine( AddWork work )
        {
            delta += work.delta;
            return this;
        }

        @Override
        public void apply( Adder adder )
        {
            usleep( 50 );
            adder.add( delta );
        }
    }

    private class Adder
    {
        public void add( int delta )
        {
            sum.getAndAdd( delta );
            count.getAndIncrement();
        }
    }

    private class RunnableWork implements Runnable
    {
        private final AddWork addWork;

        public RunnableWork( AddWork addWork )
        {
            this.addWork = addWork;
        }

        @Override
        public void run()
        {
            sync.apply( addWork );
        }
    }

    private AtomicInteger sum = new AtomicInteger();
    private AtomicInteger count = new AtomicInteger();
    private Adder adder = new Adder();
    private WorkSync<Adder,AddWork> sync = new WorkSync<>( adder );

    @Test
    public void mustApplyWork() throws Exception
    {
        sync.apply( new AddWork( 10 ) );
        assertThat( sum.get(), is( 10 ) );

        sync.apply( new AddWork( 20 ) );
        assertThat( sum.get(), is( 30 ) );
    }

    @Test
    public void mustCombineWork() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( 64 );
        for ( int i = 0; i < 1000; i++ )
        {
            executor.execute( new RunnableWork( new AddWork( 1 )) );
        }
        executor.shutdown();
        assertTrue( executor.awaitTermination( 2, TimeUnit.SECONDS ) );

        assertThat( count.get(), lessThan( sum.get() ) );
    }

    @Test
    public void mustApplyWorkEvenWhenInterrupted() throws Exception
    {
        Thread.currentThread().interrupt();

        sync.apply( new AddWork( 10 ) );

        assertThat( sum.get(), is( 10 ) );
        assertTrue( Thread.interrupted() );
    }

    @Test( timeout = 1000 )
    public void mustRecoverFromExceptions() throws Exception
    {
        final AtomicBoolean broken = new AtomicBoolean( true );
        Adder adder = new Adder()
        {
            @Override
            public void add( int delta )
            {
                if ( broken.get() )
                {
                    throw new IllegalStateException( "boom!" );
                }
                super.add( delta );
            }
        };
        sync = new WorkSync<>( adder );

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            // Run this in a different thread to account for reentrant locks.
            executor.submit( new RunnableWork( new AddWork( 10 ) ) ).get();
            fail( "Should have thrown" );
        }
        catch ( ExecutionException exception )
        {
            assertThat( exception.getCause(), instanceOf( IllegalStateException.class ) );
        }

        broken.set( false );
        sync.apply( new AddWork( 20 ) );

        assertThat( sum.get(), is( 20 ) );
        assertThat( count.get(), is( 1 ) );
    }
}
