/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.util.concurrent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertTimeout;

class AsyncEventsTest
{
    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdown();
    }

    static class Event extends AsyncEvent
    {
        Thread processedBy;
    }

    static class EventConsumer implements Consumer<Event>
    {
        final BlockingQueue<Event> eventsProcessed = new LinkedBlockingQueue<>();

        @Override
        public void accept( Event event )
        {
            event.processedBy = Thread.currentThread();
            eventsProcessed.offer( event );
        }

        public Event poll( long timeout, TimeUnit unit ) throws InterruptedException
        {
            return eventsProcessed.poll( timeout, unit );
        }
    }

    @Test
    void eventsMustBeProcessedByBackgroundThread() throws Exception
    {
        EventConsumer consumer = new EventConsumer();

        AsyncEvents<Event> asyncEvents = new AsyncEvents<>( consumer, AsyncEvents.Monitor.NONE );
        executor.submit( asyncEvents );

        Event firstSentEvent = new Event();
        asyncEvents.send( firstSentEvent );
        Event firstProcessedEvent = consumer.poll( 10, TimeUnit.SECONDS );

        Event secondSentEvent = new Event();
        asyncEvents.send( secondSentEvent );
        Event secondProcessedEvent = consumer.poll( 10, TimeUnit.SECONDS );

        asyncEvents.shutdown();

        assertThat( firstProcessedEvent, is( firstSentEvent ) );
        assertThat( secondProcessedEvent, is( secondSentEvent ) );
    }

    @Test
    void mustNotProcessEventInSameThreadWhenNotShutDown() throws Exception
    {
        EventConsumer consumer = new EventConsumer();

        AsyncEvents<Event> asyncEvents = new AsyncEvents<>( consumer, AsyncEvents.Monitor.NONE );
        executor.submit( asyncEvents );

        asyncEvents.send( new Event() );

        Thread processingThread = consumer.poll( 10, TimeUnit.SECONDS ).processedBy;
        asyncEvents.shutdown();

        assertThat( processingThread, is( not( Thread.currentThread() ) ) );
    }

    @Test
    void mustProcessEventsDirectlyWhenShutDown()
    {
        assertTimeout( ofSeconds( 10 ), () ->
        {
            EventConsumer consumer = new EventConsumer();

            AsyncEvents<Event> asyncEvents = new AsyncEvents<>( consumer, AsyncEvents.Monitor.NONE );
            executor.submit( asyncEvents );

            asyncEvents.send( new Event() );
            Thread threadForFirstEvent = consumer.poll( 10, TimeUnit.SECONDS ).processedBy;
            asyncEvents.shutdown();

            assertThat( threadForFirstEvent, is( not( Thread.currentThread() ) ) );

            Thread threadForSubsequentEvents;
            do
            {
                asyncEvents.send( new Event() );
                threadForSubsequentEvents = consumer.poll( 10, TimeUnit.SECONDS ).processedBy;
            }
            while ( threadForSubsequentEvents != Thread.currentThread() );
        } );
    }

    @Test
    void concurrentlyPublishedEventsMustAllBeProcessed()
    {
        assertTimeout( ofSeconds( 10 ), () ->
        {
            EventConsumer consumer = new EventConsumer();
            final CountDownLatch startLatch = new CountDownLatch( 1 );
            final int threads = 10;
            final int iterations = 2_000;
            final AsyncEvents<Event> asyncEvents = new AsyncEvents<>( consumer, AsyncEvents.Monitor.NONE );
            executor.submit( asyncEvents );

            ExecutorService threadPool = Executors.newFixedThreadPool( threads );
            Runnable runner = () ->
            {
                try
                {
                    startLatch.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }

                for ( int i = 0; i < iterations; i++ )
                {
                    asyncEvents.send( new Event() );
                }
            };
            for ( int i = 0; i < threads; i++ )
            {
                threadPool.submit( runner );
            }
            startLatch.countDown();

            Thread thisThread = Thread.currentThread();
            int eventCount = threads * iterations;
            try
            {
                for ( int i = 0; i < eventCount; i++ )
                {
                    Event event = consumer.poll( 1, TimeUnit.SECONDS );
                    if ( event == null )
                    {
                        i--;
                    }
                    else
                    {
                        assertThat( event.processedBy, is( not( thisThread ) ) );
                    }
                }
            }
            finally
            {
                asyncEvents.shutdown();
            }
        } );
    }

    @Test
    void awaitingShutdownMustBlockUntilAllMessagesHaveBeenProcessed() throws Exception
    {
        final Event specialShutdownObservedEvent = new Event();
        final CountDownLatch awaitStartLatch = new CountDownLatch( 1 );
        final EventConsumer consumer = new EventConsumer();
        final AsyncEvents<Event> asyncEvents = new AsyncEvents<>( consumer, AsyncEvents.Monitor.NONE );
        executor.submit( asyncEvents );

        // Wait for the background thread to start processing events
        do
        {
            asyncEvents.send( new Event() );
        }
        while ( consumer.eventsProcessed.take().processedBy == Thread.currentThread() );

        // Start a thread that awaits the termination
        Future<?> awaitShutdownFuture = executor.submit( () ->
        {
            awaitStartLatch.countDown();
            asyncEvents.awaitTermination();
            consumer.eventsProcessed.offer( specialShutdownObservedEvent );
        } );

        awaitStartLatch.await();

        // Send 5 events
        asyncEvents.send( new Event() );
        asyncEvents.send( new Event() );
        asyncEvents.send( new Event() );
        asyncEvents.send( new Event() );
        asyncEvents.send( new Event() );

        // Observe 5 events processed
        assertThat( consumer.eventsProcessed.take(), is( notNullValue() ) );
        assertThat( consumer.eventsProcessed.take(), is( notNullValue() ) );
        assertThat( consumer.eventsProcessed.take(), is( notNullValue() ) );
        assertThat( consumer.eventsProcessed.take(), is( notNullValue() ) );
        assertThat( consumer.eventsProcessed.take(), is( notNullValue() ) );

        // Observe no events left
        assertThat( consumer.eventsProcessed.poll( 20, TimeUnit.MILLISECONDS ), is( nullValue() ) );

        // Shutdown and await termination
        asyncEvents.shutdown();
        awaitShutdownFuture.get();

        // Observe termination
        assertThat( consumer.eventsProcessed.take(), sameInstance( specialShutdownObservedEvent ) );
    }
}
