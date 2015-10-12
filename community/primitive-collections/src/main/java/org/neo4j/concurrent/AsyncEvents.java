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
package org.neo4j.concurrent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.function.Consumer;

/**
 * {@code AsyncEvents} is a mechanism for queueing up events to be processed asynchronously in a background thread.
 *
 * The {@code AsyncEvents} object implements {@link Runnable}, so it can be passed to a thread pool, or given to a
 * dedicated thread. The runnable will then occupy a thread and dedicate it to background processing of events, until
 * the {@code AsyncEvents} is {@link AsyncEvents#shutdown()}.
 *
 * If events are sent to an {@code AsyncEvents} that has been shut down, then those events will be processed in the
 * foreground as a fall-back.
 *
 * Note, however, that no events are processed until the background thread is started.
 *
 * The {@code AsyncEvents} is given a {@link Consumer} of the specified event type upon construction, and will use it
 * for doing the actual processing of events once they have been collected.
 *
 * @param <T> The type of events the {@code AsyncEvents} will process.
 */
public class AsyncEvents<T extends AsyncEvent> implements AsyncEventSender<T>, Runnable
{
    // TODO use VarHandles in Java 9
    private static AtomicReferenceFieldUpdater<AsyncEvents,AsyncEvent> STACK =
            AtomicReferenceFieldUpdater.newUpdater( AsyncEvents.class, AsyncEvent.class, "stack" );
    private static final AsyncEvent END_SENTINEL = new EndSentinel();
    private static final AsyncEvent SHUTDOWN_SENTINEL = new ShutdownSentinel();

    private final Consumer<T> eventConsumer;
    private final BinaryLatch shutdownLatch;

    @SuppressWarnings( {"unused", "FieldCanBeLocal"} )
    private volatile AsyncEvent stack; // Accessed via AtomicReferenceFieldUpdater
    private volatile Thread backgroundThread;
    private volatile boolean shutdown;

    /**
     * Construct a new {@code AsyncEvents} instance, that will use the given consumer to process the events.
     *
     * @param eventConsumer The {@link Consumer} used for processing the events that are sent in.
     */
    public AsyncEvents( Consumer<T> eventConsumer )
    {
        this.eventConsumer = eventConsumer;
        shutdownLatch = new BinaryLatch();
        stack = END_SENTINEL;
    }

    @Override
    public void send( T event )
    {
        AsyncEvent prev = STACK.getAndSet( this, event );
        assert prev != null;
        event.next = prev;
        if ( prev == END_SENTINEL )
        {
            LockSupport.unpark( backgroundThread );
        }
        else if ( prev == SHUTDOWN_SENTINEL )
        {
            AsyncEvent events = STACK.getAndSet( this, SHUTDOWN_SENTINEL );
            process( events );
        }
    }

    @Override
    public void run()
    {
        assert backgroundThread == null : "A thread is already running " + backgroundThread;
        backgroundThread = Thread.currentThread();

        try
        {
            do
            {
                AsyncEvent events = STACK.getAndSet( this, END_SENTINEL );
                process( events );
                if ( stack == END_SENTINEL && !shutdown )
                {
                    LockSupport.park( this );
                }
            }
            while ( !shutdown );

            AsyncEvent events = STACK.getAndSet( this, SHUTDOWN_SENTINEL );
            process( events );
        }
        finally
        {
            backgroundThread = null;
            shutdownLatch.release();
        }
    }

    private void process( AsyncEvent events )
    {
        events = reverseAndStripEndMark( events );
        Consumer<T> consumer = this.eventConsumer;

        while ( events != null )
        {
            @SuppressWarnings( "unchecked" )
            T event = (T) events;
            consumer.accept( event );
            events = events.next;
        }

        END_SENTINEL.next = null;
        SHUTDOWN_SENTINEL.next = null;
    }

    private AsyncEvent reverseAndStripEndMark( AsyncEvent events )
    {
        AsyncEvent result = null;
        while ( events != END_SENTINEL && events != SHUTDOWN_SENTINEL )
        {
            AsyncEvent tmp;
            do
            {
                tmp = events.next;
            }
            while ( tmp == null );
            events.next = result;
            result = events;
            events = tmp;
        }
        return result;
    }

    /**
     * Initiate the shut down process of this {@code AsyncEvents} instance.
     *
     * This call does not block or otherwise wait for the background thread to terminate.
     */
    public void shutdown()
    {
        assert backgroundThread != null : "Already shut down";
        shutdown = true;
        LockSupport.unpark( backgroundThread );
    }

    public void awaitTermination() throws InterruptedException
    {
        shutdownLatch.await();
    }

    private static class EndSentinel extends AsyncEvent
    {
    }

    private static class ShutdownSentinel extends AsyncEvent
    {
    }
}
