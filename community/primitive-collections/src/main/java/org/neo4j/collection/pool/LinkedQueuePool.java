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
package org.neo4j.collection.pool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Factory;
import org.neo4j.function.primitive.LongSupplier;

public class LinkedQueuePool<R> implements Pool<R>
{
    public interface Monitor<R>
    {
        public void updatedCurrentPeakSize( int currentPeakSize );

        public void updatedTargetSize( int targetSize );

        public void created( R resource );

        public void acquired( R resource );

        public void disposed( R resource );

        public class Adapter<R> implements Monitor<R>
        {
            @Override
            public void updatedCurrentPeakSize( int currentPeakSize )
            {
            }

            @Override
            public void updatedTargetSize( int targetSize )
            {
            }

            @Override
            public void created( R resource )
            {
            }

            @Override
            public void acquired( R resource )
            {
            }

            @Override
            public void disposed( R resource )
            {
            }
        }
    }

    public interface CheckStrategy
    {
        public boolean shouldCheck();

        public class TimeoutCheckStrategy implements CheckStrategy
        {
            private final long interval;
            private long lastCheckTime;
            private final LongSupplier clock;

            public TimeoutCheckStrategy( long interval )
            {
                this(interval, new LongSupplier()
                {
                    @Override
                    public long getAsLong()
                    {
                        return System.currentTimeMillis();
                    }
                });
            }

            public TimeoutCheckStrategy( long interval, LongSupplier clock )
            {
                this.interval = interval;
                this.lastCheckTime = clock.getAsLong();
                this.clock = clock;
            }

            @Override
            public boolean shouldCheck()
            {
                long currentTime = clock.getAsLong();
                if ( currentTime > lastCheckTime + interval )
                {
                    lastCheckTime = currentTime;
                    return true;
                }
                return false;
            }
        }
    }

    public static final int DEFAULT_CHECK_INTERVAL = 60 * 1000;

    private final Queue<R> unused = new ConcurrentLinkedQueue<>();
    private final Monitor monitor;
    private final int minSize;
    private final Factory<R> factory;
    private final CheckStrategy checkStrategy;
    // Guarded by nothing. Those are estimates, losing some values doesn't matter much
    private final AtomicInteger allocated = new AtomicInteger( 0 );
    private final AtomicInteger queueSize = new AtomicInteger( 0 );
    private int currentPeakSize;
    private int targetSize;

    public LinkedQueuePool( int minSize, Factory<R> factory)
    {
        this( minSize, factory, new CheckStrategy.TimeoutCheckStrategy( DEFAULT_CHECK_INTERVAL ),
            new Monitor.Adapter() );
    }

    public LinkedQueuePool( int minSize, Factory<R> factory, CheckStrategy strategy, Monitor monitor )
    {
        this.minSize = minSize;
        this.factory = factory;
        this.currentPeakSize = 0;
        this.targetSize = minSize;
        this.checkStrategy = strategy;
        this.monitor = monitor;
    }

    protected R create()
    {
        return factory.newInstance();
    }

    protected void dispose( R resource )
    {
        monitor.disposed( resource );
        allocated.decrementAndGet();
    }

    @Override
    public final R acquire()
    {
        R resource = unused.poll();
        if ( resource == null )
        {
            resource = create();
            allocated.incrementAndGet();
            monitor.created( resource );
        }
        else
        {
            queueSize.decrementAndGet();
        }
        currentPeakSize = Math.max( currentPeakSize, allocated.get() - queueSize.get() );
        if ( checkStrategy.shouldCheck() )
        {
            targetSize = Math.max( minSize, currentPeakSize );
            monitor.updatedCurrentPeakSize( currentPeakSize );
            currentPeakSize = 0;
            monitor.updatedTargetSize( targetSize );
        }

        monitor.acquired( resource );
        return resource;
    }

    @Override
    public void release( R toRelease )
    {
        if ( queueSize.get() < targetSize )
        {
            unused.offer( toRelease );
            queueSize.incrementAndGet();
        }
        else
        {
            dispose( toRelease );
        }
    }

    /**
     * Dispose of all pooled objects.
     */
    public void disposeAll()
    {
        for(R resource = unused.poll(); resource != null; resource = unused.poll())
        {
            dispose( resource );
        }
    }

    public void close( boolean force )
    {
        disposeAll();
    }
}
