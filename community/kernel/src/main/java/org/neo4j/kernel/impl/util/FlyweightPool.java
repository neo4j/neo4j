/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Clock;

public abstract class FlyweightPool<R>
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
            private final Clock clock;

            public TimeoutCheckStrategy( long interval, Clock clock )
            {
                this.interval = interval;
                this.lastCheckTime = clock.currentTimeMillis();
                this.clock = clock;
            }

            @Override
            public boolean shouldCheck()
            {
                long currentTime = clock.currentTimeMillis();
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
    private final CheckStrategy checkStrategy;
    // Guarded by nothing. Those are estimates, losing some values doesn't matter much
    private final AtomicInteger allocated = new AtomicInteger( 0 );
    private final AtomicInteger queueSize = new AtomicInteger( 0 );
    private int currentPeakSize;
    private int targetSize;

    protected FlyweightPool( int minSize )
    {
        this( minSize, new CheckStrategy.TimeoutCheckStrategy( DEFAULT_CHECK_INTERVAL, SYSTEM_CLOCK ),
                new Monitor.Adapter() );
    }

    protected FlyweightPool( int minSize, CheckStrategy strategy, Monitor monitor )
    {
        this.minSize = minSize;
        this.currentPeakSize = 0;
        this.targetSize = minSize;
        this.checkStrategy = strategy;
        this.monitor = monitor;
    }

    protected abstract R create();

    protected void dispose( R resource )
    {
        monitor.disposed( resource );
        allocated.decrementAndGet();
    }

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

    public void close( boolean force )
    {
    }
}
