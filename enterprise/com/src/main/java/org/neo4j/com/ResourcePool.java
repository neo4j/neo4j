/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.time.Clocks;

public abstract class ResourcePool<R>
{
    public interface Monitor<R>
    {
        void updatedCurrentPeakSize( int currentPeakSize );

        void updatedTargetSize( int targetSize );

        void created( R resource );

        void acquired( R resource );

        void disposed( R resource );

        class Adapter<R> implements Monitor<R>
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
        boolean shouldCheck();

        class TimeoutCheckStrategy implements CheckStrategy
        {
            private final long interval;
            private volatile long lastCheckTime;
            private final Clock clock;

            public TimeoutCheckStrategy( long interval, Clock clock )
            {
                this.interval = interval;
                this.lastCheckTime = clock.millis();
                this.clock = clock;
            }

            @Override
            public boolean shouldCheck()
            {
                long currentTime = clock.millis();
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

    // protected for testing
    protected final LinkedList<R> unused = new LinkedList<>();
    private final Map<Thread,R> current = new ConcurrentHashMap<>();
    private final Monitor<R> monitor;
    private final int minSize;
    private final CheckStrategy checkStrategy;
    // Guarded by nothing. Those are estimates, losing some values doesn't matter much
    private int currentPeakSize;
    private int targetSize;

    protected ResourcePool( int minSize )
    {
        this( minSize, new CheckStrategy.TimeoutCheckStrategy( DEFAULT_CHECK_INTERVAL, Clocks.systemClock() ),
                new Monitor.Adapter<>() );
    }

    protected ResourcePool( int minSize, CheckStrategy strategy, Monitor<R> monitor )
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
    }

    protected int currentSize()
    {
        return current.size();
    }

    protected boolean isAlive( R resource )
    {
        return true;
    }

    public final R acquire()
    {
        Thread thread = Thread.currentThread();
        R resource = current.get( thread );
        if ( resource == null )
        {
            List<R> garbage = null;
            synchronized ( unused )
            {
                for (; ; )
                {
                    resource = unused.poll();
                    if ( resource == null )
                    {
                        break;
                    }
                    if ( isAlive( resource ) )
                    {
                        break;
                    }
                    if ( garbage == null )
                    {
                        garbage = new LinkedList<>();
                    }
                    garbage.add( resource );
                }
            }
            if ( resource == null )
            {
                resource = create();
                monitor.created( resource );
            }
            current.put( thread, resource );
            monitor.acquired( resource );
            if ( garbage != null )
            {
                for ( R dead : garbage )
                {
                    dispose( dead );
                    monitor.disposed( dead );
                }
            }
        }
        currentPeakSize = Math.max( currentPeakSize, current.size() );
        if ( checkStrategy.shouldCheck() )
        {
            targetSize = Math.max( minSize, currentPeakSize );
            monitor.updatedCurrentPeakSize( currentPeakSize );
            currentPeakSize = 0;
            monitor.updatedTargetSize( targetSize );
        }

        return resource;
    }

    public final void release()
    {
        Thread thread = Thread.currentThread();
        R resource = current.remove( thread );
        if ( resource != null )
        {
            boolean dead = false;
            synchronized ( unused )
            {
                if ( unused.size() < targetSize )
                {
                    unused.add( resource );
                }
                else
                {
                    dead = true;
                }
            }
            if ( dead )
            {
                dispose( resource );
                monitor.disposed( resource );
            }
        }
    }

    public final void close( boolean force )
    {
        List<R> dead = new LinkedList<>();
        synchronized ( unused )
        {
            dead.addAll( unused );
            unused.clear();
        }
        if ( force )
        {
            dead.addAll( current.values() );
        }
        for ( R resource : dead )
        {
            dispose( resource );
        }
    }
}
