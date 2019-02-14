/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.helpers.progress;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

final class Aggregator
{
    private final Map<ProgressListener, ProgressListener.MultiPartProgressListener.State> states = new HashMap<>();
    private final Indicator indicator;
    @SuppressWarnings( "unused"/*accessed through updater*/ )
    private volatile long progress;
    @SuppressWarnings( "unused"/*accessed through updater*/ )
    private volatile int last;
    private static final AtomicLongFieldUpdater<Aggregator> PROGRESS_UPDATER = newUpdater( Aggregator.class, "progress" );
    private static final AtomicIntegerFieldUpdater<Aggregator> LAST_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater( Aggregator.class, "last" );
    private long totalCount;

    Aggregator( Indicator indicator )
    {
        this.indicator = indicator;
    }

    synchronized void add( ProgressListener progress, long totalCount )
    {
        states.put( progress, ProgressListener.MultiPartProgressListener.State.INIT );
        this.totalCount += totalCount;
    }

    synchronized void initialize()
    {
        indicator.startProcess( totalCount );
        if ( states.isEmpty() )
        {
            indicator.progress( 0, indicator.reportResolution() );
            indicator.completeProcess();
        }
    }

    void update( long delta )
    {
        long progress = PROGRESS_UPDATER.addAndGet( this, delta );
        int current = (int) ((progress * indicator.reportResolution()) / totalCount);
        for ( int last = this.last; current > last; last = this.last )
        {
            if ( LAST_UPDATER.compareAndSet( this, last, current ) )
            {
                synchronized ( this )
                {
                    indicator.progress( last, current );
                }
            }
        }
    }

    synchronized void start( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.put( part, ProgressListener.MultiPartProgressListener.State.LIVE ) == ProgressListener.MultiPartProgressListener.State.INIT )
        {
            indicator.startPart( part.part, part.totalCount );
        }
    }

    synchronized void complete( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.remove( part ) != null )
        {
            indicator.completePart( part.part );
            if ( states.isEmpty() )
            {
                indicator.completeProcess();
            }
        }
    }

    synchronized void signalFailure( Throwable e )
    {
        indicator.failure( e );
    }
}
