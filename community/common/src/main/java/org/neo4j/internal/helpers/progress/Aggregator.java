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
package org.neo4j.internal.helpers.progress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

final class Aggregator
{
    private final Map<ProgressListener,State> states = new HashMap<>();
    private final Indicator indicator;
    @SuppressWarnings( "unused"/*accessed through updater*/ )
    private volatile long progress;
    @SuppressWarnings( "unused"/*accessed through updater*/ )
    private volatile int last;
    private static final AtomicLongFieldUpdater<Aggregator> PROGRESS_UPDATER = newUpdater( Aggregator.class, "progress" );
    private static final AtomicIntegerFieldUpdater<Aggregator> LAST_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater( Aggregator.class, "last" );
    private volatile long totalCount;

    Aggregator( Indicator indicator )
    {
        this.indicator = indicator;
    }

    synchronized void add( ProgressListener progress, long totalCount )
    {
        states.put( progress, State.INIT );
        this.totalCount += totalCount;
    }

    synchronized ProgressMonitorFactory.Completer initialize()
    {
        indicator.startProcess( totalCount );
        if ( states.isEmpty() )
        {
            indicator.progress( 0, indicator.reportResolution() );
            indicator.completeProcess();
        }

        List<ProgressListener> progressesToClose = new ArrayList<>( states.keySet() );
        return () -> progressesToClose.forEach( ProgressListener::done );
    }

    void update( long delta )
    {
        long progress = PROGRESS_UPDATER.addAndGet( this, delta );
        int current = (int) ((progress * indicator.reportResolution()) / totalCount);
        updateTo( current );
    }

    private void updateTo( int to )
    {
        for ( int last = this.last; to > last; last = this.last )
        {
            if ( LAST_UPDATER.compareAndSet( this, last, to ) )
            {
                synchronized ( this )
                {
                    indicator.progress( last, to );
                }
            }
        }
    }

    void updateRemaining()
    {
        updateTo( indicator.reportResolution() );
    }

    synchronized void start( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.put( part, State.LIVE ) == State.INIT )
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
                updateRemaining();
            }
        }
    }

    synchronized void signalFailure( Throwable e )
    {
        indicator.failure( e );
    }

    void done()
    {
        states.keySet().forEach( ProgressListener::done );
    }

    enum State
    {
        INIT, LIVE
    }
}
