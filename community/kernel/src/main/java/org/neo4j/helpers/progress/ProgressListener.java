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
package org.neo4j.helpers.progress;

/**
 * A Progress object is an object through which a process can report its progress.
 * <p>
 * Progress objects are not thread safe, and are to be used by a single thread only. Each Progress object from a {@link
 * ProgressMonitorFactory.MultiPartBuilder} can be used from different threads.
 */
public interface ProgressListener
{
    void started( String task );

    void started();

    void set( long progress );

    void add( long progress );

    void done();

    void failed( Throwable e );

    class Adapter implements ProgressListener
    {
        @Override
        public void started()
        {
            started( null );
        }

        @Override
        public void started( String task )
        {
        }

        @Override
        public void set( long progress )
        {
        }

        @Override
        public void add( long progress )
        {
        }

        @Override
        public void done()
        {
        }

        @Override
        public void failed( Throwable e )
        {
        }
    }

    ProgressListener NONE = new Adapter();

    class SinglePartProgressListener extends Adapter
    {
        final Indicator indicator;
        private final long totalCount;
        private long value;
        private int lastReported;
        private boolean stared;

        SinglePartProgressListener( Indicator indicator, long totalCount )
        {
            this.indicator = indicator;
            this.totalCount = totalCount;
        }

        @Override
        public void started( String task )
        {
            if ( !stared )
            {
                stared = true;
                indicator.startProcess( totalCount );
            }
        }

        @Override
        public void set( long progress )
        {
            update( value = progress );
        }

        @Override
        public void add( long progress )
        {
            update( value += progress );
        }

        @Override
        public void done()
        {
            set( totalCount );
            indicator.completeProcess();
        }

        @Override
        public void failed( Throwable e )
        {
            indicator.failure(e);
        }

        void update( long progress )
        {
            started();
            int current = totalCount == 0 ? 0 : (int) ((progress * indicator.reportResolution()) / totalCount);
            if ( current > lastReported )
            {
                indicator.progress( lastReported, current );
                lastReported = current;
            }
        }
    }

    final class MultiPartProgressListener extends Adapter
    {
        private final Aggregator aggregator;
        final String part;
        boolean started;
        private long value;
        private long lastReported;
        final long totalCount;

        MultiPartProgressListener( Aggregator aggregator, String part, long totalCount )
        {
            this.aggregator = aggregator;
            this.part = part;
            this.totalCount = totalCount;
        }

        @Override
        public void started( String task )
        {
            if ( !started )
            {
                aggregator.start( this );
                started = true;
            }
        }

        @Override
        public void set( long progress )
        {
            update( value = progress );
        }

        @Override
        public void add( long progress )
        {
            update( value += progress );
        }

        @Override
        public void done()
        {
            set( totalCount );
            aggregator.complete( this );
        }

        @Override
        public void failed( Throwable e )
        {
            aggregator.signalFailure( e );
        }

        private void update( long progress )
        {
            started();
            if ( progress > lastReported )
            {
                aggregator.update( progress - lastReported );
                lastReported = progress;
            }
        }

        enum State
        {
            INIT, LIVE
        }
    }
}
