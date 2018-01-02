/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.ccheck;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.progress.Indicator;
import org.neo4j.helpers.progress.ProgressMonitorFactory;

public class TimingProgress extends ProgressMonitorFactory
{
    public interface Visitor
    {
        void beginTimingProgress( long totalElementCount, long totalTimeNanos ) throws IOException;

        void phaseTimingProgress( String phase, long elementCount, long timeNanos ) throws IOException;

        void endTimingProgress() throws IOException;
    }

    private final Visitor visitor;
    private final ProgressMonitorFactory actual;

    TimingProgress( Visitor visitor, ProgressMonitorFactory actual )
    {
        this.visitor = visitor;
        this.actual = actual;
    }

    @Override
    protected Indicator newIndicator( String process )
    {
        return new ProgressIndicator( process );
    }

    @Override
    protected Indicator.OpenEnded newOpenEndedIndicator( String process, int resolution )
    {
        return new ProgressIndicator( process, resolution );
    }

    private static class Timer
    {
        private final String name;
        private long time = 0;
        private Long start = null;
        private long items = 0;

        Timer( String name )
        {
            this.name = name;
        }

        void start()
        {
            if ( start == null )
            {
                start = System.nanoTime();
            }
        }

        void stop()
        {
            if ( start != null )
            {
                time += System.nanoTime() - start;
            }
            start = null;
        }

        void accept( Visitor visitor ) throws IOException
        {
            visitor.phaseTimingProgress( name, items, time );
        }
    }

    private class ProgressIndicator extends Indicator.Decorator
    {
        private final Timer total;
        private Map<String, Timer> timers;

        ProgressIndicator( String process )
        {
            super( TimingProgress.this.actual, process );
            total = new Timer( null );
            timers = new HashMap<String, Timer>();
        }

        ProgressIndicator( String process, int resolution )
        {
            super( TimingProgress.this.actual, process, resolution );
            total = new Timer( null );
            timers = new HashMap<String, Timer>();
        }

        @Override
        public void startProcess( long totalCount )
        {
            super.startProcess( totalCount );
            total.items = totalCount;
            total.start();
        }

        @Override
        public void startPart( String part, long totalCount )
        {
            super.startPart( part, totalCount );
            Timer timer = new Timer( part );
            timers.put( part, timer );
            timer.items = totalCount;
            timer.start();
        }

        @Override
        public void completePart( String part )
        {
            timers.get( part ).stop();
            super.completePart( part );
        }

        @Override
        public void completeProcess()
        {
            total.stop();
            super.completeProcess();
            this.accept(visitor);
        }

        private void accept( Visitor visitor )
        {
            try
            {
                visitor.beginTimingProgress( total.items, total.time );
                for ( Timer timer : timers.values() )
                {
                    timer.accept( visitor );
                }
                visitor.endTimingProgress();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
    }
}
