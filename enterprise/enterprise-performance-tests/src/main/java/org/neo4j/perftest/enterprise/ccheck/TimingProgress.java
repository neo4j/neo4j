package org.neo4j.perftest.enterprise.ccheck;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.neo4j.helpers.ProgressIndicator;

public class TimingProgress implements ProgressIndicator.Factory
{
    public interface Visitor
    {
        void beginTimingProgress( long totalElementCount, long totalTimeNanos ) throws IOException;

        void phaseTimingProgress( String phase, long elementCount, long timeNanos ) throws IOException;

        void endTimingProgress() throws IOException;
    }

    private final Visitor visitor;
    private final ProgressIndicator.Factory actual;

    TimingProgress( Visitor visitor, ProgressIndicator.Factory actual )
    {
        this.visitor = visitor;
        this.actual = actual;
    }

    @Override
    public ProgressIndicator newSimpleProgressIndicator( long total )
    {
        return new Indicator( actual.newSimpleProgressIndicator( total ), total );
    }

    @Override
    public ProgressIndicator newMultiProgressIndicator( long total )
    {
        return new Indicator( actual.newMultiProgressIndicator( total ), total );
    }

    private class Indicator extends ProgressIndicator.Decorator
    {
        private final Timer total;
        private final long totalCount;
        private Timer current;
        private Map<String, Timer> timers = new HashMap<String, Timer>();

        public Indicator( ProgressIndicator actual, long totalCount )
        {
            super( actual );
            this.totalCount = totalCount;
            (this.total = new Timer( null )).start();
        }

        @Override
        public void phase( String phase )
        {
            super.phase( phase );
            if ( current != null )
            {
                current.stop();
            }
            Timer timer = timers.get( phase );
            if ( timer == null )
            {
                timers.put( phase, timer = new Timer( phase ) );
            }
            timer.start();
            current = timer;
        }

        @Override
        public void done( long totalProgress )
        {
            super.done( totalProgress );
            if ( current != null )
            {
                current.complete( totalProgress );
            }
            current = null;
        }

        @Override
        public void done()
        {
            super.done();
            total.complete( totalCount );
            this.accept( visitor );
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

        void complete( long processedItems )
        {
            stop();
            items += processedItems;
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
}
