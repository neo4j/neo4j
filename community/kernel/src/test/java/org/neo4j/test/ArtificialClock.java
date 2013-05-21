package org.neo4j.test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ArtificialClock implements Clock
{
    private volatile long currentTimeNanos;

    @Override
    public long currentTimeMillis()
    {
        return NANOSECONDS.toMillis( currentTimeNanos );
    }

    @Override
    public long nanoTime()
    {
        return currentTimeNanos;
    }

    public Progressor progressor( long time, TimeUnit unit )
    {
        return new Progressor( unit.toNanos( time ) );
    }

    public void progress( long time, TimeUnit unit )
    {
        progress( unit.toNanos( time ) );
    }

    public void setCurrentTime( long millis )
    {
        setCurrentTime( millis, 0 );
    }

    public synchronized void setCurrentTime( long millis, long nanos )
    {
        currentTimeNanos = MILLISECONDS.toNanos( millis ) + nanos;
    }

    private synchronized void progress( long nanos )
    {
        currentTimeNanos += nanos;
    }

    public class Progressor
    {
        private final long nanos;

        private Progressor( long nanos )
        {
            this.nanos = nanos;
        }

        public void tick()
        {
            progress( nanos );
        }
    }
}
