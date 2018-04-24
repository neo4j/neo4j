package org.neo4j.causalclustering.helper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.time.Clocks;

@SuppressWarnings( "WeakerAccess" )
public class Limiters
{
    private final ConcurrentHashMap<Object,Consumer<Runnable>> caps = new ConcurrentHashMap<>();
    private final Clock clock;

    public Limiters()
    {
        this.clock = Clocks.systemClock();
    }

    public Limiters( Clock clock )
    {
        this.clock = clock;
    }

    /**
     * Rate limits calls under the specified handle.
     *
     * @param handle A unique handle.
     * @param minInterval The minimum interval between invocations.
     * @return A rate limited consumer of {@link Runnable}s.
     */
    public Consumer<Runnable> rateLimiter( Object handle, Duration minInterval )
    {
        return caps.computeIfAbsent( handle, ignored -> Limiters.rateLimiter( minInterval, clock ) );
    }

    public static Consumer<Runnable> rateLimiter( Duration minInterval )
    {
        return rateLimiter( minInterval, Clocks.systemClock() );
    }

    /**
     * Rate limits calls.
     *
     * @param minInterval The minimum interval between invocations.
     * @return A rate limited consumer of {@link Runnable}s.
     */
    public static Consumer<Runnable> rateLimiter( Duration minInterval, Clock clock )
    {
        return new Consumer<Runnable>()
        {
            AtomicReference<Instant> lastRunRef = new AtomicReference<>();

            @Override
            public void accept( Runnable operation )
            {
                Instant now = clock.instant();
                Instant lastRun = lastRunRef.get();

                if ( lastRun == null )
                {
                    if ( lastRunRef.compareAndSet( null, now ) )
                    {
                        operation.run();
                    }
                    return;
                }

                if ( lastRun.plus( minInterval ).isAfter( now ) )
                {
                    return;
                }

                if ( lastRunRef.compareAndSet( lastRun, now ) )
                {
                    operation.run();
                }
            }
        };
    }
}
