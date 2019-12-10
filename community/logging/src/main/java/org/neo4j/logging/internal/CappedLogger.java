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
package org.neo4j.logging.internal;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.Log;

/**
 * A CappedLogger will accept log messages, unless they occur "too much", in which case the messages will be ignored
 * until some time passes, or the logger is reset.
 *
 * It is also desirable to be aware that log capping is taking place, so we don't mistakenly lose log output due to
 * output capping.
 *
 * By default, the CappedLogger does not filter out any messages. Filtering can be configured at any time using the
 * "set" and "unset" methods.
 */
public class CappedLogger
{
    private final Log delegate;
    // We use the filter indirection so we can atomically update the configuration without locking
    private volatile Filter filter;

    public CappedLogger( @Nonnull Log delegate )
    {
        filter = new Filter();
        this.delegate = delegate;
    }

    public void debug( @Nonnull String msg )
    {
        if ( filter.accept() )
        {
            delegate.debug( msg );
        }
    }

    public void debug( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( filter.accept() )
        {
            delegate.debug( msg, cause );
        }
    }

    public void info( @Nonnull String msg, @Nullable Object... arguments )
    {
        if ( filter.accept() )
        {
            delegate.info( msg, arguments );
        }
    }

    public void info( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( filter.accept() )
        {
            delegate.info( msg, cause );
        }
    }

    public void warn( @Nonnull String msg )
    {
        if ( filter.accept() )
        {
            delegate.warn( msg );
        }
    }

    public void warn( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( filter.accept() )
        {
            delegate.warn( msg, cause );
        }
    }

    public void warn( @Nonnull String msg, @Nullable Object... arguments )
    {
        if ( filter.accept() )
        {
            delegate.warn( msg, arguments );
        }
    }

    public void error( @Nonnull String msg )
    {
        if ( filter.accept() )
        {
            delegate.error( msg );
        }
    }

    public void error( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( filter.accept() )
        {
            delegate.error( msg, cause );
        }
    }

    /**
     * Reset the filtering state of this CappedLogger. This usually means that something good happened, and that all
     * filtering states that grows towards a state where the log messages are filtered out, should calm down and reset.
     *
     * Specifically, this means that the counter in the count limit should return to zero, and that the time limit and
     * duplicate filter should forget about past messages.
     */
    public void reset()
    {
        filter = filter.reset();
    }

    /**
     * Set a limit to the amount of logging that this logger will accept between resets.
     * @param limit The number of log messages that the CappedLogger will let through in between resets.
     */
    public CappedLogger setCountLimit( int limit )
    {
        if ( limit < 1 )
        {
            throw new IllegalArgumentException( "The count limit must be positive" );
        }
        filter = filter.setCountLimit( limit );
        return this;
    }

    /**
     * Unset the count limit, and allow any number of messages through, provided other limits don't apply.
     */
    public CappedLogger unsetCountLimit()
    {
        filter = filter.unsetCountLimit();
        return this;
    }

    /**
     * Set a time based limit to the amount of logging that this logger will accept between resets. With a time limit
     * of 1 second, for instance, then the logger will log at most one message per second.
     * @param time The time amount, must be positive.
     * @param unit The time unit.
     * @param clock The clock to use for reading the current time when checking this limit.
     */
    public CappedLogger setTimeLimit( long time, @Nonnull TimeUnit unit, @Nonnull Clock clock )
    {
        if ( time < 1 )
        {
            throw new IllegalArgumentException( "The time limit must be positive" );
        }
        filter = filter.setTimeLimit( time, unit, clock );
        return this;
    }

    /**
     * Unset the time limit, and allow any number of messages through, as often as possible, provided other limits
     * don't apply.
     */
    public CappedLogger unsetTimeLimit()
    {
        filter = filter.unsetTimeLimit();
        return this;
    }

    private static final class Filter
    {
        private static final AtomicIntegerFieldUpdater<Filter> CURRENT_COUNT =
                AtomicIntegerFieldUpdater.newUpdater( Filter.class, "currentCount" );
        private static final AtomicLongFieldUpdater<Filter> LAST_CHECK =
                AtomicLongFieldUpdater.newUpdater( Filter.class, "lastCheck" );

        // The thread-safety of these normal fields are guarded by the volatile reads and writes to the
        // CappedLogger.filter field.
        private boolean hasCountLimit;
        private int countLimit;
        private long timeLimitMillis;
        private final Clock clock;

        // Atomically updated
        private volatile int currentCount;
        private volatile long lastCheck;

        private Filter()
        {
            this( false, 0, 0, 0, 0, null );
        }

        private Filter(
                boolean hasCountLimit, int countLimit, int currentCount, long timeLimitMillis, long lastCheck, Clock clock )
        {
            this.hasCountLimit = hasCountLimit;
            this.countLimit = countLimit;
            this.currentCount = currentCount;
            this.timeLimitMillis = timeLimitMillis;
            this.lastCheck = lastCheck;
            this.clock = clock;
        }

        public Filter setCountLimit( int limit )
        {
            return new Filter( true, limit, currentCount, timeLimitMillis, lastCheck, clock );
        }

        public boolean accept()
        {
            return (!hasCountLimit || (getAndIncrementCurrentCount() < countLimit)) && (clock == null || !checkExpiredAndSetLastCheckTime());
        }

        public int getAndIncrementCurrentCount()
        {
            return CURRENT_COUNT.getAndIncrement( this );
        }

        private boolean checkExpiredAndSetLastCheckTime()
        {
            long now = clock.millis();
            long check = this.lastCheck;
            if ( check > now - timeLimitMillis )
            {
                return true;
            }
            while ( !LAST_CHECK.compareAndSet( this, check, now ) )
            {
                check = lastCheck;
                if ( check > now )
                {
                    break;
                }
            }
            return false;
        }

        public Filter reset()
        {
            return new Filter( hasCountLimit, countLimit, 0, timeLimitMillis, 0, clock );
        }

        public Filter unsetCountLimit()
        {
            return new Filter( false, 0, currentCount, timeLimitMillis, lastCheck, clock );
        }

        public Filter setTimeLimit( long time, @Nonnull TimeUnit unit, @Nonnull Clock clock )
        {
            return new Filter( hasCountLimit, countLimit, currentCount, unit.toMillis( time ), lastCheck, clock );
        }

        public Filter unsetTimeLimit()
        {
            return new Filter( hasCountLimit, countLimit, currentCount, 0, lastCheck, null );
        }
    }
}
