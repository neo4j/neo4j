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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.neo4j.helpers.Clock;
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

    public CappedLogger( Log delegate )
    {
        if ( delegate == null )
        {
            throw new IllegalArgumentException( "The delegate StringLogger cannot be null" );
        }
        filter = new Filter();
        this.delegate = delegate;
    }

    public void debug( String msg )
    {
        if ( filter.accept( msg, null ) )
        {
            delegate.debug( msg );
        }
    }

    public void debug( String msg, Throwable cause )
    {
        if ( filter.accept( msg, cause ) )
        {
            delegate.debug( msg, cause );
        }
    }
    
    public void info( String msg )
    {
        if ( filter.accept( msg, null ) )
        {
            delegate.info( msg );
        }
    }

    public void info( String msg, Throwable cause )
    {
        if ( filter.accept( msg, cause ) )
        {
            delegate.info( msg, cause );
        }
    }
    
    public void warn( String msg )
    {
        if ( filter.accept( msg, null ) )
        {
            delegate.warn( msg );
        }
    }

    public void warn( String msg, Throwable cause )
    {
        if ( filter.accept( msg, cause ) )
        {
            delegate.warn( msg, cause );
        }
    }
    
    public void error( String msg )
    {
        if ( filter.accept( msg, null ) )
        {
            delegate.error( msg );
        }
    }

    public void error( String msg, Throwable cause )
    {
        if ( filter.accept( msg, cause ) )
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
    public CappedLogger setTimeLimit( long time, TimeUnit unit, Clock clock )
    {
        if ( time < 1 )
        {
            throw new IllegalArgumentException( "The time limit must be positive" );
        }
        if ( unit == null )
        {
            throw new IllegalArgumentException( "The time unit cannot be null" );
        }
        if ( clock == null )
        {
            throw new IllegalArgumentException( "The clock used for time limiting cannot be null" );
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

    /**
     * Enable or disable filtering of duplicate messages. This filtering only looks at the previous message, so a
     * sequence of identical messages will only have that message logged once, but a sequence of two alternating
     * messages will get logged in full.
     * @param enabled {@code true} if duplicates should be filtered, {@code false} if they should not.
     */
    public CappedLogger setDuplicateFilterEnabled( boolean enabled )
    {
        filter = filter.setDuplicateFilterEnabled( enabled );
        return this;
    }

    private static class Filter
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
        private Clock clock;
        private boolean filterDuplicates;

        // Atomically updated
        private volatile int currentCount;
        private volatile long lastCheck;

        // Read and updated together; guarded by synchronized(this) in checkDuplicate()
        private String lastMessage;
        private Throwable lastException;

        private Filter()
        {
            this( false, 0, 0, 0, 0, null, false );
        }

        private Filter(
                boolean hasCountLimit,
                int countLimit,
                int currentCount,
                long timeLimitMillis,
                long lastCheck,
                Clock clock, boolean filterDuplicates )
        {
            this.hasCountLimit = hasCountLimit;
            this.countLimit = countLimit;
            this.currentCount = currentCount;
            this.timeLimitMillis = timeLimitMillis;
            this.lastCheck = lastCheck;
            this.clock = clock;
            this.filterDuplicates = filterDuplicates;
        }

        public Filter setCountLimit( int limit )
        {
            return new Filter( true, limit, currentCount, timeLimitMillis, lastCheck, clock, filterDuplicates );
        }

        public boolean accept( String msg, Throwable cause )
        {
            return (!hasCountLimit || (getAndIncrementCurrentCount() < countLimit))
                    && (clock == null || !checkExpiredAndSetLastCheckTime())
                    && (!filterDuplicates || checkDuplicate( msg, cause ));
        }

        public int getAndIncrementCurrentCount()
        {
            return CURRENT_COUNT.getAndIncrement( this );
        }

        private boolean checkExpiredAndSetLastCheckTime()
        {
            long now = clock.currentTimeMillis();
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
        
        private synchronized boolean checkDuplicate( String msg, Throwable cause )
        {
            String last = lastMessage;
            Throwable exc = lastException;
            if ( stringEqual( last, msg ) && sameClass( cause, exc ) && sameMsg( cause, exc ) )
            {
                // Duplicate! Filter it out.
                return false;
            }
            else
            {
                // Distinct! Update and let it through.
                lastMessage = msg;
                lastException = cause;
                return true;
            }
        }

        private boolean sameMsg( Throwable cause, Throwable exc )
        {
            return ( cause == null && exc == null ) ||
                    ( cause != null && exc != null && stringEqual( exc.getMessage(), cause.getMessage() ) );
        }

        private boolean stringEqual( String a, String b )
        {
            return a == null ? b == null : a.equals( b );
        }

        private boolean sameClass( Throwable cause, Throwable exc )
        {
            return ( cause == null && exc == null ) ||
                    ( cause != null && exc != null && exc.getClass().equals( cause.getClass() ) );
        }

        public Filter reset()
        {
            return new Filter( hasCountLimit, countLimit, 0, timeLimitMillis, 0, clock, filterDuplicates );
        }

        public Filter unsetCountLimit()
        {
            return new Filter( false, 0, currentCount, timeLimitMillis, lastCheck, clock, filterDuplicates );
        }

        public Filter setTimeLimit( long time, TimeUnit unit, Clock clock )
        {
            return new Filter(
                    hasCountLimit, countLimit, currentCount, unit.toMillis( time ), lastCheck, clock, filterDuplicates );
        }

        public Filter unsetTimeLimit()
        {
            return new Filter( hasCountLimit, countLimit, currentCount, 0, lastCheck, null, filterDuplicates );
        }

        public Filter setDuplicateFilterEnabled( boolean enabled )
        {
            return new Filter( hasCountLimit, countLimit, currentCount, timeLimitMillis, lastCheck, clock, enabled );
        }
    }
}
