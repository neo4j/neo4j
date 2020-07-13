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
package org.neo4j.logging.internal;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.Log;

import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.requirePositive;

/**
 * A CappedLogger will accept log messages, unless they occur "too much", in which case the messages will be ignored
 * until some time passes, or the logger is reset.
 *
 * It is also desirable to be aware that log capping is taking place, so we don't mistakenly lose log output due to
 * output capping.
 */
public class CappedLogger
{
    private static final AtomicLongFieldUpdater<CappedLogger> LAST_CHECK =
            AtomicLongFieldUpdater.newUpdater( CappedLogger.class, "lastCheck" );

    private final Log delegate;
    private final long timeLimitMillis;
    private final Clock clock;

    // Atomically updated
    private volatile long lastCheck;

    /**
     * Logger with a time based limit to the amount of logging it will accept between resets. With a time limit
     * of 1 second, for instance, then the logger will log at most one message per second.
     *
     * @param time The time amount, must be positive.
     * @param unit The time unit.
     * @param clock The clock to use for reading the current time when checking this limit.
     */
    public CappedLogger( Log delegate, long time, TimeUnit unit, Clock clock )
    {
        this.delegate = requireNonNull( delegate );
        this.clock = requireNonNull( clock );
        this.timeLimitMillis = unit.toMillis( requirePositive( time ) );
    }

    public void debug( @Nonnull String msg )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.debug( msg );
        }
    }

    public void debug( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.debug( msg, cause );
        }
    }

    public void info( @Nonnull String msg, @Nullable Object... arguments )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.info( msg, arguments );
        }
    }

    public void info( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.info( msg, cause );
        }
    }

    public void warn( @Nonnull String msg )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.warn( msg );
        }
    }

    public void warn( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.warn( msg, cause );
        }
    }

    public void warn( @Nonnull String msg, @Nullable Object... arguments )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.warn( msg, arguments );
        }
    }

    public void error( @Nonnull String msg )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.error( msg );
        }
    }

    public void error( @Nonnull String msg, @Nonnull Throwable cause )
    {
        if ( checkExpiredAndSetLastCheckTime() )
        {
            delegate.error( msg, cause );
        }
    }

    private boolean checkExpiredAndSetLastCheckTime()
    {
        long now = clock.millis();
        long check = this.lastCheck;
        if ( check > now - timeLimitMillis )
        {
            return false;
        }
        while ( !LAST_CHECK.compareAndSet( this, check, now ) )
        {
            check = lastCheck;
            if ( check > now )
            {
                break;
            }
        }
        return true;
    }
}
