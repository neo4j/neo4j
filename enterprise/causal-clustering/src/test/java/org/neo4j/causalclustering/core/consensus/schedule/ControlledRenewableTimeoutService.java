/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.schedule;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ControlledRenewableTimeoutService implements RenewableTimeoutService
{
    public static class Timer implements RenewableTimeout
    {
        TimeoutHandler callback;
        long delayMillis;
        boolean enabled;
        long renewalCount;

        Timer( TimeoutHandler callback, long delayMillis )
        {
            this.callback = callback;
            this.delayMillis = delayMillis;
            enabled = true;
        }

        @Override
        public void renew()
        {
            enabled = true;
            renewalCount++;
        }

        public long renewalCount()
        {
            return renewalCount;
        }

        @Override
        public void cancel()
        {
            enabled = false;
        }
    }

    private Map<TimeoutName,Timer> timers = new HashMap<>();
    private final FakeClock clock;

    public ControlledRenewableTimeoutService()
    {
        this( Clocks.fakeClock() );
    }

    public ControlledRenewableTimeoutService( FakeClock clock )
    {
        this.clock = clock;
    }

    @Override
    public RenewableTimeout create( TimeoutName name, long delayInMillis, long randomRangeInMillis, TimeoutHandler callback )
    {
        Timer timer = new Timer( callback, delayInMillis );
        timers.put( name, timer );
        return timer;
    }

    public void invokeTimeout( TimeoutName name )
    {
        Timer timer = timers.get( name );
        if ( timer == null )
        {
            /* not registered */
            return;
        }
        /* invoking a certain timer moves time forward the same amount */
        clock.forward( timer.delayMillis, MILLISECONDS );
        if ( !timer.enabled )
        {
            throw new IllegalStateException( "Invoked timer which is not enabled" );
        }
        timer.cancel();
        timer.callback.onTimeout( timer );
    }

    public Timer getTimeout( TimeoutName name )
    {
        return timers.get( name );
    }
}
