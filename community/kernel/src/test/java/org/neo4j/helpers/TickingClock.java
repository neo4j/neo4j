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
package org.neo4j.helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test implementation of Clock that increments time by given amount on each call to currentTimeMillis.
 */
public class TickingClock implements Clock
{
    private long currentNanoTime;
    private long tickNanos;

    private Map<Long, List<Runnable>> actions = new HashMap<>();

    public TickingClock( long current, long tick, TimeUnit timeUnit )
    {
        this.currentNanoTime = timeUnit.toNanos( current );
        this.tickNanos = timeUnit.toNanos( tick );
    }

    public TickingClock at( long time, TimeUnit timeUnit, Runnable action )
    {
        long timeNanos = timeUnit.toNanos( time );
        List<Runnable> actionList = actions.get( timeNanos );
        if ( actionList == null )
        {
            actionList = new ArrayList<>();
            actions.put( timeNanos, actionList );
        }

        actionList.add( action );

        return this;
    }

    @Override
    public long currentTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( nanoTime() );
    }

    @Override
    public long nanoTime()
    {
        List<Runnable> actionList = actions.get( currentNanoTime );
        if ( actionList != null )
        {
            for ( Runnable runnable : actionList )
            {
                runnable.run();
            }
        }

        try
        {
            return currentNanoTime;
        }
        finally
        {
            currentNanoTime += tickNanos;
        }
    }
}
