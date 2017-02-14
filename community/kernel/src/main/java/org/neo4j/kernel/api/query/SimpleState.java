package org.neo4j.kernel.api.query;

/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.time.SystemNanoClock;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

final class SimpleState extends ExecutingQueryStatus
{
    private static final ExecutingQueryStatus PLANNING = new SimpleState( singletonMap( "state", "PLANNING" ) );
    private static final ExecutingQueryStatus RUNNING = new SimpleState( singletonMap( "state", "RUNNING" ) );
    private final Map<String,Object> state;

    public static ExecutingQueryStatus planning()
    {
        return PLANNING;
    }

    public static ExecutingQueryStatus running()
    {
        return RUNNING;
    }

    private SimpleState( Map<String,Object> state )
    {
        this.state = unmodifiableMap( state );
    }

    @Override
    public long waitTimeNanos( long currentTimeNanos )
    {
        return 0;
    }

    @Override
    public Map<String,Object> toMap( long currentTimeNanos )
    {
        return state;
    }

    @Override
    public boolean isPlanning()
    {
        return this == PLANNING;
    }
}
