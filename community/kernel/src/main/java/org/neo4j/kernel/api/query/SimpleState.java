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

import static java.util.Collections.emptyMap;

final class SimpleState extends ExecutingQueryStatus
{
    private static final ExecutingQueryStatus PLANNING = new SimpleState( PLANNING_STATE );
    private static final ExecutingQueryStatus RUNNING = new SimpleState( RUNNING_STATE );
    private final String name;

    static ExecutingQueryStatus planning()
    {
        return PLANNING;
    }

    static ExecutingQueryStatus running()
    {
        return RUNNING;
    }

    private SimpleState( String name )
    {
        this.name = name;
    }

    @Override
    long waitTimeNanos( long currentTimeNanos )
    {
        return 0;
    }

    @Override
    Map<String,Object> toMap( long currentTimeNanos )
    {
        return emptyMap();
    }

    @Override
    String name()
    {
        return name;
    }

    @Override
    boolean isPlanning()
    {
        return this == PLANNING;
    }
}
