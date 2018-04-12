/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class CountingTimerService extends TimerService
{
    private final Map<String,Long> counts = new HashMap<>();

    public CountingTimerService( JobScheduler scheduler, LogProvider logProvider )
    {
        super( scheduler, logProvider );
    }

    @Override
    public Timer create( TimerName name, JobScheduler.Group group, TimeoutHandler handler )
    {
        TimeoutHandler countingHandler = timer -> {
            long count = counts.getOrDefault( name.name(), 0L );
            counts.put( name.name(), count + 1 );
            handler.onTimeout( timer );
        };
        return super.create( name, group, countingHandler );
    }

    public long invocationCount( TimerName name )
    {
        return counts.getOrDefault( name.name(), 0L );
    }
}
