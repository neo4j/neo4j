/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.schedule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

/**
 * A timer service allowing the creation of timers which can be set to expire
 * at a future point in time.
 */
public class TimerService
{
    protected final JobScheduler scheduler;
    private final Collection<Timer> timers = new ArrayList<>();
    private final Log log;

    public TimerService( JobScheduler scheduler, LogProvider logProvider )
    {
        this.scheduler = scheduler;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Creates a timer in the deactivated state.
     *
     * @param name The name of the timer.
     * @param group The scheduler group from which timeouts fire.
     * @param handler The handler invoked on a timeout.
     * @return The deactivated timer.
     */
    public synchronized Timer create( TimerName name, JobScheduler.Group group, TimeoutHandler handler )
    {
        Timer timer = new Timer( name, scheduler, log, group, handler );
        timers.add( timer );
        return timer;
    }

    /**
     * Gets all timers registered under the specified name.
     *
     * @param name The name of the timer(s).
     * @return The timers matching the name.
     */
    public synchronized Collection<Timer> getTimers( TimerName name )
    {
        return timers.stream().filter( timer -> timer.name().equals( name ) ).collect( Collectors.toList() );
    }

    /**
     * Invokes all timers matching the name.
     *
     * @param name The name of the timer(s).
     */
    public synchronized void invoke( TimerName name )
    {
        getTimers( name ).forEach( Timer::invoke );
    }

    /**
     * Convenience interface for timer enums.
     */
    public interface TimerName
    {
        String name();
    }
}
