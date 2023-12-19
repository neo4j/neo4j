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

import java.util.Collection;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.FakeClock;

public class OnDemandTimerService extends TimerService
{
    private final FakeClock clock;
    private OnDemandJobScheduler onDemandJobScheduler;

    public OnDemandTimerService( FakeClock clock )
    {
        super( new OnDemandJobScheduler(), NullLogProvider.getInstance() );
        this.clock = clock;
        onDemandJobScheduler = (OnDemandJobScheduler) scheduler;
    }

    @Override
    public void invoke( TimerName name )
    {
        Collection<Timer> timers = getTimers( name );

        for ( Timer timer : timers )
        {
            Delay delay = timer.delay();
            clock.forward( delay.amount(), delay.unit() );
        }

        for ( Timer timer : timers )
        {
            timer.invoke();
        }

        onDemandJobScheduler.runJob();
    }
}
