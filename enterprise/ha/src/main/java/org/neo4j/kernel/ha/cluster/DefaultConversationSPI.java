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
package org.neo4j.kernel.ha.cluster;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.scheduler.JobScheduler;

/**
 * Default implementation of {@link ConversationSPI} used on master in HA setup.
 */
public class DefaultConversationSPI implements ConversationSPI
{
    private final Locks locks;
    private final JobScheduler jobScheduler;

    public DefaultConversationSPI( Locks locks, JobScheduler jobScheduler )
    {
        this.locks = locks;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public Locks.Client acquireClient()
    {
        return locks.newClient();
    }

    @Override
    public JobScheduler.JobHandle scheduleRecurringJob( JobScheduler.Group group, long interval, Runnable job )
    {
        return jobScheduler.scheduleRecurring( group, job, interval, TimeUnit.MILLISECONDS);
    }
}
