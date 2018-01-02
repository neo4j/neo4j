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
package org.neo4j.kernel.ha.cluster;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.util.JobScheduler;

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
