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
package org.neo4j.kernel.impl.scheduler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

final class ScheduledJobHandle extends PooledJobHandle
{
    private final ScheduledTask scheduledTask;
    private final ScheduledFuture<?> job;

    ScheduledJobHandle( ScheduledFuture<?> job, ScheduledTask scheduledTask )
    {
        super( job );
        this.job = job;
        this.scheduledTask = scheduledTask;
    }

    @Override
    public void waitTermination() throws InterruptedException, ExecutionException
    {
        try
        {
            scheduledTask.waitTermination();
        }
        catch ( ExecutionException e )
        {
            job.cancel( true );
            throw e;
        }
        super.waitTermination();
    }
}
