/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.index.internal.gbptree;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.Preconditions;

/**
 * Runs cleanup work as they're added in {@link #add(CleanupJob)}, but the thread that calls {@link #add(CleanupJob)} will not execute them itself.
 */
public class GroupingRecoveryCleanupWorkCollector extends RecoveryCleanupWorkCollector
{
    private final BlockingQueue<CleanupJob> jobs = new LinkedBlockingQueue<>();
    private final JobScheduler jobScheduler;
    private final Group group;
    private volatile boolean started;
    private JobHandle handle;

    /**
     * @param jobScheduler {@link JobScheduler} to queue {@link CleanupJob} into.
     * @param group {@link Group} to which all cleanup jobs should be scheduled.
     */
    public GroupingRecoveryCleanupWorkCollector( JobScheduler jobScheduler, Group group )
    {
        this.jobScheduler = jobScheduler;
        this.group = group;
    }

    @Override
    public void init()
    {
        scheduleJobs();
    }

    @Override
    public void add( CleanupJob job )
    {
        Preconditions.checkState( !started, "Index clean jobs can't be added after collector start." );
        jobs.add( job );
    }

    @Override
    public void start()
    {
        Preconditions.checkState( !started, "Already started" );
        started = true;
    }

    @Override
    public void shutdown() throws ExecutionException, InterruptedException
    {
        started = true;
        if ( handle != null )
        {
            // Also set the started flag which acts as a signal to exit the scheduled job on empty queue,
            // this is of course a special case where perhaps not start() gets called, i.e. if something fails
            // before reaching that phase in the lifecycle.
            handle.waitTermination();
        }
        CleanupJob job;
        while ( (job = jobs.poll()) != null )
        {
            job.close();
        }
    }

    private void scheduleJobs()
    {
        handle = jobScheduler.schedule( group, allJobs() );
    }

    private Runnable allJobs()
    {
        return () ->
        {
            CleanupJob job = null;
            do
            {
                try
                {
                    job = jobs.poll( 100, TimeUnit.MILLISECONDS );
                    if ( job != null )
                    {
                        job.run( jobScheduler.executor( group ) );
                    }
                }
                catch ( Exception e )
                {
                    // There's no audience for these exceptions. The jobs themselves know if they've failed and communicates
                    // that to its tree. The scheduled job is just a vessel for running these cleanup jobs.
                }
                finally
                {
                    if ( job != null )
                    {
                        job.close();
                    }
                }
            }
            // Even if there are no jobs in the queue then continue looping until we go to started state
            while ( !jobs.isEmpty() || !started );
        };
    }
}
