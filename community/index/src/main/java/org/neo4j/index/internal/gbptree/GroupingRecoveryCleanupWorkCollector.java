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

import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

/**
 * Runs cleanup work as they're added in {@link #add(CleanupJob)}, but the thread that calls {@link #add(CleanupJob)} will not execute them itself.
 */
public class GroupingRecoveryCleanupWorkCollector extends RecoveryCleanupWorkCollector
{
    private final BlockingQueue<CleanupJob> jobs = new LinkedBlockingQueue<>();
    private final JobScheduler jobScheduler;
    private volatile boolean started;
    private JobHandle handle;

    /**
     * @param jobScheduler {@link JobScheduler} to queue {@link CleanupJob} into.
     */
    public GroupingRecoveryCleanupWorkCollector( JobScheduler jobScheduler )
    {
        this.jobScheduler = jobScheduler;
    }

    @Override
    public void init()
    {
        started = false;
        if ( !jobs.isEmpty() )
        {
            StringJoiner joiner = new StringJoiner( String.format( "%n  " ), "Did not expect there to be any cleanup jobs still here. Jobs[", "]" );
            consumeAndCloseJobs( cj -> joiner.add( jobs.toString() ) );
            throw new IllegalStateException( joiner.toString() );
        }
        scheduleJobs();
    }

    @Override
    public void add( CleanupJob job )
    {
        if ( started )
        {
            throw new IllegalStateException( "Index clean jobs can't be added after collector start." );
        }
        jobs.add( job );
    }

    @Override
    public void start()
    {
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
        consumeAndCloseJobs( cj -> {} );
    }

    private void scheduleJobs()
    {
        handle = jobScheduler.schedule( Group.STORAGE_MAINTENANCE, allJobs() );
    }

    private Runnable allJobs()
    {
        return () ->
                executeWithExecutor( executor ->
                {
                    CleanupJob job = null;
                    do
                    {
                        try
                        {
                            job = jobs.poll( 100, TimeUnit.MILLISECONDS );
                            if ( job != null )
                            {
                                job.run( executor );
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
                } );
    }

    private void consumeAndCloseJobs( Consumer<CleanupJob> consumer )
    {
        CleanupJob job;
        while ( (job = jobs.poll()) != null )
        {
            consumer.accept( job );
            job.close();
        }
    }
}
