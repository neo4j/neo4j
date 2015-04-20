/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store.io;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

import org.neo4j.unsafe.impl.batchimport.executor.Task;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Writer;

/**
 * A queue of {@link WriteJob write jobs} for one and the same {@link Writer}.
 * As jobs are {@link #offer(WriteJob) offered} a task is submitted to an {@link ExecutorService}
 * managed by this queue. If there have been multiple jobs offered before the task is executed
 * then all of them will be {@link #drain() collected} and executed in the same task.
 */
class WriteQueue implements Task<Void>
{
    private final LinkedList<WriteJob> queue = new LinkedList<>();
    private final TaskExecutor<Void> executor;
    private final JobMonitor jobMonitor;

    public WriteQueue( TaskExecutor<Void> executor, JobMonitor jobMonitor )
    {
        this.executor = executor;
        this.jobMonitor = jobMonitor;
    }

    synchronized void offer( WriteJob job )
    {
        boolean wasEmpty = queue.isEmpty();
        queue.addLast( job );
        if ( wasEmpty )
        {
            executor.submit( this );
            jobMonitor.jobQueued();
        }
    }

    @Override
    public void run( Void nothing ) throws IOException
    {
        try
        {
            for ( WriteJob job : drain() )
            {
                job.execute();
            }
        }
        finally
        {
            jobMonitor.jobExecuted();
        }
    }

    synchronized WriteJob[] drain()
    {
        WriteJob[] result = new WriteJob[queue.size()];
        queue.toArray( result );
        queue.clear();
        return result;
    }
}
