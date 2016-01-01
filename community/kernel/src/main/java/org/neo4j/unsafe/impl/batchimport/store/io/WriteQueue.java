/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

class WriteQueue implements Callable<Void>
{
    private final LinkedList<WriteJob> queue = new LinkedList<>();
    private final ExecutorService executor;
    private final JobMonitor jobMonitor;

    public WriteQueue( ExecutorService executor, JobMonitor jobMonitor )
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
    public Void call() throws IOException
    {
        try
        {
            for ( WriteJob job : drain() )
            {
                job.execute();
            }
            return null;
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
