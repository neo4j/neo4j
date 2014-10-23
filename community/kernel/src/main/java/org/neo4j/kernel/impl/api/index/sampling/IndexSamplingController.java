/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;

public class IndexSamplingController implements Runnable
{
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final IndexSamplingJobTracker jobTracker;
    private final IndexSamplingJobQueue jobQueue;
    private final IndexSamplingJobFactory jobFactory;
    private final Lock emptyLock =  new ReentrantLock( true );

    public IndexSamplingController( IndexSamplingJobFactory jobFactory,
                                    IndexSamplingJobTracker jobTracker,
                                    IndexMapSnapshotProvider indexMapSnapshotProvider )
    {
        this.jobFactory = jobFactory;
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.jobTracker = jobTracker;
        this.jobQueue = new IndexSamplingJobQueue();
    }

    @Override
    public void run()
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        fillQueue( indexMap );
        emptyQueue( indexMap );
    }

    private void fillQueue( IndexMap indexMap )
    {
        Iterator<IndexDescriptor> descriptors = indexMap.descriptors();
        while ( descriptors.hasNext() )
        {
            IndexDescriptor descriptor = descriptors.next();
            jobQueue.sampleIndex( descriptor );
        }
    }

    private void emptyQueue( IndexMap indexMap )
    {
        if ( emptyLock.tryLock() )
        {
            try
            {
                while ( jobTracker.canExecuteMoreSamplingJobs() )
                {
                    IndexDescriptor descriptor = jobQueue.poll();
                    if ( descriptor == null )
                    {
                        return;
                    }

                    IndexProxy proxy = indexMap.getIndexProxy( descriptor );
                    if ( proxy == null || proxy.getState() != InternalIndexState.ONLINE )
                    {
                        continue;
                    }

                    jobTracker.scheduleSamplingJob( jobFactory.create( proxy ) );
                }
            }
            finally
            {
                emptyLock.unlock();
            }
        }
    }
}
