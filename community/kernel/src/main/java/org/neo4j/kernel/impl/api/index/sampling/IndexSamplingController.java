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

public class IndexSamplingController
{
    private final IndexSamplingConfig config;
    private final IndexSamplingJobFactory jobFactory;
    private final IndexSamplingJobTracker jobTracker;
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final IndexSamplingJobQueue jobQueue;
    private final Lock emptyLock;

    public IndexSamplingController( IndexSamplingConfig config,
                                    IndexSamplingJobFactory jobFactory,
                                    IndexSamplingJobQueue jobQueue,
                                    IndexSamplingJobTracker jobTracker,
                                    IndexMapSnapshotProvider indexMapSnapshotProvider )
    {
        this.config = config;
        this.jobFactory = jobFactory;
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.jobQueue = jobQueue;
        this.jobTracker = jobTracker;
        this.emptyLock =  new ReentrantLock( true );
    }

    public IndexSamplingConfig config()
    {
        return config;
    }

    public void sampleIndexes( IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        fillQueue( mode, indexMap );

        if ( mode.blockUntilAllScheduled )
        {
            // Wait until last sampling job has been started
            emptyQueue( indexMap );
        }
        else
        {
            // Try to schedule as many sampling jobs as possible
            tryEmptyQueue( indexMap );
        }
    }

    private void fillQueue( IndexSamplingMode mode, IndexMap indexMap )
    {
        Iterator<IndexDescriptor> descriptors = indexMap.descriptors();
        while ( descriptors.hasNext() )
        {
            jobQueue.sampleIndex( mode, descriptors.next() );
        }
    }

    private void tryEmptyQueue( IndexMap indexMap )
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

                    sampleIndex( indexMap, descriptor );
                }
            }
            finally
            {
                emptyLock.unlock();
            }
        }
    }

    private void emptyQueue( IndexMap indexMap )
    {
        emptyLock.lock();
        try
        {
            Iterable<IndexDescriptor> descriptors = jobQueue.pollAll();

            for ( IndexDescriptor descriptor : descriptors )
            {
                jobTracker.waitUntilCanExecuteMoreSamplingJobs();
                sampleIndex( indexMap, descriptor );
            }
        }
        finally
        {
            emptyLock.unlock();
        }
    }

    private void sampleIndex( IndexMap indexMap, IndexDescriptor descriptor )
    {
        IndexProxy proxy = indexMap.getIndexProxy( descriptor );
        if ( proxy == null || proxy.getState() != InternalIndexState.ONLINE )
        {
            return;
        }

        jobTracker.scheduleSamplingJob( jobFactory.create( config, proxy ) );
    }
}
