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

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class IndexSamplingController implements Runnable
{
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue();
    private final IndexSamplingJobTracker jobTracker;
    private final StringLogger logger;

    public IndexSamplingController( Logging logging, JobScheduler scheduler, IndexMapSnapshotProvider indexMapSnapshotProvider, int samplingJobLimit )
    {
        this.logger = logging.getMessagesLog( logging.getClass() );
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.jobTracker = new IndexSamplingJobTracker( scheduler, samplingJobLimit );
    }

    @Override
    public void run()
    {
        fillQueue();
        emptyQueue();
    }

    private void fillQueue()
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        Iterator<IndexDescriptor> descriptors = indexMap.descriptors();
        while ( descriptors.hasNext() )
        {
            IndexDescriptor descriptor = descriptors.next();
            jobQueue.sampleIndex( descriptor );
        }
    }

    private void emptyQueue()
    {

        while ( jobTracker.canExecuteMoreSamplingJobs() )
        {
            IndexDescriptor descriptor = jobQueue.poll();
            if ( descriptor == null )
            {
                return;
            }

            jobTracker.scheduleSamplingJob( sampleIndex( descriptor ) );
        }
    }

    private Runnable sampleIndex( final IndexDescriptor descriptor )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println( "I has sampled teh index: " + descriptor );
                logger.warn( "I has sampled teh index: " + descriptor );
            }
        };
    }

}
