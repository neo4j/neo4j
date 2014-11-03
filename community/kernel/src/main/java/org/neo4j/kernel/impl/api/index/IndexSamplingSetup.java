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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.ValueSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingJobQueue;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingJobTracker;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.OnlineIndexSamplingJobFactory;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.logging.Logging;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.BACKGROUND_REBUILD_UPDATED;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.indexSamplingController;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class IndexSamplingSetup
{
    private final IndexSamplingConfig samplingConfig;
    private final IndexStoreView storeView;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final Logging logging;

    public IndexSamplingSetup( IndexSamplingConfig samplingConfig, IndexStoreView storeView,
                               JobScheduler scheduler, TokenNameLookup tokenNameLookup, Logging logging )
    {
        this.samplingConfig = samplingConfig;
        this.storeView = storeView;
        this.scheduler = scheduler;
        this.tokenNameLookup = tokenNameLookup;
        this.logging = logging;
    }

    public IndexSamplingController createIndexSamplingController( IndexMapSnapshotProvider snapshotProvider )
    {
        OnlineIndexSamplingJobFactory jobFactory =
                new OnlineIndexSamplingJobFactory( storeView, tokenNameLookup, logging );
        IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue( createSamplingUpdatePredicate( samplingConfig ) );
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( samplingConfig, scheduler );
        return new IndexSamplingController( samplingConfig, jobFactory, jobQueue, jobTracker, snapshotProvider );
    }

    public ValueSampler createValueSampler( boolean unique )
    {
        return unique ? new UniqueIndexSampler() : new NonUniqueIndexSampler( samplingConfig.bufferSize() );
    }

    private Predicate<IndexDescriptor> createSamplingUpdatePredicate( final IndexSamplingConfig samplingConfig )
    {
        return new Predicate<IndexDescriptor>()
        {
            @Override
            public boolean accept( IndexDescriptor descriptor )
            {
                long updates = storeView.indexUpdates( descriptor );

                long size = storeView.indexSize( descriptor );
                long threshold = Math.round( samplingConfig.updateRatio() * size );

                return updates > threshold;
            }
        };
    }

    public void scheduleBackgroundJob( final IndexSamplingController samplingController )
    {
        if ( samplingConfig.backgroundSampling() )
        {
            Runnable samplingRunner = new Runnable()
            {
                @Override
                public void run()
                {
                    samplingController.sampleIndexes( BACKGROUND_REBUILD_UPDATED );
                }
            };
            scheduler.scheduleRecurring( indexSamplingController, samplingRunner, 10, SECONDS );
        }
    }

    public Predicate<IndexDescriptor> reSamplingPredicate()
    {
        return new Predicate<IndexDescriptor>()
        {
            @Override
            public boolean accept( IndexDescriptor descriptor )
            {
                return storeView.indexSample( descriptor, newDoubleLongRegister() ).readSecond() == 0;
            }
        };
    }
}
