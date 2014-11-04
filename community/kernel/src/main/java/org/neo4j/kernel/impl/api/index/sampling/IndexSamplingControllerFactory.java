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

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.register.Registers.newDoubleLongRegister;

public class IndexSamplingControllerFactory
{
    private final IndexSamplingConfig config;
    private final IndexStoreView storeView;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final Logging logging;

    public IndexSamplingControllerFactory( IndexSamplingConfig config, IndexStoreView storeView,
                                           JobScheduler scheduler, TokenNameLookup tokenNameLookup,
                                           Logging logging )
    {
        this.config = config;
        this.storeView = storeView;
        this.scheduler = scheduler;
        this.tokenNameLookup = tokenNameLookup;
        this.logging = logging;
    }

    public IndexSamplingController create( IndexMapSnapshotProvider snapshotProvider )
    {
        OnlineIndexSamplingJobFactory jobFactory =
                new OnlineIndexSamplingJobFactory( storeView, tokenNameLookup, logging );
        Predicate<IndexDescriptor> samplingUpdatePredicate = createSamplingPredicate();
        IndexSamplingJobQueue jobQueue = new IndexSamplingJobQueue( samplingUpdatePredicate );
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, scheduler );
        Predicate<IndexDescriptor> indexRecoveryCondition = createIndexDescriptorPredicate();
        return new IndexSamplingController(
                config, jobFactory, jobQueue, jobTracker, snapshotProvider, scheduler, indexRecoveryCondition
        );
    }

    private Predicate<IndexDescriptor> createSamplingPredicate()
    {
        return new Predicate<IndexDescriptor>()
        {
            @Override
            public boolean accept( IndexDescriptor descriptor )
            {
                long updates = storeView.indexUpdates( descriptor );

                long size = storeView.indexSize( descriptor );
                long threshold = Math.round( config.updateRatio() * size );

                return updates > threshold;
            }
        };
    }

    private Predicate<IndexDescriptor> createIndexDescriptorPredicate()
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
