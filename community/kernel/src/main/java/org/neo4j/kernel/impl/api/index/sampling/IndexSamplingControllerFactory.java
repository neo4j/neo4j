/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.function.LongPredicate;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class IndexSamplingControllerFactory
{
    private final IndexSamplingConfig samplingConfig;
    private final IndexStatisticsStore indexStatisticsStore;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final LogProvider logProvider;
    private final PageCacheTracer cacheTracer;
    private final Config config;
    private final String databaseName;

    public IndexSamplingControllerFactory( IndexSamplingConfig samplingConfig, IndexStatisticsStore indexStatisticsStore,
                                           JobScheduler scheduler, TokenNameLookup tokenNameLookup,
                                           LogProvider logProvider, PageCacheTracer cacheTracer, Config config, String databaseName )
    {
        this.samplingConfig = samplingConfig;
        this.indexStatisticsStore = indexStatisticsStore;
        this.scheduler = scheduler;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
        this.cacheTracer = cacheTracer;
        this.config = config;
        this.databaseName = databaseName;
    }

    public IndexSamplingController create( IndexMapSnapshotProvider snapshotProvider )
    {
        OnlineIndexSamplingJobFactory jobFactory = new OnlineIndexSamplingJobFactory( indexStatisticsStore, tokenNameLookup, logProvider, cacheTracer );
        LongPredicate samplingUpdatePredicate = createSamplingPredicate();
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( scheduler, databaseName );
        RecoveryCondition indexRecoveryCondition = createIndexRecoveryCondition( logProvider, tokenNameLookup );
        return new IndexSamplingController( samplingConfig, jobFactory, samplingUpdatePredicate, jobTracker, snapshotProvider, scheduler,
                indexRecoveryCondition, logProvider, config, databaseName );
    }

    private LongPredicate createSamplingPredicate()
    {
        return indexId -> {
            var indexInfo = indexStatisticsStore.indexSample( indexId );
            long updates = indexInfo.updates();
            long size = indexInfo.indexSize();
            long threshold = Math.round( samplingConfig.updateRatio() * size );
            return updates > threshold;
        };
    }

    private RecoveryCondition createIndexRecoveryCondition( final LogProvider logProvider,
                                                                     final TokenNameLookup tokenNameLookup )
    {
        return new RecoveryCondition()
        {
            private final Log log = logProvider.getLog( IndexSamplingController.class );

            @Override
            public boolean test( IndexDescriptor descriptor )
            {
                IndexSample indexSample = indexStatisticsStore.indexSample( descriptor.getId() );
                long samples = indexSample.sampleSize();
                long size = indexSample.indexSize();
                boolean empty = (samples == 0) || (size == 0);
                if ( empty )
                {
                    log.debug( "Recovering index sampling for index %s", descriptor.schema().userDescription( tokenNameLookup ) );
                }
                return empty;
            }
        };
    }
}
