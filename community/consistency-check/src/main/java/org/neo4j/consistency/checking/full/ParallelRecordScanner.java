/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.QueueDistribution.QueueDistributor;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.progress.ProgressMonitorFactory.MultiPartBuilder;
import org.neo4j.kernel.api.direct.BoundedIterable;

import static org.neo4j.consistency.checking.cache.DefaultCacheAccess.DEFAULT_QUEUE_SIZE;
import static org.neo4j.consistency.checking.full.RecordDistributor.distributeRecords;

public class ParallelRecordScanner<RECORD> extends RecordScanner<RECORD>
{
    private final CacheAccess cacheAccess;
    private final QueueDistribution distribution;

    public ParallelRecordScanner( String name, Statistics statistics, int threads, BoundedIterable<RECORD> store,
            MultiPartBuilder builder, RecordProcessor<RECORD> processor, CacheAccess cacheAccess,
            QueueDistribution distribution,
            IterableStore... warmUpStores )
    {
        super( name, statistics, threads, store, builder, processor, warmUpStores );
        this.cacheAccess = cacheAccess;
        this.distribution = distribution;
    }

    @Override
    protected void scan()
    {
        long recordsPerCPU = RecordDistributor.calculateRecodsPerCpu( store.maxCount(), numberOfThreads );
        cacheAccess.prepareForProcessingOfSingleStore( recordsPerCPU );

        QueueDistributor<RECORD> distributor = distribution.distributor( recordsPerCPU, numberOfThreads );
        distributeRecords( numberOfThreads, getClass().getSimpleName() + "-" + name,
                DEFAULT_QUEUE_SIZE, store, progress, processor, distributor );
    }
}
