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
package org.neo4j.unsafe.impl.batchimport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;

/**
 * Processes relationship records, feeding them to {@link RelationshipCountsProcessor} which keeps
 * the accumulated counts per thread. Aggregated in {@link #done()}.
 */
public class ProcessRelationshipCountsDataStep extends ProcessorStep<RelationshipRecord[]>
{
    private final NodeLabelsCache cache;
    private final Map<Thread,RelationshipCountsProcessor> processors = new ConcurrentHashMap<>();
    private final int highLabelId;
    private final int highRelationshipTypeId;
    private final CountsAccessor.Updater countsUpdater;
    private final NumberArrayFactory cacheFactory;
    private final ProgressReporter progressMonitor;

    public ProcessRelationshipCountsDataStep( StageControl control, NodeLabelsCache cache, Configuration config, int
            highLabelId, int highRelationshipTypeId,
            CountsAccessor.Updater countsUpdater, NumberArrayFactory cacheFactory,
            ProgressReporter progressReporter )
    {
        super( control, "COUNT", config, numberOfProcessors( config, cache, highLabelId, highRelationshipTypeId ) );
        this.cache = cache;
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
        this.countsUpdater = countsUpdater;
        this.cacheFactory = cacheFactory;
        this.progressMonitor = progressReporter;
    }

    /**
     * Keeping all counts for all combinations of label/reltype can require a lot of memory if there are lots of those tokens.
     * Each processor will allocate such a data structure and so in extreme cases the number of processors will have to
     * be limited to not surpass the available memory limits.
     *
     * @param config {@link Configuration} holding things like max number of processors and max memory.
     * @param cache {@link NodeLabelsCache} which is the only other data structure occupying memory at this point.
     * @param highLabelId high label id for this store.
     * @param highRelationshipTypeId high relationship type id for this store.
     * @return number of processors suitable for this step. In most cases this will be 0, which is the typical value used
     * when just allowing the importer to grab up to {@link Configuration#maxNumberOfProcessors()}. The returned value
     * will at least be 1.
     */
    private static int numberOfProcessors( Configuration config, NodeLabelsCache cache, int highLabelId, int highRelationshipTypeId )
    {
        GatheringMemoryStatsVisitor memVisitor = new GatheringMemoryStatsVisitor();
        cache.acceptMemoryStatsVisitor( memVisitor );

        long availableMem = config.maxMemoryUsage() - memVisitor.getTotalUsage();
        long threadMem = RelationshipCountsProcessor.calculateMemoryUsage( highLabelId, highRelationshipTypeId );
        long possibleThreads = availableMem / threadMem;
        return possibleThreads >= config.maxNumberOfProcessors() ? 0 : toIntExact( max( 1, possibleThreads ) );
    }

    @Override
    protected void process( RelationshipRecord[] batch, BatchSender sender )
    {
        RelationshipCountsProcessor processor = processor();
        for ( RelationshipRecord record : batch )
        {
            if ( record.inUse() )
            {
                processor.process( record );
            }
        }
        progressMonitor.progress( batch.length );
    }

    private RelationshipCountsProcessor processor()
    {
        // This is OK since in this step implementation we use TaskExecutor which sticks to its threads deterministically.
        return processors.computeIfAbsent( Thread.currentThread(),
                k -> new RelationshipCountsProcessor( cache, highLabelId, highRelationshipTypeId, countsUpdater, cacheFactory ) );
    }

    @Override
    protected void done()
    {
        super.done();
        RelationshipCountsProcessor all = null;
        for ( RelationshipCountsProcessor processor : processors.values() )
        {
            if ( all == null )
            {
                all = processor;
            }
            else
            {
                all.addCountsFrom( processor );
            }
        }
        if ( all != null )
        {
            all.done();
        }

        for ( RelationshipCountsProcessor processor : processors.values() )
        {
            processor.close();
        }
    }
}
