/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.apache.lucene.util.NamedThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.neo4j.unsafe.impl.batchimport.DataStatistics.RelationshipTypeCount;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.StageExecution;
import org.neo4j.unsafe.impl.batchimport.staging.Step;
import org.neo4j.unsafe.impl.batchimport.stats.Key;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.unsafe.impl.batchimport.stats.Stats.longStat;

/**
 * Imports data from {@link Input} into a store. Unlinked entity data and property data is imported here.
 * Linking records, except properties, with each other is not done in here.
 *
 * Main design goal here is low garbage generation and having as much as possible able to withstand multiple
 * threads passing through. So each import consists of instantiating an input source reader, optimal number
 * of threads and letting each thread:
 * <ol>
 * <li>Get {@link InputChunk chunk} of data and for every entity in it:</li>
 * <li>Parse its data, filling current record with data using {@link InputEntityVisitor} callback from parsing</li>
 * <li>Write record(s)</li>
 * <li>Repeat until no more chunks from input.</li>
 * </ol>
 */
public class DataImporter
{
    public static final String ID_PROPERTY = "__id";
    public static final String NODE_IMPORT_NAME = "Nodes";
    public static final String RELATIONSHIP_IMPORT_NAME = "Relationships";

    public static class Monitor
    {
        private final LongAdder nodes = new LongAdder();
        private final LongAdder relationships = new LongAdder();
        private final LongAdder properties = new LongAdder();

        public void nodesImported( long nodes )
        {
            this.nodes.add( nodes );
        }

        public void relationshipsImported( long relationships )
        {
            this.relationships.add( relationships );
        }

        public void propertiesImported( long properties )
        {
            this.properties.add( properties );
        }

        @Override
        public String toString()
        {
            return format( "Imported:%n  %d nodes%n  %d relationships%n  %d properties",
                    nodes.sum(), relationships.sum(), properties.sum() );
        }
    }

    private static long importData( String title, int numRunners, InputIterable data, BatchingNeoStores stores,
            Supplier<EntityImporter> visitors, ExecutionMonitor executionMonitor, StatsProvider memoryStatsProvider )
            throws IOException
    {
        LongAdder roughEntityCountProgress = new LongAdder();
        ExecutorService pool = Executors.newFixedThreadPool( numRunners,
                new NamedThreadFactory( title + "Importer" ) );
        IoMonitor writeMonitor = new IoMonitor( stores.getIoTracer() );
        ControllableStep step = new ControllableStep( title, roughEntityCountProgress, Configuration.DEFAULT,
                writeMonitor, memoryStatsProvider );
        StageExecution execution = new StageExecution( title, null, Configuration.DEFAULT, Collections.singletonList( step ), 0 );
        InputIterator dataIterator = data.iterator();
        for ( int i = 0; i < numRunners; i++ )
        {
            pool.submit( new ExhaustingEntityImporterRunnable(
                    execution, dataIterator, visitors.get(), roughEntityCountProgress ) );
        }
        pool.shutdown();

        executionMonitor.start( execution );
        long startTime = currentTimeMillis();
        long nextWait = 0;
        try
        {
            while ( !pool.awaitTermination( nextWait, TimeUnit.MILLISECONDS ) )
            {
                executionMonitor.check( execution );
                nextWait = executionMonitor.nextCheckTime() - currentTimeMillis();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IOException( e );
        }
        execution.assertHealthy();
        step.markAsCompleted();
        writeMonitor.stop();
        executionMonitor.end( execution, currentTimeMillis() - startTime );

        return roughEntityCountProgress.sum();
    }

    public static void importNodes( int numRunners, Input input, BatchingNeoStores stores, IdMapper idMapper,
            NodeRelationshipCache nodeRelationshipCache, ExecutionMonitor executionMonitor, Monitor monitor )
            throws IOException
    {
        importData( NODE_IMPORT_NAME, numRunners, input.nodes(), stores, () ->
            new NodeImporter( stores, idMapper, monitor ), executionMonitor, new MemoryUsageStatsProvider( stores, idMapper ) );
        nodeRelationshipCache.setNodeCount( stores.getNodeStore().getHighId() );
    }

    public static DataStatistics importRelationships( int numRunners, Input input,
            BatchingNeoStores stores, IdMapper idMapper, Collector badCollector, ExecutionMonitor executionMonitor,
            Monitor monitor, boolean validateRelationshipData )
                    throws IOException
    {
        DataStatistics typeDistribution = new DataStatistics( monitor.nodes.sum(), monitor.properties.sum(), new RelationshipTypeCount[0] );
        importData( RELATIONSHIP_IMPORT_NAME, numRunners, input.relationships(), stores, () ->
                new RelationshipImporter( stores, idMapper, typeDistribution, monitor, badCollector, validateRelationshipData,
                        stores.usesDoubleRelationshipRecordUnits() ), executionMonitor, new MemoryUsageStatsProvider( stores, idMapper ) );
        return typeDistribution;
    }

    /**
     * Here simply to be able to fit into the ExecutionMonitor thing
     */
    private static class ControllableStep implements Step<Void>, StatsProvider
    {
        private final String name;
        private final LongAdder progress;
        private final int batchSize;
        private final Key[] keys = new Key[] {Keys.done_batches, Keys.avg_processing_time};
        private final Collection<StatsProvider> statsProviders = new ArrayList<>();

        private volatile boolean completed;

        ControllableStep( String name, LongAdder progress, Configuration config, StatsProvider... additionalStatsProviders )
        {
            this.name = name;
            this.progress = progress;
            this.batchSize = config.batchSize(); // just to be able to report correctly

            statsProviders.add( this );
            statsProviders.addAll( Arrays.asList( additionalStatsProviders ) );
        }

        void markAsCompleted()
        {
            this.completed = true;
        }

        @Override
        public void receivePanic( Throwable cause )
        {
        }

        @Override
        public void start( int orderingGuarantees )
        {
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public long receive( long ticket, Void batch )
        {
            return 0;
        }

        @Override
        public StepStats stats()
        {
            return new StepStats( name, completed, statsProviders );
        }

        @Override
        public void endOfUpstream()
        {
        }

        @Override
        public boolean isCompleted()
        {
            return completed;
        }

        @Override
        public void setDownstream( Step<?> downstreamStep )
        {
        }

        @Override
        public void close() throws Exception
        {
        }

        @Override
        public Stat stat( Key key )
        {
            if ( key == Keys.done_batches )
            {
                return longStat( progress.sum() / batchSize );
            }
            if ( key == Keys.avg_processing_time )
            {
                return longStat( 10 );
            }
            return null;
        }

        @Override
        public Key[] keys()
        {
            return keys;
        }
    }
}
