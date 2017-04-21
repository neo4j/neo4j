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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Format;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.DynamicProcessorAssigner;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.SourceOrCachedInputIterable.cachedForSure;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.MAIN;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseExecution;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.withDynamicProcessorAssignment;

/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 * <p>
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * I/O is only allowed to be read to and written from sequentially, any random access drastically reduces performance.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches between these steps through each stage, i.e. passing batches downstream.
 */
public class ParallelBatchImporter implements BatchImporter
{
    private final File storeDir;
    private final FileSystemAbstraction fileSystem;
    private final Configuration config;
    private final LogService logService;
    private final Log log;
    private final ExecutionMonitor executionMonitor;
    private final AdditionalInitialIds additionalInitialIds;
    private final Config dbConfig;
    private final RecordFormats recordFormats;
    private final PageCache pageCache;

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     */
    public ParallelBatchImporter( File storeDir, FileSystemAbstraction fileSystem, Configuration config,
            LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds,
            Config dbConfig, RecordFormats recordFormats )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.pageCache = null;
        this.config = config;
        this.logService = logService;
        this.dbConfig = dbConfig;
        this.recordFormats = recordFormats;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
    }

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     */
    public ParallelBatchImporter( File storeDir, FileSystemAbstraction fileSystem, PageCache pageCache,
            Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats )
    {
        this.pageCache = pageCache;
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logService = logService;
        this.dbConfig = dbConfig;
        this.recordFormats = recordFormats;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
    }

    /**
     * Instantiates {@link ParallelBatchImporter} with default services and behaviour.
     * The provided {@link ExecutionMonitor} will be decorated with {@link DynamicProcessorAssigner} for
     * optimal assignment of processors to bottleneck steps over time.
     */
    public ParallelBatchImporter( File storeDir, FileSystemAbstraction fileSystem, Configuration config,
            LogService logService, ExecutionMonitor executionMonitor, Config dbConfig )
    {
        this( storeDir, fileSystem, config, logService,
                withDynamicProcessorAssignment( executionMonitor, config ), EMPTY, dbConfig,
                RecordFormatSelector.selectForConfig( dbConfig, NullLogProvider.getInstance() ) );
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        log.info( "Import starting" );

        // Things that we need to close later. The reason they're not in the try-with-resource statement
        // is that we need to close, and set to null, at specific points preferably. So use good ol' finally block.
        long maxMemory = config.maxMemoryUsage();
        NodeRelationshipCache nodeRelationshipCache = null;
        NodeLabelsCache nodeLabelsCache = null;
        long startTime = currentTimeMillis();
        CountingStoreUpdateMonitor storeUpdateMonitor = new CountingStoreUpdateMonitor();
        try ( BatchingNeoStores neoStore = getBatchingNeoStores();
              CountsAccessor.Updater countsUpdater = neoStore.getCountsStore().reset(
                    neoStore.getLastCommittedTransactionId() );
              InputCache inputCache = new InputCache( fileSystem, storeDir, recordFormats, config ) )
        {
            Collector badCollector = input.badCollector();
            // Some temporary caches and indexes in the import
            IoMonitor writeMonitor = new IoMonitor( neoStore.getIoTracer() );
            IdMapper idMapper = input.idMapper();
            IdGenerator idGenerator = input.idGenerator();
            nodeRelationshipCache = new NodeRelationshipCache( AUTO, config.denseNodeThreshold() );
            StatsProvider memoryUsageStats = new MemoryUsageStatsProvider( nodeRelationshipCache, idMapper );
            InputIterable<InputNode> nodes = input.nodes();
            InputIterable<InputRelationship> relationships = input.relationships();
            InputIterable<InputNode> cachedNodes = cachedForSure( nodes, inputCache.nodes( MAIN, true ) );
            InputIterable<InputRelationship> cachedRelationships =
                    cachedForSure( relationships, inputCache.relationships( MAIN, false ) );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();

            // Stage 1 -- nodes, properties, labels
            Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
            NodeStage nodeStage = new NodeStage( nodeConfig, writeMonitor,
                    nodes, idMapper, idGenerator, neoStore, inputCache, neoStore.getLabelScanStore(),
                    storeUpdateMonitor, nodeRelationshipCache, memoryUsageStats );
            neoStore.startFlushingPageCache();
            executeStage( nodeStage );
            neoStore.stopFlushingPageCache();
            if ( idMapper.needsPreparation() )
            {
                executeStage( new IdMapperPreparationStage( config, idMapper, cachedNodes,
                        badCollector, memoryUsageStats ) );
                PrimitiveLongIterator duplicateNodeIds = badCollector.leftOverDuplicateNodesIds();
                if ( duplicateNodeIds.hasNext() )
                {
                    executeStage( new DeleteDuplicateNodesStage( config, duplicateNodeIds, neoStore ) );
                }
            }

            // Stage 2 -- calculate dense node threshold
            Configuration relationshipConfig =
                    configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
            CalculateDenseNodesStage calculateDenseNodesStage = new CalculateDenseNodesStage(
                    relationshipConfig,
                    relationships, nodeRelationshipCache, idMapper, badCollector, inputCache, neoStore );
            executeStage( calculateDenseNodesStage );

            long availableMemory = maxMemory - totalMemoryUsageOf( nodeRelationshipCache, idMapper );
            importRelationships( nodeRelationshipCache, storeUpdateMonitor, neoStore, writeMonitor,
                    idMapper, cachedRelationships, calculateDenseNodesStage.getDistribution(),
                    availableMemory );

            // Release this potentially really big piece of cached data
            long peakMemoryUsage = totalMemoryUsageOf( idMapper, nodeRelationshipCache );
            long highNodeId = nodeRelationshipCache.getHighNodeId();
            idMapper.close();
            idMapper = null;
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;

            new RelationshipGroupDefragmenter( config, executionMonitor ).run( max( maxMemory, peakMemoryUsage ),
                    neoStore, highNodeId );

            // Stage 6 -- count nodes per label and labels per node
            nodeLabelsCache = new NodeLabelsCache( AUTO, neoStore.getLabelRepository().getHighId() );
            memoryUsageStats = new MemoryUsageStatsProvider( nodeLabelsCache );
            executeStage( new NodeCountsStage( config, nodeLabelsCache, neoStore.getNodeStore(),
                    neoStore.getLabelRepository().getHighId(), countsUpdater, memoryUsageStats ) );
            // Stage 7 -- count label-[type]->label
            executeStage( new RelationshipCountsStage( config, nodeLabelsCache, relationshipStore,
                    neoStore.getLabelRepository().getHighId(),
                    neoStore.getRelationshipTypeRepository().getHighId(), countsUpdater, AUTO ) );

            // We're done, do some final logging about it
            long totalTimeMillis = currentTimeMillis() - startTime;
            executionMonitor.done( totalTimeMillis,
                    format( "%n" ) +
                    storeUpdateMonitor.toString() +
                    format( "%n" ) +
                    "Peak memory usage: " + bytes( peakMemoryUsage ) );
            log.info( "Import completed, took " + Format.duration( totalTimeMillis ) + ". " + storeUpdateMonitor );
        }
        catch ( Throwable t )
        {
            log.error( "Error during import", t );
            throw Exceptions.launderedException( IOException.class, t );
        }
        finally
        {
            if ( nodeRelationshipCache != null )
            {
                nodeRelationshipCache.close();
            }
            if ( nodeLabelsCache != null )
            {
                nodeLabelsCache.close();
            }
        }
    }

    private BatchingNeoStores getBatchingNeoStores()
    {
        if ( pageCache == null )
        {
            return BatchingNeoStores.batchingNeoStores( fileSystem, storeDir, recordFormats, config, logService,
                    additionalInitialIds, dbConfig );
        }
        else
        {
            return BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, pageCache,
                    PageCacheTracer.NULL, storeDir, recordFormats, config, logService, additionalInitialIds, dbConfig );
        }
    }

    private long totalMemoryUsageOf( MemoryStatsVisitor.Visitable... users )
    {
        GatheringMemoryStatsVisitor total = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable user : users )
        {
            user.acceptMemoryStatsVisitor( total );
        }
        return total.getHeapUsage() + total.getOffHeapUsage();
    }

    private void importRelationships( NodeRelationshipCache nodeRelationshipCache,
            CountingStoreUpdateMonitor storeUpdateMonitor, BatchingNeoStores neoStore,
            IoMonitor writeMonitor, IdMapper idMapper, InputIterable<InputRelationship> relationships,
            RelationshipTypeDistribution typeDistribution, long freeMemoryForDenseNodeCache )
    {
        // Imports the relationships from the Input. This isn't a straight forward as importing nodes,
        // since keeping track of and updating heads of relationship chains in scenarios where most nodes
        // are dense and there are many relationship types scales poorly w/ regards to cache memory usage
        // also as a side-effect time required to update this cache.
        //
        // The approach is instead to do multiple iterations where each iteration imports relationships
        // of a single type. For each iteration Node --> Relationship and Relationship --> Relationship
        // stages _for dense nodes only_ are run so that the cache can be reused to hold relationship chain heads
        // of the next type in the next iteration. All relationships will be imported this way and then
        // finally there will be one Node --> Relationship and Relationship --> Relationship stage linking
        // all sparse relationship chains together.

        long nextRelationshipId = 0;
        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
        Iterator<Collection<Object>> rounds = nodeRelationshipCache.splitRelationshipTypesIntoRounds(
                typeDistribution.iterator(), freeMemoryForDenseNodeCache );

        // Do multiple rounds of relationship importing. Each round fits as many relationship types
        // as it can (comparing with worst-case memory usage and available memory).
        int typesImported = 0;
        int round = 0;
        for ( round = 0; rounds.hasNext(); round++ )
        {
            // Figure out which types we can fit in node-->relationship cache memory.
            // Types go from biggest to smallest group and so towards the end there will be
            // smaller and more groups per round in this loop
            Collection<Object> typesToImportThisRound = rounds.next();
            boolean thisIsTheOnlyRound = round == 0 && !rounds.hasNext();

            // Import relationships and their properties
            nodeRelationshipCache.setForwardScan( true, true/*dense*/ );
            String range = typesToImportThisRound.size() == 1
                    ? String.valueOf( typesImported + 1 )
                    : (typesImported + 1) + "-" + (typesImported + typesToImportThisRound.size());
            String topic = " " + range + "/" + typeDistribution.getNumberOfRelationshipTypes();
            Predicate<InputRelationship> typeFilter = thisIsTheOnlyRound
                    ? relationship -> true // optimization when all rels are imported in this round
                    : relationship -> typesToImportThisRound.contains( relationship.typeAsObject() );
            RelationshipStage relationshipStage = new RelationshipStage( topic, config,
                    writeMonitor, typeFilter, relationships.iterator(), idMapper, neoStore,
                    nodeRelationshipCache, storeUpdateMonitor, nextRelationshipId );
            neoStore.startFlushingPageCache();
            executeStage( relationshipStage );
            neoStore.stopFlushingPageCache();

            int nodeTypes = thisIsTheOnlyRound ? NodeType.NODE_TYPE_ALL : NodeType.NODE_TYPE_DENSE;

            // Set node nextRel fields for dense nodes
            executeStage( new NodeFirstRelationshipStage( topic, nodeConfig, neoStore.getNodeStore(),
                    neoStore.getTemporaryRelationshipGroupStore(), nodeRelationshipCache, nodeTypes ) );

            // Link relationship chains together for dense nodes
            nodeRelationshipCache.setForwardScan( false, true/*dense*/ );
            executeStage( new RelationshipLinkbackStage( topic,
                    relationshipConfig,
                    neoStore.getRelationshipStore(),
                    nodeRelationshipCache, nextRelationshipId,
                    relationshipStage.getNextRelationshipId(), nodeTypes ) );
            nextRelationshipId = relationshipStage.getNextRelationshipId();
            typesImported += typesToImportThisRound.size();
        }

        // There's an optimization above which will piggy-back sparse linking on the dense linking
        // if all relationships are imported in one round. The sparse linking below will be done if
        // there were multiple passes of dense linking above.
        if ( round > 1 )
        {
            // Set node nextRel fields for sparse nodes
            String topic = " Sparse";
            nodeRelationshipCache.setForwardScan( true, false/*sparse*/ );
            executeStage( new NodeFirstRelationshipStage( topic, nodeConfig, neoStore.getNodeStore(),
                    neoStore.getTemporaryRelationshipGroupStore(), nodeRelationshipCache, NodeType.NODE_TYPE_SPARSE ) );

            // Link relationship chains together for sparse nodes
            nodeRelationshipCache.setForwardScan( false, false/*sparse*/ );
            executeStage( new RelationshipLinkbackStage( topic, relationshipConfig,
                    neoStore.getRelationshipStore(), nodeRelationshipCache, 0, nextRelationshipId,
                    NodeType.NODE_TYPE_SPARSE ) );
        }
    }

    private static Configuration configWithRecordsPerPageBasedBatchSize( Configuration source, RecordStore<?> store )
    {
        return Configuration.withBatchSize( source, store.getRecordsPerPage() * 100 );
    }

    private void executeStage( Stage stage )
    {
        superviseExecution( executionMonitor, config, stage );
    }
}
