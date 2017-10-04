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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
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
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeLabelsCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
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
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;
import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.SourceOrCachedInputIterable.cachedForSure;
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
    private final PageCache externalPageCache;

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     *
     * @param externalPageCache a {@link PageCache} to use, otherwise {@code null} where an appropriate one will be created.
     */
    public ParallelBatchImporter( File storeDir, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats )
    {
        this.externalPageCache = externalPageCache;
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
        this( storeDir, fileSystem, null, config, logService,
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
        long totalTimeMillis;
        try ( BatchingNeoStores neoStore = getBatchingNeoStores();
              InputCache inputCache = new InputCache( fileSystem, storeDir, recordFormats, config ) )
        {
            NumberArrayFactory numberArrayFactory =
                    NumberArrayFactory.auto( neoStore.getPageCache(), storeDir, config.allowCacheAllocationOnHeap() );
            Collector badCollector = input.badCollector();
            // Some temporary caches and indexes in the import
            IoMonitor writeMonitor = new IoMonitor( neoStore.getIoTracer() );
            IdMapper idMapper = input.idMapper( numberArrayFactory );
            IdGenerator idGenerator = input.idGenerator();
            nodeRelationshipCache = new NodeRelationshipCache( numberArrayFactory, config.denseNodeThreshold() );
            StatsProvider memoryUsageStats = new MemoryUsageStatsProvider( nodeRelationshipCache, idMapper );
            InputIterable<InputNode> nodes = input.nodes();
            InputIterable<InputRelationship> relationships = input.relationships();
            InputIterable<InputNode> cachedNodes = cachedForSure( nodes, inputCache.nodes( MAIN, true ) );

            RelationshipStore relationshipStore = neoStore.getRelationshipStore();

            // Import nodes, properties, labels
            Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
            NodeStage nodeStage = new NodeStage( nodeConfig, writeMonitor,
                    nodes, idMapper, idGenerator, neoStore, inputCache, neoStore.getLabelScanStore(),
                    storeUpdateMonitor, memoryUsageStats );
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
            // Import relationships (unlinked), properties
            Configuration relationshipConfig =
                    configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
            RelationshipStage unlinkedRelationshipStage =
                    new RelationshipStage( relationshipConfig, writeMonitor, relationships, idMapper,
                            badCollector, inputCache, neoStore, storeUpdateMonitor );
            neoStore.startFlushingPageCache();
            executeStage( unlinkedRelationshipStage );
            neoStore.stopFlushingPageCache();
            idMapper.close();
            idMapper = null;
            // Link relationships together with each other, their nodes and their relationship groups
            long availableMemory = maxMemory - totalMemoryUsageOf( nodeRelationshipCache, neoStore );
            // This is where the nodeRelationshipCache is allocated memory.
            // This has to happen after idMapped is released
            nodeRelationshipCache.setHighNodeId( neoStore.getNodeStore().getHighId() );
            NodeDegreeCountStage nodeDegreeStage = new NodeDegreeCountStage( relationshipConfig,
                    neoStore.getRelationshipStore(), nodeRelationshipCache );
            neoStore.startFlushingPageCache();
            executeStage( nodeDegreeStage );
            neoStore.stopFlushingPageCache();

            linkData( nodeRelationshipCache, neoStore, unlinkedRelationshipStage.getDistribution(),
                    availableMemory );

            // Release this potentially really big piece of cached data
            long peakMemoryUsage = totalMemoryUsageOf( nodeRelationshipCache, neoStore );
            long highNodeId = nodeRelationshipCache.getHighNodeId();
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;
            // Defragment relationships groups for better performance
            new RelationshipGroupDefragmenter( config, executionMonitor, numberArrayFactory )
                    .run( max( maxMemory, peakMemoryUsage ), neoStore, highNodeId );

            // Count nodes per label and labels per node
            try ( CountsAccessor.Updater countsUpdater = neoStore.getCountsStore().reset(
                    neoStore.getLastCommittedTransactionId() ) )
            {
                MigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
                nodeLabelsCache = new NodeLabelsCache( numberArrayFactory, neoStore.getLabelRepository().getHighId() );
                memoryUsageStats = new MemoryUsageStatsProvider( nodeLabelsCache );
                executeStage( new NodeCountsStage( config, nodeLabelsCache, neoStore.getNodeStore(),
                        neoStore.getLabelRepository().getHighId(), countsUpdater, progressMonitor.startSection( "Nodes" ),
                        memoryUsageStats ) );
                // Count label-[type]->label
                executeStage( new RelationshipCountsStage( config, nodeLabelsCache, relationshipStore,
                        neoStore.getLabelRepository().getHighId(),
                        neoStore.getRelationshipTypeRepository().getHighId(),
                        countsUpdater, numberArrayFactory, progressMonitor.startSection( "Relationships" ) ) );
            }

            // We're done, do some final logging about it
            totalTimeMillis = currentTimeMillis() - startTime;
            executionMonitor.done( totalTimeMillis,
                    format( "%n" ) +
                    storeUpdateMonitor.toString() +
                    format( "%n" ) +
                    "Peak memory usage: " + bytes( peakMemoryUsage ) );
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

        log.info( "Import completed successfully, took " + Format.duration( totalTimeMillis ) + ". " + storeUpdateMonitor );
    }

    private BatchingNeoStores getBatchingNeoStores()
    {
        if ( externalPageCache == null )
        {
            return BatchingNeoStores.batchingNeoStores( fileSystem, storeDir, recordFormats, config, logService,
                    additionalInitialIds, dbConfig );
        }
        else
        {
            return BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, externalPageCache,
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

    /**
     * Performs one or more rounds linking together relationships with each other. Number of rounds required
     * is dictated by available memory. The more dense nodes and relationship types, the more memory required.
     * Every round all relationships of one or more types are linked.
     *
     * Links together:
     * <ul>
     * <li>
     * Relationship <--> Relationship. Two sequential passes are made over the relationship store.
     * The forward pass links next pointers, each next pointer pointing "backwards" to lower id.
     * The backward pass links prev pointers, each prev pointer pointing "forwards" to higher id.
     * </li>
     * Sparse Node --> Relationship. Sparse nodes are updated with relationship heads of completed chains.
     * This is done in the first round only, if there are multiple rounds.
     * </li>
     * </ul>
     *
     * Other linking happens after this method.
     *
     * @param nodeRelationshipCache cache to use for linking.
     * @param neoStore the stores.
     * @param typeDistribution distribution of imported relationship types.
     * @param freeMemoryForDenseNodeCache max available memory to use for caching.
     */
    private void linkData( NodeRelationshipCache nodeRelationshipCache,
            BatchingNeoStores neoStore, RelationshipTypeDistribution typeDistribution,
            long freeMemoryForDenseNodeCache )
    {
        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
        Iterator<Collection<Object>> rounds = nodeRelationshipCache.splitRelationshipTypesIntoRounds(
                typeDistribution.iterator(), freeMemoryForDenseNodeCache );
        Configuration groupConfig =
                configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipGroupStore() );

        // Do multiple rounds of relationship linking. Each round fits as many relationship types
        // as it can (comparing with worst-case memory usage and available memory).
        int typesImported = 0;
        int round = 0;
        for ( round = 0; rounds.hasNext(); round++ )
        {
            // Figure out which types we can fit in node-->relationship cache memory.
            // Types go from biggest to smallest group and so towards the end there will be
            // smaller and more groups per round in this loop
            Collection<Object> typesToLinkThisRound = rounds.next();
            boolean thisIsTheFirstRound = round == 0;
            boolean thisIsTheOnlyRound = thisIsTheFirstRound && !rounds.hasNext();

            nodeRelationshipCache.setForwardScan( true, true/*dense*/ );
            String range = typesToLinkThisRound.size() == 1
                    ? String.valueOf( typesImported + 1 )
                    : (typesImported + 1) + "-" + (typesImported + typesToLinkThisRound.size());
            String topic = " " + range + "/" + typeDistribution.getNumberOfRelationshipTypes();
            int nodeTypes = thisIsTheFirstRound ? NodeType.NODE_TYPE_ALL : NodeType.NODE_TYPE_DENSE;
            Predicate<RelationshipRecord> readFilter = thisIsTheFirstRound
                    ? null // optimization when all rels are imported in this round
                    : typeIdFilter( typesToLinkThisRound, neoStore.getRelationshipTypeRepository() );
            Predicate<RelationshipRecord> denseChangeFilter = thisIsTheOnlyRound
                    ? null // optimization when all rels are imported in this round
                    : typeIdFilter( typesToLinkThisRound, neoStore.getRelationshipTypeRepository() );

            // LINK Forward
            RelationshipLinkforwardStage linkForwardStage = new RelationshipLinkforwardStage( topic, relationshipConfig,
                    neoStore.getRelationshipStore(), nodeRelationshipCache, readFilter, denseChangeFilter, nodeTypes );
            executeStage( linkForwardStage );

            // Write relationship groups cached from the relationship import above
            executeStage( new RelationshipGroupStage( topic, groupConfig,
                    neoStore.getTemporaryRelationshipGroupStore(), nodeRelationshipCache ) );
            if ( thisIsTheFirstRound )
            {
                // Set node nextRel fields for sparse nodes
                executeStage( new SparseNodeFirstRelationshipStage( nodeConfig, neoStore.getNodeStore(),
                        nodeRelationshipCache ) );
            }

            // LINK backward
            nodeRelationshipCache.setForwardScan( false, true/*dense*/ );
            executeStage( new RelationshipLinkbackStage( topic, relationshipConfig, neoStore.getRelationshipStore(),
                    nodeRelationshipCache, readFilter, denseChangeFilter, nodeTypes ) );
            typesImported += typesToLinkThisRound.size();
        }
    }

    private static Predicate<RelationshipRecord> typeIdFilter( Collection<Object> typesToLinkThisRound,
            BatchingRelationshipTypeTokenRepository relationshipTypeRepository )
    {
        PrimitiveIntSet set = Primitive.intSet( typesToLinkThisRound.size() );
        for ( Object type : typesToLinkThisRound )
        {
            int id;
            if ( type instanceof Number )
            {
                id = ((Number) type).intValue();
            }
            else
            {
                id = relationshipTypeRepository.applyAsInt( type );
            }
            set.add( id );
        }
        return relationship -> set.contains( relationship.getType() );
    }

    private static Configuration configWithRecordsPerPageBasedBatchSize( Configuration source, RecordStore<?> store )
    {
        return Configuration.withBatchSize( source, store.getRecordsPerPage() * 10 );
    }

    private void executeStage( Stage stage )
    {
        superviseExecution( executionMonitor, config, stage );
    }
}
