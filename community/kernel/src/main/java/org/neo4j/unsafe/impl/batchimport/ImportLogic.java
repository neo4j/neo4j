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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.logging.Log;
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
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.io.IoMonitor;

import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.unsafe.impl.batchimport.SourceOrCachedInputIterable.cachedForSure;
import static org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.calculateMaxMemoryUsage;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.auto;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.MAIN;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.superviseExecution;

/**
 * Contains all algorithms and logic for doing an import. It exposes all stages as methods so that
 * it's possible to implement a {@link BatchImporter} which calls those.
 * This class has state which typically gets modified in each invocation of an import method.
 *
 * To begin with the methods are fairly coarse-grained, but can and probably will be split up into smaller parts
 * to allow external implementors have greater control over the flow.
 */
public class ImportLogic implements Closeable
{
    private final File storeDir;
    private final FileSystemAbstraction fileSystem;
    private final BatchingNeoStores neoStore;
    private final Configuration config;
    private final Log log;
    private final ExecutionMonitor executionMonitor;
    private final RecordFormats recordFormats;
    private final CountingStoreUpdateMonitor storeUpdateMonitor = new CountingStoreUpdateMonitor();
    private final long maxMemory;

    // This map contains additional state that gets populated, created and used throughout the stages.
    // The reason that this is a map is to allow for a uniform way of accessing and loading this stage
    // from the outside. Currently these things live here:
    //   - RelationshipTypeDistribution
    private final Map<Class<?>,Object> accessibleState = new HashMap<>();

    // components which may get assigned and unassigned in some methods
    private NodeRelationshipCache nodeRelationshipCache;
    private NodeLabelsCache nodeLabelsCache;
    private long startTime;
    private InputCache inputCache;
    private NumberArrayFactory numberArrayFactory;
    private Collector badCollector;
    private IoMonitor writeMonitor;
    private IdMapper idMapper;
    private IdGenerator idGenerator;
    private MemoryUsageStatsProvider memoryUsageStats;
    private InputIterable<InputNode> nodes;
    private InputIterable<InputRelationship> relationships;
    private InputIterable<InputNode> cachedNodes;
    private long peakMemoryUsage;
    private long availableMemoryForLinking;

    /**
     * Advanced usage of the parallel batch importer, for special and very specific cases. Please use
     * a constructor with fewer arguments instead.
     *
     * @param storeDir directory which the db will be created in.
     * @param fileSystem {@link FileSystemAbstraction} that the {@code storeDir} lives in.
     * @param neoStore {@link BatchingNeoStores} to import into.
     * @param config import-specific {@link Configuration}.
     * @param logService {@link LogService} to use.
     * @param executionMonitor {@link ExecutionMonitor} to follow progress as the import proceeds.
     * @param recordFormats which {@link RecordFormats record format} to use for the created db.
     * @param input {@link Input} containing the data to import.
     */
    public ImportLogic( File storeDir, FileSystemAbstraction fileSystem, BatchingNeoStores neoStore,
            Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            RecordFormats recordFormats, Input input )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.neoStore = neoStore;
        this.config = config;
        this.recordFormats = recordFormats;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.executionMonitor = ExecutionSupervisors.withDynamicProcessorAssignment( executionMonitor, config );
        this.maxMemory = config.maxMemoryUsage();

        initialize( input );
    }

    private void initialize( Input input )
    {
        log.info( "Import starting" );
        startTime = currentTimeMillis();
        inputCache = new InputCache( fileSystem, storeDir, recordFormats, config );

        numberArrayFactory = auto( neoStore.getPageCache(), storeDir, config.allowCacheAllocationOnHeap() );
        badCollector = input.badCollector();
        // Some temporary caches and indexes in the import
        writeMonitor = new IoMonitor( neoStore.getIoTracer() );
        idMapper = input.idMapper( numberArrayFactory );
        idGenerator = input.idGenerator();
        nodeRelationshipCache = new NodeRelationshipCache( numberArrayFactory, config.denseNodeThreshold() );
        memoryUsageStats = new MemoryUsageStatsProvider( nodeRelationshipCache, idMapper );
        nodes = input.nodes();
        relationships = input.relationships();
        cachedNodes = cachedForSure( nodes, inputCache.nodes( MAIN, true ) );
    }

    /**
     * Accesses state of a certain {@code type}. This is state that may be long- or short-lived and perhaps
     * created in one part of the import to be used in another.
     *
     * @param type {@link Class} of the state to get.
     * @return the state of the given type.
     * @throws IllegalStateException if the state of the given {@code type} isn't available.
     */
    public <T> T getState( Class<T> type )
    {
        return type.cast( accessibleState.get( type ) );
    }

    /**
     * Puts state of a certain type.
     *
     * @param state state instance to set.
     * @see #getState(Class)
     * @throws IllegalStateException if state of this type has already been defined.
     */
    public <T> void putState( T state )
    {
        accessibleState.put( state.getClass(), state );
    }

    /**
     * Imports nodes w/ their properties and labels from {@link Input#nodes()}. This will as a side-effect populate the {@link IdMapper},
     * to later be used for looking up ID --> nodeId in {@link #importRelationships()}. After a completed node import,
     * {@link #prepareIdMapper()} must be called.
     *
     * @throws IOException on I/O error.
     */
    public void importNodes() throws IOException
    {
        // Import nodes, properties, labels
        Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
        NodeStage nodeStage = new NodeStage( nodeConfig, writeMonitor,
                nodes, idMapper, idGenerator, neoStore, inputCache, neoStore.getLabelScanStore(),
                storeUpdateMonitor, memoryUsageStats );
        neoStore.startFlushingPageCache();
        executeStage( nodeStage );
        neoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
    }

    /**
     * Prepares {@link IdMapper} to be queried for ID --> nodeId lookups. This is required for running {@link #importRelationships()}.
     */
    public void prepareIdMapper()
    {
        if ( idMapper.needsPreparation() )
        {
            executeStage( new IdMapperPreparationStage( config, idMapper, cachedNodes,
                    badCollector, memoryUsageStats ) );
            PrimitiveLongIterator duplicateNodeIds = badCollector.leftOverDuplicateNodesIds();
            if ( duplicateNodeIds.hasNext() )
            {
                executeStage( new DeleteDuplicateNodesStage( config, duplicateNodeIds, neoStore ) );
            }
            updatePeakMemoryUsage();
        }
    }

    /**
     * Uses {@link IdMapper} as lookup for ID --> nodeId and imports all relationships from {@link Input#relationships()}
     * and writes them into the {@link RelationshipStore}. No linking between relationships is done in this method,
     * it's done later in {@link #linkRelationships(int)}.
     *
     * @throws IOException on I/O error.
     */
    public void importRelationships() throws IOException
    {
        // Import relationships (unlinked), properties
        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        RelationshipStage unlinkedRelationshipStage =
                new RelationshipStage( relationshipConfig, writeMonitor, relationships, idMapper,
                        badCollector, inputCache, neoStore, storeUpdateMonitor );
        neoStore.startFlushingPageCache();
        executeStage( unlinkedRelationshipStage );
        neoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
        idMapper.close();
        idMapper = null;
        putState( unlinkedRelationshipStage.getDistribution() );
    }

    /**
     * Populates {@link NodeRelationshipCache} with node degrees, which is required to know how to physically layout each
     * relationship chain. This is required before running {@link #linkRelationships(int)}.
     */
    public void calculateNodeDegrees()
    {
        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        nodeRelationshipCache.setHighNodeId( neoStore.getNodeStore().getHighId() );
        NodeDegreeCountStage nodeDegreeStage = new NodeDegreeCountStage( relationshipConfig,
                neoStore.getRelationshipStore(), nodeRelationshipCache );
        executeStage( nodeDegreeStage );
        nodeRelationshipCache.countingCompleted();
        availableMemoryForLinking = maxMemory - totalMemoryUsageOf( nodeRelationshipCache, neoStore );
    }

    /**
     * Performs one round of linking together relationships with each other. Number of rounds required
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
     * A linking loop (from external caller POV) typically looks like:
     * <pre>
     * int type = 0;
     * do
     * {
     *    type = logic.linkRelationships( type );
     * }
     * while ( type != -1 );
     * </pre>
     *
     * @param startingFromType relationship type to start from.
     * @return the next relationship type to start linking and, if != -1, should be passed into next call to this method.
     */
    public int linkRelationships( int startingFromType )
    {
        assert startingFromType >= 0 : startingFromType;

        // Link relationships together with each other, their nodes and their relationship groups
        RelationshipTypeDistribution relationshipTypeDistribution = getState( RelationshipTypeDistribution.class );

        // Figure out which types we can fit in node-->relationship cache memory.
        // Types go from biggest to smallest group and so towards the end there will be
        // smaller and more groups per round in this loop
        int upToType = nextSetOfTypesThatFitInMemory(
                relationshipTypeDistribution, startingFromType, availableMemoryForLinking, nodeRelationshipCache.getNumberOfDenseNodes() );

        Collection<Object> typesToLinkThisRound = relationshipTypeDistribution.types( startingFromType, upToType );
        int typesImported = typesToLinkThisRound.size();
        boolean thisIsTheFirstRound = startingFromType == 0;
        boolean thisIsTheOnlyRound = thisIsTheFirstRound && upToType == relationshipTypeDistribution.getNumberOfRelationshipTypes();

        Configuration relationshipConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
        Configuration groupConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipGroupStore() );

        nodeRelationshipCache.setForwardScan( true, true/*dense*/ );
        String range = typesToLinkThisRound.size() == 1
                ? String.valueOf( oneBased( startingFromType ) )
                : oneBased( startingFromType ) + "-" + (startingFromType + typesImported);
        String topic = " " + range + "/" + relationshipTypeDistribution.getNumberOfRelationshipTypes();
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

        updatePeakMemoryUsage();

        if ( upToType == relationshipTypeDistribution.getNumberOfRelationshipTypes() )
        {
            // This means that we've linked all the types
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;
            return -1;
        }

        return upToType;
    }

    /**
     * Convenience method (for code reading) to have a zero-based value become one based (for printing/logging).
     */
    private static int oneBased( int value )
    {
        return value + 1;
    }

    /**
     * @return index (into {@link RelationshipTypeDistribution}) of last relationship type that fit in memory this round.
     */
    static int nextSetOfTypesThatFitInMemory( RelationshipTypeDistribution typeDistribution, int startingFromType,
            long freeMemoryForDenseNodeCache, long numberOfDenseNodes )
    {
        assert startingFromType >= 0 : startingFromType;

        long currentSetOfRelationshipsMemoryUsage = 0;
        int numberOfTypes = typeDistribution.getNumberOfRelationshipTypes();
        int toType = startingFromType;
        for ( ; toType < numberOfTypes; toType++ )
        {
            // Calculate worst-case scenario
            Pair<Object,Long> type = typeDistribution.get( toType );
            long relationshipCountForThisType = type.other();
            long memoryUsageForThisType = calculateMaxMemoryUsage( numberOfDenseNodes, relationshipCountForThisType );
            long memoryUsageUpToAndIncludingThisType =
                    currentSetOfRelationshipsMemoryUsage + memoryUsageForThisType;
            if ( memoryUsageUpToAndIncludingThisType > freeMemoryForDenseNodeCache &&
                    currentSetOfRelationshipsMemoryUsage > 0 )
            {
                // OK the current set of types is enough to fill the cache
                break;
            }

            currentSetOfRelationshipsMemoryUsage += memoryUsageForThisType;
        }
        return toType;
    }

    /**
     * Optimizes the relationship groups store by physically locating groups for each node together.
     */
    public void defragmentRelationshipGroups()
    {
        // Defragment relationships groups for better performance
        new RelationshipGroupDefragmenter( config, executionMonitor, numberArrayFactory )
                .run( max( maxMemory, peakMemoryUsage ), neoStore, neoStore.getNodeStore().getHighId() );
    }

    /**
     * Builds the counts store. Requires that {@link #importNodes()} and {@link #importRelationships()} has run.
     */
    public void buildCountsStore()
    {
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
            executeStage( new RelationshipCountsStage( config, nodeLabelsCache, neoStore.getRelationshipStore(),
                    neoStore.getLabelRepository().getHighId(),
                    neoStore.getRelationshipTypeRepository().getHighId(),
                    countsUpdater, numberArrayFactory, progressMonitor.startSection( "Relationships" ) ) );
        }
    }

    @Override
    public void close() throws IOException
    {
        // We're done, do some final logging about it
        long totalTimeMillis = currentTimeMillis() - startTime;
        executionMonitor.done( totalTimeMillis, format( "%n%s%nPeak memory usage: %s", storeUpdateMonitor, bytes( peakMemoryUsage ) ) );
        log.info( "Import completed successfully, took " + duration( totalTimeMillis ) + ". " + storeUpdateMonitor );

        if ( nodeRelationshipCache != null )
        {
            nodeRelationshipCache.close();
        }
        if ( nodeLabelsCache != null )
        {
            nodeLabelsCache.close();
        }
        if ( idMapper != null )
        {
            idMapper.close();
        }
        inputCache.close();
        // TODO close badCollector here instead of in import tool?
    }

    private void updatePeakMemoryUsage()
    {
        peakMemoryUsage = max( peakMemoryUsage, totalMemoryUsageOf( nodeRelationshipCache, idMapper, neoStore ) );
    }

    public static BatchingNeoStores instantiateNeoStores( FileSystemAbstraction fileSystem, File storeDir,
            PageCache externalPageCache, RecordFormats recordFormats, Configuration config,
            LogService logService, AdditionalInitialIds additionalInitialIds, Config dbConfig )
    {
        if ( externalPageCache == null )
        {
            return BatchingNeoStores.batchingNeoStores( fileSystem, storeDir, recordFormats, config, logService,
                    additionalInitialIds, dbConfig );
        }

        return BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, externalPageCache,
                PageCacheTracer.NULL, storeDir, recordFormats, config, logService, additionalInitialIds, dbConfig );
    }

    private static long totalMemoryUsageOf( MemoryStatsVisitor.Visitable... users )
    {
        GatheringMemoryStatsVisitor total = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable user : users )
        {
            if ( user != null )
            {
                user.acceptMemoryStatsVisitor( total );
            }
        }
        return total.getHeapUsage() + total.getOffHeapUsage();
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
