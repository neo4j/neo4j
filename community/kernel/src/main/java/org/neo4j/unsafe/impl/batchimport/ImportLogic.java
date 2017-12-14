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
import org.neo4j.kernel.impl.util.Dependencies;
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
import org.neo4j.unsafe.impl.batchimport.input.EstimationSanityChecker;
import org.neo4j.unsafe.impl.batchimport.input.EstimationSanityChecker.Monitor;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
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
    protected final CountingStoreUpdateMonitor storeUpdateMonitor = new CountingStoreUpdateMonitor();
    private final long maxMemory;
    private final Dependencies dependencies = new Dependencies();

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
    private InputIterable<InputNode> nodes;
    private InputIterable<InputRelationship> relationships;
    private InputIterable<InputNode> cachedNodes;
    private long peakMemoryUsage;
    private long availableMemoryForLinking;

    /**
     * @param storeDir directory which the db will be created in.
     * @param fileSystem {@link FileSystemAbstraction} that the {@code storeDir} lives in.
     * @param neoStore {@link BatchingNeoStores} to import into.
     * @param config import-specific {@link Configuration}.
     * @param logService {@link LogService} to use.
     * @param executionMonitor {@link ExecutionMonitor} to follow progress as the import proceeds.
     * @param recordFormats which {@link RecordFormats record format} to use for the created db.
     */
    public ImportLogic( File storeDir, FileSystemAbstraction fileSystem, BatchingNeoStores neoStore,
            Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            RecordFormats recordFormats )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.neoStore = neoStore;
        this.config = config;
        this.recordFormats = recordFormats;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.executionMonitor = ExecutionSupervisors.withDynamicProcessorAssignment( executionMonitor, config );
        this.maxMemory = config.maxMemoryUsage();
    }

    public void initialize( Input input ) throws IOException
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
        nodes = input.nodes();
        relationships = input.relationships();
        cachedNodes = cachedForSure( nodes, inputCache.nodes( MAIN, true ) );
        Estimates inputEstimates = input.calculateEstimates( neoStore.getPropertyStore().newValueEncodedSizeCalculator() );
        sanityCheckEstimatesWithRecordFormat( inputEstimates );
        dependencies.satisfyDependencies( inputEstimates, idMapper, neoStore, nodeRelationshipCache );

        if ( neoStore.determineDoubleRelationshipRecordUnits( inputEstimates ) )
        {
            System.out.println( "Will use double record units for all relationships" );
        }

        executionMonitor.initialize( dependencies );
    }

    private void sanityCheckEstimatesWithRecordFormat( Estimates inputEstimates )
    {
        new EstimationSanityChecker( recordFormats, new Monitor()
        {
            @Override
            public void nodeCountCapacity( long capacity, long estimatedCount )
            {
                System.err.printf( "WARNING: estimated number of relationships %d may exceed capacity %d of selected record format%n",
                        estimatedCount, capacity );
            }

            @Override
            public void relationshipCountCapacity( long capacity, long estimatedCount )
            {
                System.err.printf( "WARNING: estimated number of nodes %d may exceed capacity %d of selected record format%n",
                        estimatedCount, capacity );
            }
        } ).sanityCheck( inputEstimates );
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
        dependencies.satisfyDependency( state );
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
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, idMapper );
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
            MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, idMapper );
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
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, idMapper );
        RelationshipStage unlinkedRelationshipStage =
                new RelationshipStage( relationshipConfig, writeMonitor, relationships, idMapper,
                        badCollector, inputCache, neoStore, storeUpdateMonitor, memoryUsageStats );
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
        nodeRelationshipCache.setNodeCount( neoStore.getNodeStore().getHighId() );
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, nodeRelationshipCache );
        NodeDegreeCountStage nodeDegreeStage = new NodeDegreeCountStage( relationshipConfig,
                neoStore.getRelationshipStore(), nodeRelationshipCache, memoryUsageStats );
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
        DataStatistics relationshipTypeDistribution = getState( DataStatistics.class );
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, nodeRelationshipCache );

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
                neoStore, nodeRelationshipCache, readFilter, denseChangeFilter, nodeTypes,
                new RelationshipLinkingProgress(), memoryUsageStats );
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
        executeStage( new RelationshipLinkbackStage( topic, relationshipConfig, neoStore,
                nodeRelationshipCache, readFilter, denseChangeFilter, nodeTypes,
                new RelationshipLinkingProgress(), memoryUsageStats ) );

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
     * Links relationships of all types, potentially doing multiple passes, each pass calling {@link #linkRelationships(int)}
     * with a type range.
     */
    public void linkRelationshipsOfAllTypes()
    {
        int type = 0;
        do
        {
            type = linkRelationships( type );
        }
        while ( type != -1 );
    }

    /**
     * Convenience method (for code reading) to have a zero-based value become one based (for printing/logging).
     */
    private static int oneBased( int value )
    {
        return value + 1;
    }

    /**
     * @return index (into {@link DataStatistics}) of last relationship type that fit in memory this round.
     */
    static int nextSetOfTypesThatFitInMemory( DataStatistics typeDistribution, int startingFromType,
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
        new RelationshipGroupDefragmenter( config, executionMonitor, RelationshipGroupDefragmenter.Monitor.EMPTY, numberArrayFactory )
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
            MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, nodeLabelsCache );
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
        DataStatistics stats = getState( DataStatistics.class );
        executionMonitor.done( totalTimeMillis, format( "%n%s%nPeak memory usage: %s", stats, bytes( peakMemoryUsage ) ) );
        log.info( "Import completed successfully, took " + duration( totalTimeMillis ) + ". " + stats );

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
