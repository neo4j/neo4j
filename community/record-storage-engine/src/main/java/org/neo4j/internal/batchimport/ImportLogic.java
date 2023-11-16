/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import static java.lang.Long.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.auto;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.IOUtils.closeAll;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.NodeType;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.PageCacheArrayFactoryMonitor;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.EstimationSanityChecker;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionSupervisors;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;

/**
 * Contains all algorithms and logic for doing an import. It exposes all stages as methods so that
 * it's possible to implement a {@link BatchImporter} which calls those.
 * This class has state which typically gets modified in each invocation of an import method.
 *
 * To begin with the methods are fairly coarse-grained, but can and probably will be split up into smaller parts
 * to allow external implementors have greater control over the flow.
 */
public class ImportLogic implements Closeable {
    private static final String ID_MAPPER_PREPARATION_TAG = "Id mapper preparation.";
    public static final Supplier<SchemaMonitor> NO_SCHEMA_MONITORING = () -> SchemaMonitor.NO_MONITOR;
    private static final RelationshipLinkingMonitor NO_LINKING_MONITOR = new RelationshipLinkingMonitor() {};

    private final Path databaseDirectory;
    private final String databaseName;
    protected final BatchingNeoStores neoStore;
    protected final Configuration config;
    private final Config dbConfig;
    private final InternalLog log;
    protected final CursorContextFactory contextFactory;
    private final IndexImporterFactory indexImporterFactory;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final ExecutionMonitor executionMonitor;
    private final RecordFormats recordFormats;
    private final DataImporter.Monitor storeUpdateMonitor = new DataImporter.Monitor();
    private final long maxMemory;
    private final Dependencies dependencies = new Dependencies();
    private final Monitor monitor;
    private Input input;
    private boolean successful;

    // This map contains additional state that gets populated, created and used throughout the stages.
    // The reason that this is a map is to allow for a uniform way of accessing and loading this stage
    // from the outside. Currently these things live here:
    //   - RelationshipTypeDistribution
    private final Map<Class<?>, Object> accessibleState = new HashMap<>();

    // components which may get assigned and unassigned in some methods
    protected NodeRelationshipCache nodeRelationshipCache;
    private NodeLabelsCache nodeLabelsCache;
    private long startTime;
    private NumberArrayFactory numberArrayFactory;
    private final Collector badCollector;
    private IdMapper idMapper;
    private long peakMemoryUsage;
    private long availableMemoryForLinking;

    /**
     * @param databaseLayout directory which the db will be created in.
     * @param neoStore {@link BatchingNeoStores} to import into.
     * @param config import-specific {@link Configuration}.
     * @param logService {@link LogService} to use.
     * @param executionMonitor {@link ExecutionMonitor} to follow progress as the import proceeds.
     * @param badCollector {@link Collector} for bad entries.
     * @param monitor {@link Monitor} for some events.
     */
    public ImportLogic(
            DatabaseLayout databaseLayout,
            BatchingNeoStores neoStore,
            Configuration config,
            Config dbConfig,
            LogService logService,
            ExecutionMonitor executionMonitor,
            Collector badCollector,
            Monitor monitor,
            CursorContextFactory contextFactory,
            IndexImporterFactory indexImporterFactory,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker) {
        this.databaseDirectory = databaseLayout.databaseDirectory();
        this.databaseName = databaseLayout.getDatabaseName();
        this.neoStore = neoStore;
        this.config = config;
        this.dbConfig = dbConfig;
        this.recordFormats = neoStore.recordFormats();
        this.badCollector = badCollector;
        this.monitor = monitor;
        this.log = logService.getInternalLogProvider().getLog(getClass());
        this.contextFactory = contextFactory;
        this.indexImporterFactory = indexImporterFactory;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
        this.executionMonitor = ExecutionSupervisors.withDynamicProcessorAssignment(executionMonitor, config);
        this.maxMemory = config.maxOffHeapMemory();
    }

    public void initialize(Input input) throws IOException {
        log.info("Import starting");
        startTime = currentTimeMillis();
        this.input = input;
        PageCacheArrayFactoryMonitor numberArrayFactoryMonitor = new PageCacheArrayFactoryMonitor();
        numberArrayFactory = auto(
                neoStore.getPageCache(),
                contextFactory,
                databaseDirectory,
                false,
                numberArrayFactoryMonitor,
                log,
                databaseName);
        // Some temporary caches and indexes in the import
        idMapper = instantiateIdMapper(input);
        nodeRelationshipCache = new NodeRelationshipCache(
                numberArrayFactory, dbConfig.get(GraphDatabaseSettings.dense_node_threshold), memoryTracker);
        Input.Estimates inputEstimates =
                input.calculateEstimates(neoStore.getPropertyStore().newValueEncodedSizeCalculator());

        // Sanity checking against estimates
        new EstimationSanityChecker(recordFormats, monitor).sanityCheck(inputEstimates);
        new HeapSizeSanityChecker(monitor)
                .sanityCheck(
                        inputEstimates,
                        recordFormats,
                        neoStore,
                        NodeRelationshipCache.memoryEstimation(inputEstimates.numberOfNodes()),
                        idMapper.memoryEstimation(inputEstimates.numberOfNodes()));

        dependencies.satisfyDependencies(
                inputEstimates, idMapper, neoStore, nodeRelationshipCache, numberArrayFactoryMonitor);

        if (neoStore.determineDoubleRelationshipRecordUnits(inputEstimates)) {
            monitor.doubleRelationshipRecordUnitsEnabled();
        }

        executionMonitor.initialize(dependencies);
    }

    protected IdMapper instantiateIdMapper(Input input) {
        return switch (input.idType()) {
            case STRING -> IdMappers.strings(
                    numberArrayFactory, input.groups(), config.strictNodeCheck(), memoryTracker);
            case INTEGER -> IdMappers.longs(numberArrayFactory, input.groups(), memoryTracker);
            case ACTUAL -> IdMappers.actual();
        };
    }

    /**
     * Accesses state of a certain {@code type}. This is state that may be long- or short-lived and perhaps
     * created in one part of the import to be used in another.
     *
     * @param type {@link Class} of the state to get.
     * @return the state of the given type.
     * @throws IllegalStateException if the state of the given {@code type} isn't available.
     */
    public <T> T getState(Class<T> type) {
        return type.cast(accessibleState.get(type));
    }

    /**
     * Puts state of a certain type.
     *
     * @param state state instance to set.
     * @see #getState(Class)
     * @throws IllegalStateException if state of this type has already been defined.
     */
    public <T> void putState(T state) {
        accessibleState.put(state.getClass(), state);
        dependencies.satisfyDependency(state);
    }

    public void importNodes() throws IOException {
        importNodes(NO_SCHEMA_MONITORING);
    }

    /**
     * Imports nodes w/ their properties and labels from {@link Input#nodes(Collector)}. This will as a side-effect populate the {@link IdMapper},
     * to later be used for looking up ID --> nodeId in {@link #importRelationships()}. After a completed node import,
     * {@link #prepareIdMapper()} must be called.
     *
     * @throws IOException on I/O error.
     */
    public void importNodes(Supplier<SchemaMonitor> schemaMonitors) throws IOException {
        // Import nodes, properties, labels
        neoStore.startFlushingPageCache();
        DataImporter.importNodes(
                config,
                input,
                neoStore,
                idMapper,
                badCollector,
                executionMonitor,
                storeUpdateMonitor,
                contextFactory,
                memoryTracker,
                schemaMonitors);
        neoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
    }

    /**
     * Prepares {@link IdMapper} to be queried for ID --> nodeId lookups. This is required for running {@link #importRelationships()}.
     */
    public void prepareIdMapper() {
        if (idMapper.needsPreparation()) {
            MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider(neoStore, idMapper);
            try (var cursorContext = contextFactory.create(ID_MAPPER_PREPARATION_TAG)) {
                var inputIdLookup = new NodeInputIdPropertyLookup(
                        neoStore.getTemporaryPropertyStore(),
                        () -> new CachedStoreCursors(neoStore.getTemporaryNeoStores(), cursorContext));
                executeStage(
                        new IdMapperPreparationStage(config, idMapper, inputIdLookup, badCollector, memoryUsageStats));
            }
            final LongIterator duplicateNodeIds = idMapper.leftOverDuplicateNodesIds();
            if (duplicateNodeIds.hasNext()) {
                executeStage(new DeleteDuplicateNodesStage(
                        config, duplicateNodeIds, neoStore.getNeoStores(), storeUpdateMonitor, contextFactory));
            }
            updatePeakMemoryUsage();
        }
    }

    public void importRelationships() throws IOException {
        importRelationships(NO_SCHEMA_MONITORING);
    }

    public void removeViolatingRelationships(LongSet violatingRelationships) throws IOException {
        if (violatingRelationships.notEmpty()) {
            // Make sure to keep the typeDistribution up to date
            DataStatistics state = getState(DataStatistics.class);
            try (DataStatistics.Client client = state.newClient()) {
                executeStage(new DeleteViolatingRelationshipsStage(
                        config,
                        violatingRelationships.longIterator(),
                        neoStore.getNeoStores(),
                        storeUpdateMonitor,
                        client,
                        contextFactory));
            }
        }
    }

    /**
     * Uses {@link IdMapper} as lookup for ID --> nodeId and imports all relationships from {@link Input#relationships(Collector)}
     * and writes them into the {@link RelationshipStore}. No linking between relationships is done in this method,
     * it's done later in {@link #linkRelationships(int,RelationshipLinkingMonitor)}.
     *
     * @throws IOException on I/O error.
     */
    public void importRelationships(Supplier<SchemaMonitor> schemaMonitors) throws IOException {
        // Import relationships (unlinked), properties
        neoStore.startFlushingPageCache();
        DataStatistics typeDistribution = DataImporter.importRelationships(
                config,
                input,
                neoStore,
                idMapper,
                config.strictNodeCheck(),
                badCollector,
                executionMonitor,
                storeUpdateMonitor,
                !badCollector.isCollectingBadRelationships(),
                contextFactory,
                memoryTracker,
                schemaMonitors);
        neoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
        idMapper.close();
        idMapper = null;
        putState(typeDistribution);
    }

    /**
     * Populates {@link NodeRelationshipCache} with node degrees, which is required to know how to physically layout each
     * relationship chain. This is required before running {@link #linkRelationships(int,RelationshipLinkingMonitor)}.
     */
    public void calculateNodeDegrees() {
        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize(config, neoStore.getRelationshipStore());
        var nodeStore = neoStore.getNodeStore();
        nodeRelationshipCache.setNodeCount(nodeStore.getIdGenerator().getHighId());
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider(neoStore, nodeRelationshipCache);
        NodeDegreeCountStage nodeDegreeStage = new NodeDegreeCountStage(
                relationshipConfig,
                neoStore.getRelationshipStore(),
                nodeRelationshipCache,
                memoryUsageStats,
                contextFactory);
        executeStage(nodeDegreeStage);
        nodeRelationshipCache.countingCompleted();
        availableMemoryForLinking = maxMemory - totalMemoryUsageOf(nodeRelationshipCache, neoStore);
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
    public int linkRelationships(int startingFromType, RelationshipLinkingMonitor linkingMonitor) throws IOException {
        assert startingFromType >= 0 : startingFromType;

        // Link relationships together with each other, their nodes and their relationship groups
        DataStatistics relationshipTypeDistribution = getState(DataStatistics.class);
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider(neoStore, nodeRelationshipCache);

        // Figure out which types we can fit in node-->relationship cache memory.
        // Types go from biggest to smallest group and so towards the end there will be
        // smaller and more groups per round in this loop
        int upToType = nextSetOfTypesThatFitInMemory(
                relationshipTypeDistribution,
                startingFromType,
                availableMemoryForLinking,
                nodeRelationshipCache.getNumberOfDenseNodes());

        final IntSet typesToLinkThisRound = relationshipTypeDistribution.types(startingFromType, upToType);
        int typesImported = typesToLinkThisRound.size();
        boolean thisIsTheFirstRound = startingFromType == 0;
        boolean thisIsTheLastRound = upToType == relationshipTypeDistribution.getNumberOfRelationshipTypes();
        boolean thisIsTheOnlyRound = thisIsTheFirstRound && thisIsTheLastRound;

        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize(config, neoStore.getRelationshipStore());
        Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize(config, neoStore.getNodeStore());
        Configuration groupConfig =
                configWithRecordsPerPageBasedBatchSize(config, neoStore.getRelationshipGroupStore());

        nodeRelationshipCache.setForwardScan(true, true /*dense*/);
        String range = typesToLinkThisRound.size() == 1
                ? String.valueOf(oneBased(startingFromType))
                : oneBased(startingFromType) + "-" + (startingFromType + typesImported);
        String topic = " " + range + "/" + relationshipTypeDistribution.getNumberOfRelationshipTypes();
        int nodeTypes = thisIsTheFirstRound ? NodeType.NODE_TYPE_ALL : NodeType.NODE_TYPE_DENSE;
        Predicate<RelationshipRecord> readFilter = thisIsTheFirstRound
                ? alwaysTrue() // optimization when all rels are imported in this round
                : record -> typesToLinkThisRound.contains(record.getType());
        Predicate<RelationshipRecord> denseChangeFilter = thisIsTheOnlyRound
                ? alwaysTrue() // optimization when all rels are imported in this round
                : record -> typesToLinkThisRound.contains(record.getType());

        // LINK Forward
        Function<CursorContext, StoreCursors> neoStoreCursorCreator =
                cursorContext -> new CachedStoreCursors(neoStore.getNeoStores(), cursorContext);
        RelationshipLinkforwardStage linkForwardStage = new RelationshipLinkforwardStage(
                topic,
                relationshipConfig,
                neoStore,
                nodeRelationshipCache,
                readFilter,
                neoStoreCursorCreator,
                denseChangeFilter,
                nodeTypes,
                contextFactory,
                new RelationshipLinkingProgress(),
                memoryUsageStats);
        executeStage(linkForwardStage);

        // Write relationship groups cached from the relationship import above
        executeStage(new RelationshipGroupStage(
                topic,
                groupConfig,
                neoStore.getTemporaryRelationshipGroupStore(),
                nodeRelationshipCache,
                contextFactory,
                cursorContext -> new CachedStoreCursors(neoStore.getTemporaryNeoStores(), cursorContext)));
        if (thisIsTheFirstRound) {
            // Set node nextRel fields for sparse nodes
            executeStage(new SparseNodeFirstRelationshipStage(
                    nodeConfig,
                    neoStore.getNodeStore(),
                    nodeRelationshipCache,
                    contextFactory,
                    cursorContext -> new CachedStoreCursors(neoStore.getNeoStores(), cursorContext)));
        }
        linkingMonitor.forwardLinkingCompleted(startingFromType, upToType, thisIsTheFirstRound, thisIsTheLastRound);

        // LINK backward
        nodeRelationshipCache.setForwardScan(false, true /*dense*/);
        executeStage(new RelationshipLinkbackStage(
                topic,
                relationshipConfig,
                neoStore,
                nodeRelationshipCache,
                readFilter,
                neoStoreCursorCreator,
                denseChangeFilter,
                nodeTypes,
                contextFactory,
                new RelationshipLinkingProgress(),
                memoryUsageStats));
        linkingMonitor.backwardLinkingCompleted(startingFromType, upToType, thisIsTheFirstRound, thisIsTheLastRound);

        updatePeakMemoryUsage();

        if (upToType == relationshipTypeDistribution.getNumberOfRelationshipTypes()) {
            // This means that we've linked all the types
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;
            return -1;
        }

        return upToType;
    }

    public void linkRelationshipsOfAllTypes() throws IOException {
        linkRelationshipsOfAllTypes(NO_LINKING_MONITOR);
    }

    /**
     * Links relationships of all types, potentially doing multiple passes, each pass calling {@link #linkRelationships(int,RelationshipLinkingMonitor)}
     * with a type range.
     */
    public void linkRelationshipsOfAllTypes(RelationshipLinkingMonitor linkingMonitor) throws IOException {
        int type = 0;
        do {
            type = linkRelationships(type, linkingMonitor);
        } while (type != -1);
    }

    /**
     * Convenience method (for code reading) to have a zero-based value become one based (for printing/logging).
     */
    private static int oneBased(int value) {
        return value + 1;
    }

    /**
     * @return index (into {@link DataStatistics}) of last relationship type that fit in memory this round.
     */
    static int nextSetOfTypesThatFitInMemory(
            DataStatistics typeDistribution,
            int startingFromType,
            long freeMemoryForDenseNodeCache,
            long numberOfDenseNodes) {
        assert startingFromType >= 0 : startingFromType;

        long currentSetOfRelationshipsMemoryUsage = 0;
        int numberOfTypes = typeDistribution.getNumberOfRelationshipTypes();
        int toType = startingFromType;
        for (; toType < numberOfTypes; toType++) {
            // Calculate worst-case scenario
            DataStatistics.RelationshipTypeCount type = typeDistribution.get(toType);
            long relationshipCountForThisType = type.getCount();
            long memoryUsageForThisType =
                    NodeRelationshipCache.calculateMaxMemoryUsage(numberOfDenseNodes, relationshipCountForThisType);
            long memoryUsageUpToAndIncludingThisType = currentSetOfRelationshipsMemoryUsage + memoryUsageForThisType;
            if (memoryUsageUpToAndIncludingThisType > freeMemoryForDenseNodeCache
                    && currentSetOfRelationshipsMemoryUsage > 0) {
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
    public void defragmentRelationshipGroups() {
        // Defragment relationships groups for better performance
        var nodeStore = neoStore.getNodeStore();
        new RelationshipGroupDefragmenter(
                        config,
                        executionMonitor,
                        RelationshipGroupDefragmenter.Monitor.EMPTY,
                        numberArrayFactory,
                        contextFactory,
                        memoryTracker)
                .run(
                        max(maxMemory, peakMemoryUsage),
                        neoStore,
                        nodeStore.getIdGenerator().getHighId());
    }

    public void buildAuxiliaryStores() {
        buildAuxiliaryStores(0);
    }

    /**
     * Builds the counts store and lookup indexes. Requires that {@link #importNodes()} and {@link #importRelationships()} has run.
     */
    public void buildAuxiliaryStores(long fromNodeId) {
        neoStore.buildCountsStore(
                new CountsBuilder() {
                    @Override
                    public void initialize(
                            CountsUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                        MigrationProgressMonitor progressMonitor = MigrationProgressMonitor.SILENT;
                        var labelTokenStore = neoStore.getNeoStores().getLabelTokenStore();
                        int highLabelId =
                                toIntExact(labelTokenStore.getIdGenerator().getHighId());
                        var relTypeTokenStore = neoStore.getNeoStores().getRelationshipTypeTokenStore();
                        int highRelationshipTypeId =
                                toIntExact(relTypeTokenStore.getIdGenerator().getHighId());
                        var nodeStore = neoStore.getNodeStore();
                        nodeLabelsCache = new NodeLabelsCache(
                                numberArrayFactory, nodeStore.getIdGenerator().getHighId(), highLabelId, memoryTracker);
                        MemoryUsageStatsProvider memoryUsageStats =
                                new MemoryUsageStatsProvider(neoStore, nodeLabelsCache);
                        Function<CursorContext, StoreCursors> storeCursorsFactory =
                                context -> new CachedStoreCursors(neoStore.getNeoStores(), context);
                        try (var progress = progressMonitor.startSection("Nodes")) {
                            executeStage(new NodeCountsAndLabelIndexBuildStage(
                                    config,
                                    neoStore,
                                    nodeLabelsCache,
                                    neoStore.getNodeStore(),
                                    highLabelId,
                                    updater,
                                    progress,
                                    indexImporterFactory,
                                    fromNodeId,
                                    contextFactory,
                                    pageCacheTracer,
                                    storeCursorsFactory,
                                    memoryTracker,
                                    memoryUsageStats));
                        }
                        try (var progress = progressMonitor.startSection("Relationships")) {
                            // Count label-[type]->label
                            executeStage(new RelationshipCountsAndTypeIndexBuildStage(
                                    config,
                                    neoStore,
                                    nodeLabelsCache,
                                    neoStore.getRelationshipStore(),
                                    highLabelId,
                                    highRelationshipTypeId,
                                    updater,
                                    numberArrayFactory,
                                    progress,
                                    indexImporterFactory,
                                    contextFactory,
                                    pageCacheTracer,
                                    storeCursorsFactory,
                                    memoryTracker));
                        }
                    }

                    @Override
                    public long lastCommittedTxId() {
                        return neoStore.getLastCommittedTransactionId();
                    }
                },
                contextFactory,
                memoryTracker);
    }

    public void success() {
        neoStore.success();
        successful = true;
    }

    @Override
    public void close() throws IOException {
        // We're done, do some final logging about it
        long totalTimeMillis = startTime > 0 ? currentTimeMillis() - startTime : 0;
        DataStatistics state = getState(DataStatistics.class);
        String additionalInformation = Objects.toString(state, "Data statistics is not available.");
        executionMonitor.done(
                successful,
                totalTimeMillis,
                format("%n%s%nPeak memory usage: %s", additionalInformation, bytesToString(peakMemoryUsage)));
        log.info("Import " + (successful ? "completed successfully" : "failed") + ", took " + duration(totalTimeMillis)
                + ". " + additionalInformation);
        closeAll(nodeRelationshipCache, nodeLabelsCache, idMapper);
    }

    private void updatePeakMemoryUsage() {
        peakMemoryUsage = max(peakMemoryUsage, totalMemoryUsageOf(nodeRelationshipCache, idMapper, neoStore));
    }

    public static BatchingNeoStores instantiateNeoStores(
            FileSystemAbstraction fileSystem,
            RecordDatabaseLayout databaseLayout,
            PageCacheTracer cacheTracer,
            Configuration config,
            LogService logService,
            AdditionalInitialIds additionalInitialIds,
            LogTailLogVersionsMetadata logTailMetadata,
            Config dbConfig,
            JobScheduler scheduler,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory) {
        return BatchingNeoStores.batchingNeoStores(
                fileSystem,
                databaseLayout,
                config,
                logService,
                additionalInitialIds,
                logTailMetadata,
                dbConfig,
                scheduler,
                cacheTracer,
                contextFactory,
                memoryTracker);
    }

    private static long totalMemoryUsageOf(MemoryStatsVisitor.Visitable... users) {
        GatheringMemoryStatsVisitor total = new GatheringMemoryStatsVisitor();
        for (MemoryStatsVisitor.Visitable user : users) {
            if (user != null) {
                user.acceptMemoryStatsVisitor(total);
            }
        }
        return total.getHeapUsage() + total.getOffHeapUsage();
    }

    private static Configuration configWithRecordsPerPageBasedBatchSize(Configuration source, RecordStore<?> store) {
        return new Configuration.Overridden(source) {
            @Override
            public int batchSize() {
                // 500 pages, i.e. 4 MiB worth of data per batch. This makes reading performance more consistent and
                // sequential,
                // and helps _a lot_ for backwards scanning where reading each batch now will jump back much further and
                // sequentially
                // read forwards.
                return store.getRecordsPerPage() * 500;
            }

            @Override
            public int maxQueueSize() {
                // Limit the queue size now that each batch is quite large, otherwise heap will fill up
                return 10;
            }
        };
    }

    private void executeStage(Stage stage) {
        ExecutionSupervisors.superviseExecution(executionMonitor, stage);
    }

    public interface RelationshipLinkingMonitor {
        default void forwardLinkingCompleted(int fromType, int toType, boolean firstRound, boolean lastRound)
                throws IOException {}

        default void backwardLinkingCompleted(int fromType, int toType, boolean firstRound, boolean lastRound)
                throws IOException {}
    }
}
