/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.checking.index.IndexAccessors.IndexAccessorLookup;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.counts.GBPTreeGenericCountsStore;
import org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.consistency_checker_fail_fast_threshold;
import static org.neo4j.consistency.checker.ParallelExecution.DEFAULT_IDS_PER_CHUNK;
import static org.neo4j.consistency.checker.SchemaChecker.moreDescriptiveRecordToStrings;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;

/**
 * A consistency checker for a {@link RecordStorageEngine}, focused on keeping abstractions to a minimum and having clean and understandable
 * algorithms. Revolves around the {@link CacheAccess} which uses up {@code 11 * NodeStore#getHighId()}. The checking is split up into a couple
 * of checkers, mostly focused on a single store, e.g. nodes or relationships, which are run one after the other. Each checker's algorithm is
 * designed to try to maximize both CPU and I/O to its full extent, or rather at least maxing out one of them.
 */
public class RecordStorageConsistencyChecker implements AutoCloseable
{
    private static final String INDEX_STRUCTURE_CHECKER_TAG = "indexStructureChecker";
    private static final String COUNT_STORE_CONSISTENCY_CHECKER_TAG = "countStoreConsistencyChecker";
    private static final String SCHEMA_CONSISTENCY_CHECKER_TAG = "schemaConsistencyChecker";
    private static final String CONSISTENCY_CHECKER_TOKEN_LOADER_TAG = "consistencyCheckerTokenLoader";
    static final int[] DEFAULT_SLOT_SIZES = {CacheSlots.ID_SLOT_SIZE, CacheSlots.ID_SLOT_SIZE, 1, 1, 1, 1, 1, 1 /*2 bits unused*/};

    private final FileSystemAbstraction fileSystem;
    private final RecordDatabaseLayout databaseLayout;
    private final PageCache pageCache;
    private final NeoStores neoStores;
    private final IdGeneratorFactory idGeneratorFactory;
    private final ConsistencySummaryStatistics summary;
    private final ProgressMonitorFactory progressFactory;
    private final Log log;
    private final ConsistencyFlags consistencyFlags;
    private final PageCacheTracer cacheTracer;
    private final CacheAccess cacheAccess;
    private final ConsistencyReporter reporter;
    private final CountsState observedCounts;
    private final EntityBasedMemoryLimiter limiter;
    private final CheckerContext context;
    private final ProgressMonitorFactory.MultiPartBuilder progress;
    private final IndexAccessors indexAccessors;
    private final InconsistencyReport report;

    public RecordStorageConsistencyChecker( FileSystemAbstraction fileSystem, RecordDatabaseLayout databaseLayout, PageCache pageCache, NeoStores neoStores,
            IndexProviderMap indexProviders, IndexAccessorLookup accessorLookup, IdGeneratorFactory idGeneratorFactory, ConsistencySummaryStatistics summary,
            ProgressMonitorFactory progressFactory, Config config, int numberOfThreads, Log log, boolean verbose, ConsistencyFlags consistencyFlags,
            EntityBasedMemoryLimiter.Factory memoryLimit, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.neoStores = neoStores;
        this.idGeneratorFactory = idGeneratorFactory;
        this.summary = summary;
        this.progressFactory = progressFactory;
        this.log = log;
        this.consistencyFlags = consistencyFlags;
        this.cacheTracer = cacheTracer;
        int stopCountThreshold = config.get( consistency_checker_fail_fast_threshold );
        AtomicInteger stopCount = new AtomicInteger( 0 );
        ConsistencyReporter.Monitor monitor = ConsistencyReporter.NO_MONITOR;
        if ( stopCountThreshold > 0 )
        {
            monitor = ( ignoredArg1, ignoredArg2, ignoredArg3 ) ->
            {
                if ( !isCancelled() && stopCount.incrementAndGet() >= stopCountThreshold )
                {
                    cancel( "Observed " + stopCount.get() + " inconsistencies." );
                }
            };
        }
        TokenHolders tokenHolders = safeLoadTokens( neoStores, cacheTracer );
        this.report = new InconsistencyReport( new InconsistencyMessageLogger( log, moreDescriptiveRecordToStrings( neoStores, tokenHolders ) ), summary );
        this.reporter = new ConsistencyReporter( report, monitor );
        ParallelExecution execution = new ParallelExecution( numberOfThreads,
                exception -> cancel( "Unexpected exception" ), // Exceptions should interrupt all threads to exit faster
                DEFAULT_IDS_PER_CHUNK );
        RecordLoading recordLoading = new RecordLoading( neoStores );
        this.limiter = instantiateMemoryLimiter( memoryLimit );
        this.cacheAccess = new DefaultCacheAccess( DefaultCacheAccess.defaultByteArray( limiter.rangeSize(), memoryTracker ), Counts.NONE, numberOfThreads );
        this.observedCounts = new CountsState( neoStores, cacheAccess, memoryTracker );
        this.progress = progressFactory.multipleParts( "Consistency check" );
        this.indexAccessors = instantiateIndexAccessors( neoStores, indexProviders, accessorLookup, tokenHolders, config );
        this.context = new CheckerContext( neoStores, indexAccessors,
                execution, reporter, cacheAccess, tokenHolders, recordLoading, observedCounts, limiter, progress, pageCache, cacheTracer, memoryTracker, log,
                verbose, consistencyFlags );
    }

    private IndexAccessors instantiateIndexAccessors( NeoStores neoStores, IndexProviderMap indexProviders, IndexAccessorLookup accessorLookup,
            TokenHolders tokenHolders, Config config )
    {
        try ( var storeCursors = new CachedStoreCursors( neoStores, CursorContext.NULL ) )
        {
            SchemaRuleAccess schemaRuleAccess =
                    SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(), tokenHolders, neoStores.getMetaDataStore() );
            return new IndexAccessors( indexProviders, () -> schemaRuleAccess.indexesGetAllIgnoreMalformed( storeCursors ), new IndexSamplingConfig( config ),
                    cacheTracer, tokenHolders );
        }
    }

    public void check() throws ConsistencyCheckIncompleteException
    {
        assert !context.isCancelled();
        try
        {
            consistencyCheckIndexes();

            context.initialize();
            // Starting by loading all tokens from store into the TokenHolders, loaded in a safe way of course
            // Check schema - constraints and indexes, that sort of thing
            // This is done before instantiating the other checker instances because the schema checker will also
            // populate maps regarding mandatory properties which the node/relationship checkers uses
            SchemaChecker schemaChecker = new SchemaChecker( context );
            MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties = new IntObjectHashMap<>();
            MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties = new IntObjectHashMap<>();
            try ( var cursorContext = new CursorContext( cacheTracer.createPageCursorTracer( SCHEMA_CONSISTENCY_CHECKER_TAG ) );
                  var storeCursors = new CachedStoreCursors( context.neoStores, cursorContext ) )
            {
                schemaChecker.check( mandatoryNodeProperties, mandatoryRelationshipProperties, cursorContext, storeCursors );
            }

            // Some pieces of check logic are extracted from this main class to reduce the size of this class. Instantiate those here first
            NodeChecker nodeChecker = new NodeChecker( context, mandatoryNodeProperties );
            NodeIndexChecker indexChecker = new NodeIndexChecker( context );
            RelationshipIndexChecker relationshipIndexChecker = new RelationshipIndexChecker( context );
            RelationshipChecker relationshipChecker = new RelationshipChecker( context, mandatoryRelationshipProperties );
            RelationshipGroupChecker relationshipGroupChecker = new RelationshipGroupChecker( context );
            RelationshipChainChecker relationshipChainChecker = new RelationshipChainChecker( context );
            ProgressMonitorFactory.Completer progressCompleter = progress.build();

            int numberOfRanges = limiter.numberOfRanges();
            for ( int i = 1; limiter.hasNext(); i++ )
            {
                if ( isCancelled() )
                {
                    break;
                }

                EntityBasedMemoryLimiter.CheckRange range = limiter.next();
                if ( numberOfRanges > 1 )
                {
                    context.debug( "=== Checking range %d/%d (%s) ===", i, numberOfRanges, range );
                }
                context.initializeRange();

                // Tell the cache that the pivot node id is the low end of this range. This will make all interactions with the cache
                // take that into consideration when working with offset arrays where the index is based on node ids.
                cacheAccess.setPivotId( range.from() );

                if ( range.applicableForRelationshipBasedChecks() )
                {
                    LongRange relationshipRange = range.getRelationshipRange();
                    context.runIfAllowed( relationshipIndexChecker, relationshipRange );
                    // We don't clear the cache here since it will be cleared before it is used again:
                    // either in NodeIndexChecker, explicitly a few rows down before NodeChecker, or in next range of RelationshipIndexChecker.
                }

                if ( range.applicableForNodeBasedChecks() )
                {
                    LongRange nodeRange = range.getNodeRange();
                    // Go into a node-centric mode where the nodes themselves are checked and somewhat cached off-heap.
                    // Then while we have the nodes loaded in cache do all other checking that has anything to do with nodes
                    // so that the "other" store can be checked sequentially and the random node lookups will be cheap
                    context.runIfAllowed( indexChecker, nodeRange );
                    cacheAccess.setCacheSlotSizesAndClear( DEFAULT_SLOT_SIZES );
                    context.runIfAllowed( nodeChecker, nodeRange );
                    context.runIfAllowed( relationshipGroupChecker, nodeRange );
                    context.runIfAllowed( relationshipChecker, nodeRange );
                    context.runIfAllowed( relationshipChainChecker, nodeRange );
                }
            }

            if ( !isCancelled() )
            {
                // All counts we've observed while doing other checking along the way we compare against the counts store here
                checkCounts();
                checkRelationshipGroupDegressStore();
            }
            progressCompleter.close();
        }
        catch ( Exception e )
        {
            cancel( "ConsistencyChecker failed unexpectedly" );
            throw new ConsistencyCheckIncompleteException( e );
        }
    }

    private void consistencyCheckIndexes()
    {
        if ( consistencyFlags.isCheckIndexStructure() )
        {
            try ( var cursorContext = new CursorContext( cacheTracer.createPageCursorTracer( INDEX_STRUCTURE_CHECKER_TAG ) ) )
            {
                List<IdGenerator> idGenerators = new ArrayList<>();
                idGeneratorFactory.visit( idGenerators::add );

                ProgressListener progressListener = progressFactory.singlePart( "Index structure consistency check",
                        indexAccessors.onlineRules().size() + ((indexAccessors.nodeLabelIndex() != null) ? 1 : 0) +
                                ((indexAccessors.relationshipTypeIndex() != null) ? 1 : 0) + idGenerators.size() );

                if ( indexAccessors.nodeLabelIndex() != null )
                {
                    consistencyCheckSingleCheckable( report, progressListener, indexAccessors.nodeLabelIndex(), RecordType.LABEL_SCAN_DOCUMENT, cursorContext );
                }
                if ( indexAccessors.relationshipTypeIndex() != null )
                {
                    consistencyCheckSingleCheckable( report, progressListener, indexAccessors.relationshipTypeIndex(),
                            RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, cursorContext );
                }

                List<IndexDescriptor> rulesToRemove = new ArrayList<>();
                for ( IndexDescriptor onlineRule : indexAccessors.onlineRules() )
                {
                    ConsistencyReporter.FormattingDocumentedHandler handler = ConsistencyReporter.formattingHandler( report, RecordType.INDEX );
                    ReporterFactory reporterFactory = new ReporterFactory( handler );
                    IndexAccessor accessor = indexAccessors.accessorFor( onlineRule );
                    if ( !accessor.consistencyCheck( reporterFactory, cursorContext ) )
                    {
                        rulesToRemove.add( onlineRule );
                    }
                    handler.updateSummary();
                    progressListener.add( 1 );
                }
                for ( IndexDescriptor toRemove : rulesToRemove )
                {
                    indexAccessors.remove( toRemove );
                }

                for ( IdGenerator idGenerator : idGenerators )
                {
                    consistencyCheckSingleCheckable( report, progressListener, idGenerator, RecordType.ID_STORE, cursorContext );
                }
            }
        }
    }

    private EntityBasedMemoryLimiter instantiateMemoryLimiter( EntityBasedMemoryLimiter.Factory memoryLimit )
    {
        // The checker makes use of a large memory array to hold data per node. For large stores there may not be enough memory
        // to hold all node data and in that case the checking will happen iteratively where one part of the node store is selected
        // and checked and all other stores related to any of those nodes too. When that part is done the next part of the node store
        // is selected until all the nodes, e.g. all the data have been checked.

        long pageCacheMemory = pageCache.maxCachedPages() * pageCache.pageSize();
        long nodeCount = neoStores.getNodeStore().getHighId();
        long relationshipCount = neoStores.getRelationshipStore().getHighId();
        return memoryLimit.create( pageCacheMemory, nodeCount, relationshipCount );
    }

    @Override
    public void close()
    {
        context.cancel();
        observedCounts.close();
        indexAccessors.close();
    }

    private void checkCounts()
    {
        try ( var countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fileSystem, RecoveryCleanupWorkCollector.ignore(),
                new CountsBuilder()
                {
                    @Override
                    public void initialize( CountsAccessor.Updater updater, CursorContext cursorContext, MemoryTracker memoryTracker )
                    {
                        throw new UnsupportedOperationException(
                                "Counts store needed rebuild, consistency checker will instead report broken or missing store" );
                    }

                    @Override
                    public long lastCommittedTxId()
                    {
                        return neoStores.getMetaDataStore().getLastCommittedTransactionId();
                    }
                }, readOnly(), cacheTracer, GBPTreeCountsStore.NO_MONITOR, databaseLayout.getDatabaseName(), 100, NullLogProvider.getInstance() );
                var checker = observedCounts.checker( reporter );
                var cursorContext = new CursorContext( cacheTracer.createPageCursorTracer( COUNT_STORE_CONSISTENCY_CHECKER_TAG ) ) )
        {
            if ( context.consistencyFlags.isCheckGraph() )
            {
                countsStore.accept( checker, cursorContext );
            }
            consistencyCheckSingleCheckable( report, ProgressListener.NONE, countsStore, RecordType.COUNTS, CursorContext.NULL );
        }
        catch ( Exception e )
        {
            log.error( "Counts store is missing, broken or of an older format and will not be consistency checked", e );
            summary.genericError( "Counts store is missing, broken or of an older format" );
        }
    }

    private void checkRelationshipGroupDegressStore()
    {
        try ( var relationshipGroupDegrees = new GBPTreeRelationshipGroupDegreesStore( pageCache, databaseLayout.relationshipGroupDegreesStore(), fileSystem,
                RecoveryCleanupWorkCollector.ignore(), new GBPTreeRelationshipGroupDegreesStore.DegreesRebuilder()
        {
            @Override
            public void rebuild( RelationshipGroupDegreesStore.Updater updater, CursorContext cursorContext, MemoryTracker memoryTracker )
            {
                throw new UnsupportedOperationException(
                        "Counts store needed rebuild, consistency checker will instead report broken or missing store" );
            }

            @Override
            public long lastCommittedTxId()
            {
                return neoStores.getMetaDataStore().getLastCommittedTransactionId();
            }
        }, readOnly(), cacheTracer, GBPTreeGenericCountsStore.NO_MONITOR, databaseLayout.getDatabaseName(), 100, NullLogProvider.getInstance() ) )
        {
            consistencyCheckSingleCheckable( report, ProgressListener.NONE, relationshipGroupDegrees, RecordType.RELATIONSHIP_GROUP, CursorContext.NULL );
        }
        catch ( Exception e )
        {
            log.error( "Relationship group degrees is missing, broken or of an older format and will not be consistency checked", e );
            summary.genericError( "Relationship group degrees store is missing, broken or of an older format" );
        }
    }

    private static TokenHolders safeLoadTokens( NeoStores neoStores, PageCacheTracer cacheTracer )
    {
        TokenHolders tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        try ( var cursorContext = new CursorContext( cacheTracer.createPageCursorTracer( CONSISTENCY_CHECKER_TOKEN_LOADER_TAG ) ) )
        {
            tokenHolders.relationshipTypeTokens().setInitialTokens( RecordLoading.safeLoadTokens( neoStores.getRelationshipTypeTokenStore(), cursorContext ) );
            tokenHolders.labelTokens().setInitialTokens( RecordLoading.safeLoadTokens( neoStores.getLabelTokenStore(), cursorContext ) );
            tokenHolders.propertyKeyTokens().setInitialTokens( RecordLoading.safeLoadTokens( neoStores.getPropertyKeyTokenStore(), cursorContext ) );
        }
        return tokenHolders;
    }

    private void cancel( String message )
    {
        if ( !isCancelled() )
        {
            context.debug( "Stopping: %s", message );
            context.cancel();
        }
    }

    private boolean isCancelled()
    {
        return context.isCancelled();
    }

    private void consistencyCheckSingleCheckable( InconsistencyReport report, ProgressListener listener, ConsistencyCheckable checkable,
            RecordType recordType, CursorContext cursorContext )
    {
        if ( consistencyFlags.isCheckIndexStructure() )
        {
            ConsistencyReporter.FormattingDocumentedHandler handler = ConsistencyReporter.formattingHandler( report, recordType );
            ReporterFactory proxyFactory = new ReporterFactory( handler );

            checkable.consistencyCheck( proxyFactory, cursorContext );
            handler.updateSummary();
            listener.add( 1 );
        }
    }
}
