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
package org.neo4j.consistency.checker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.counts.CountsStore;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.consistency_checker_fail_fast_threshold;
import static org.neo4j.consistency.checker.ParallelExecution.DEFAULT_IDS_PER_CHUNK;
import static org.neo4j.consistency.checking.cache.CacheSlots.ID_SLOT_SIZE;
import static org.neo4j.consistency.checking.cache.DefaultCacheAccess.defaultByteArray;

/**
 * A consistency checker for a {@link RecordStorageEngine}, focused on keeping abstractions to a minimum and having clean and understandable
 * algorithms. Revolves around the {@link CacheAccess} which uses up {@code 11 * NodeStore#getHighId()}. The checking is split up into a couple
 * of checkers, mostly focused on a single store, e.g. nodes or relationships, which are run one after the other. Each checker's algorithm is
 * designed to try to maximize both CPU and I/O to its full extent, or rather at least maxing out one of them.
 */
public class RecordStorageConsistencyChecker implements AutoCloseable
{
    private static final String COUNT_STORE_CONSISTENCY_CHECKER_TAG = "countStoreConsistencyChecker";
    private static final String SCHEMA_CONSISTENCY_CHECKER_TAG = "schemaConsistencyChecker";
    private static final String CONSISTENCY_CHECKER_TOKEN_LOADER_TAG = "consistencyCheckerTokenLoader";
    static final int[] DEFAULT_SLOT_SIZES = {ID_SLOT_SIZE, ID_SLOT_SIZE, 1, 1, 1, 1, 1};

    private final PageCache pageCache;
    private final NeoStores neoStores;
    private final TokenHolders tokenHolders;
    private final CountsStore counts;
    private final PageCacheTracer cacheTracer;
    private final CacheAccess cacheAccess;
    private final ConsistencyReporter reporter;
    private final CountsState observedCounts;
    private final NodeBasedMemoryLimiter limiter;
    private final CheckerContext context;
    private final ProgressMonitorFactory.MultiPartBuilder progress;

    public RecordStorageConsistencyChecker( PageCache pageCache, NeoStores neoStores, CountsStore counts, LabelScanStore labelScanStore,
            RelationshipTypeScanStore relationshipTypeScanStore, IndexAccessors indexAccessors, InconsistencyReport report,
            ProgressMonitorFactory progressFactory, Config config, int numberOfThreads, boolean debug, ConsistencyFlags consistencyFlags,
            NodeBasedMemoryLimiter.Factory memoryLimit, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        this.pageCache = pageCache;
        this.neoStores = neoStores;
        this.counts = counts;
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
        this.reporter = new ConsistencyReporter( report, monitor );
        this.tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        ParallelExecution execution = new ParallelExecution( numberOfThreads,
                exception -> cancel( "Unexpected exception" ), // Exceptions should interrupt all threads to exit faster
                DEFAULT_IDS_PER_CHUNK );
        RecordLoading recordLoading = new RecordLoading( neoStores );
        this.limiter = instantiateMemoryLimiter( memoryLimit );
        this.cacheAccess = new DefaultCacheAccess( defaultByteArray( limiter.rangeSize(), memoryTracker ), Counts.NONE, numberOfThreads );
        this.observedCounts = new CountsState( neoStores, cacheAccess, memoryTracker );
        this.progress = progressFactory.multipleParts( "Consistency check" );
        this.context = new CheckerContext( neoStores, indexAccessors, labelScanStore, relationshipTypeScanStore, execution,
                reporter, cacheAccess, tokenHolders, recordLoading, observedCounts, limiter, progress, pageCache, cacheTracer, memoryTracker,
                debug, consistencyFlags );
    }

    public void check() throws ConsistencyCheckIncompleteException
    {
        assert !context.isCancelled();
        try
        {
            context.initialize();
            // Starting by loading all tokens from store into the TokenHolders, loaded in a safe way of course
            safeLoadTokens( neoStores );
            // Check schema - constraints and indexes, that sort of thing
            // This is done before instantiating the other checker instances because the schema checker will also
            // populate maps regarding mandatory properties which the node/relationship checkers uses
            SchemaChecker schemaChecker = new SchemaChecker( context );
            MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties = new IntObjectHashMap<>();
            MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties = new IntObjectHashMap<>();
            try ( var cursorTracer = cacheTracer.createPageCursorTracer( SCHEMA_CONSISTENCY_CHECKER_TAG ) )
            {
                schemaChecker.check( mandatoryNodeProperties, mandatoryRelationshipProperties, cursorTracer );
            }

            // Some pieces of check logic are extracted from this main class to reduce the size of this class. Instantiate those here first
            NodeChecker nodeChecker = new NodeChecker( context, mandatoryNodeProperties );
            IndexChecker indexChecker = new IndexChecker( context, EntityType.NODE );
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

                LongRange range = limiter.next();
                if ( numberOfRanges > 1 )
                {
                    context.debug( "=== Checking range %d/%d (%s) ===", i, numberOfRanges, range );
                }
                context.initializeRange();

                // Tell the cache that the pivot node id is the low end of this range. This will make all interactions with the cache
                // take that into consideration when working with offset arrays where the index is based on node ids.
                cacheAccess.setPivotId( range.from() );

                // Go into a node-centric mode where the nodes themselves are checked and somewhat cached off-heap.
                // Then while we have the nodes loaded in cache do all other checking that has anything to do with nodes
                // so that the "other" store can be checked sequentially and the random node lookups will be cheap
                context.runIfAllowed( indexChecker, range );
                cacheAccess.setCacheSlotSizesAndClear( DEFAULT_SLOT_SIZES );
                context.runIfAllowed( nodeChecker, range );
                context.runIfAllowed( relationshipGroupChecker, range );
                context.runIfAllowed( relationshipChecker, range );
                context.runIfAllowed( relationshipChainChecker, range );
            }

            if ( !isCancelled() && context.consistencyFlags.isCheckGraph() )
            {
                // All counts we've observed while doing other checking along the way we compare against the counts store here
                checkCounts();
            }
            progressCompleter.close();
        }
        catch ( Exception e )
        {
            cancel( "ConsistencyChecker failed unexpectedly" );
            throw new ConsistencyCheckIncompleteException( e );
        }
    }

    private NodeBasedMemoryLimiter instantiateMemoryLimiter( NodeBasedMemoryLimiter.Factory memoryLimit )
    {
        // The checker makes use of a large memory array to hold data per node. For large stores there may not be enough memory
        // to hold all node data and in that case the checking will happen iteratively where one part of the node store is selected
        // and checked and all other stores related to any of those nodes too. When that part is done the next part of the node store
        // is selected until all the nodes, e.g. all the data have been checked.

        long pageCacheMemory = pageCache.maxCachedPages() * pageCache.pageSize();
        long nodeCount = neoStores.getNodeStore().getHighId();
        return memoryLimit.create( pageCacheMemory, nodeCount );
    }

    @Override
    public void close() throws Exception
    {
        context.cancel();
        observedCounts.close();
    }

    private void checkCounts()
    {
        if ( counts != CountsStore.NULL_INSTANCE )
        {
            // Report unexpected counts from existing counts store --> counts collected in this consistency check
            try ( CountsState.CountsChecker checker = observedCounts.checker( reporter );
                  var cursorTracer = cacheTracer.createPageCursorTracer( COUNT_STORE_CONSISTENCY_CHECKER_TAG ) )
            {
                counts.accept( checker, cursorTracer );
            } // Here when closing we report counts that we've seen, but the counts store doesn't have
        }
    }

    private void safeLoadTokens( NeoStores neoStores )
    {
        try ( var cursorTracer = cacheTracer.createPageCursorTracer( CONSISTENCY_CHECKER_TOKEN_LOADER_TAG ) )
        {
            tokenHolders.relationshipTypeTokens().setInitialTokens( RecordLoading.safeLoadTokens( neoStores.getRelationshipTypeTokenStore(), cursorTracer ) );
            tokenHolders.labelTokens().setInitialTokens( RecordLoading.safeLoadTokens( neoStores.getLabelTokenStore(), cursorTracer ) );
            tokenHolders.propertyKeyTokens().setInitialTokens( RecordLoading.safeLoadTokens( neoStores.getPropertyKeyTokenStore(), cursorTracer ) );
        }
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
}
