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
package org.neo4j.consistency.newchecker;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.newchecker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.time.Stopwatch;
import org.neo4j.token.NonTransactionalTokenNameLookup;
import org.neo4j.token.TokenHolders;

import static org.neo4j.internal.helpers.Format.duration;

class CheckerContext
{
    final NeoStores neoStores;
    final IndexAccessors indexAccessors;
    final ConsistencyFlags consistencyFlags;
    final IndexSizes indexSizes;
    final LabelScanStore labelScanStore;
    final ParallelExecution execution;
    final ConsistencyReport.Reporter reporter;
    final CacheAccess cacheAccess;
    final TokenHolders tokenHolders;
    final RecordLoading recordLoader;
    final CountsState observedCounts;
    final NodeBasedMemoryLimiter limiter;
    final ProgressMonitorFactory.MultiPartBuilder progress;
    final TokenNameLookup tokenNameLookup;
    final PageCache pageCache;
    final long highNodeId;
    private final boolean debug;
    private final AtomicBoolean cancelled;

    CheckerContext(
            NeoStores neoStores,
            IndexAccessors indexAccessors,
            LabelScanStore labelScanStore,
            ParallelExecution execution,
            ConsistencyReport.Reporter reporter,
            CacheAccess cacheAccess,
            TokenHolders tokenHolders,
            RecordLoading recordLoader,
            CountsState observedCounts,
            NodeBasedMemoryLimiter limiter,
            ProgressMonitorFactory.MultiPartBuilder progress,
            PageCache pageCache,
            boolean debug,
            ConsistencyFlags consistencyFlags )
    {
        this( neoStores, indexAccessors, labelScanStore, execution, reporter, cacheAccess, tokenHolders, recordLoader, observedCounts, limiter, progress,
                pageCache, debug, new AtomicBoolean(), consistencyFlags );
    }

    private CheckerContext(
            NeoStores neoStores,
            IndexAccessors indexAccessors,
            LabelScanStore labelScanStore,
            ParallelExecution execution,
            ConsistencyReport.Reporter reporter,
            CacheAccess cacheAccess,
            TokenHolders tokenHolders,
            RecordLoading recordLoader,
            CountsState observedCounts,
            NodeBasedMemoryLimiter limiter,
            ProgressMonitorFactory.MultiPartBuilder progress,
            PageCache pageCache,
            boolean debug,
            AtomicBoolean cancelled,
            ConsistencyFlags consistencyFlags )
    {
        this.neoStores = neoStores;
        this.highNodeId = neoStores.getNodeStore().getHighId();
        this.indexAccessors = indexAccessors;
        this.debug = debug;
        this.consistencyFlags = consistencyFlags;
        this.indexSizes = new IndexSizes( execution, indexAccessors, neoStores.getNodeStore().getHighId() );
        this.labelScanStore = labelScanStore;
        this.execution = execution;
        this.reporter = reporter;
        this.cacheAccess = cacheAccess;
        this.tokenHolders = tokenHolders;
        this.recordLoader = recordLoader;
        this.observedCounts = observedCounts;
        this.limiter = limiter;
        this.progress = progress;
        this.cancelled = cancelled;
        this.tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders, true );
        this.pageCache = pageCache;
    }

    CheckerContext withoutReporting()
    {
        return new CheckerContext( neoStores, indexAccessors, labelScanStore, execution, ConsistencyReport.NO_REPORT, cacheAccess, tokenHolders,
                recordLoader, observedCounts, limiter, progress, pageCache, debug, cancelled, consistencyFlags );
    }

    void initialize() throws Exception
    {
        debug( limiter.toString() );
        timeOperation( "Initialize index sizes", indexSizes::initialize, false );
        if ( debug )
        {
            debugPrintIndexes( indexSizes.largeIndexes( EntityType.NODE ), "considered large node indexes" );
            debugPrintIndexes( indexSizes.smallIndexes( EntityType.NODE ), "considered small node indexes" );
            debugPrintIndexes( indexAccessors.onlineRules( EntityType.RELATIONSHIP ), "the relationship indexes" );
        }
    }

    void initializeRange()
    {
        observedCounts.clearDynamicNodeLabelsCache();
    }

    private void debugPrintIndexes( List<IndexDescriptor> indexes, String description )
    {
        if ( !indexes.isEmpty() )
        {
            debug( "These are %s (%d):", description, indexes.size() );
            indexes.forEach( index -> debug( "  %s", index ) );
        }
    }

    private void timeOperation( String description, ThrowingRunnable action, boolean linePadding ) throws Exception
    {
        Stopwatch stopwatch = Stopwatch.start();
        try
        {
            debug( linePadding, "STARTED %s", description );
            action.doRun();
            debug( linePadding, "COMPLETED %s, took %s", description, duration( stopwatch.elapsed( TimeUnit.MILLISECONDS ) ) );
        }
        catch ( Exception e )
        {
            debug( linePadding, "FAILED %s after %s", description, duration( stopwatch.elapsed( TimeUnit.MILLISECONDS ) ) );
            throw e;
        }
    }

    boolean isCancelled()
    {
        return cancelled.get();
    }

    void cancel()
    {
        cancelled.set( true );
    }

    void runIfAllowed( Checker checker, LongRange range ) throws Exception
    {
        if ( !isCancelled() && checker.shouldBeChecked( consistencyFlags ) )
        {
            timeOperation( checker.toString(), () -> checker.check( range, limiter.isFirst( range ), limiter.isLast( range ) ), true );
        }
    }

    void paddedDebug( String format, Object... params )
    {
        debug( true, format, params );
    }

    void debug( String format, Object... params )
    {
        debug( false, format, params );
    }

    private void debug( boolean linePadded, String format, Object... params )
    {
        if ( debug )
        {
            System.out.println( String.format( (linePadded ? "%n" : "") + format, params ) );
        }
    }

    ProgressListener progressReporter( Checker checker, String name, long totalCount )
    {
        return roundInsensitiveProgressReporter( checker, name, totalCount * limiter.numberOfRanges() );
    }

    ProgressListener roundInsensitiveProgressReporter( Checker checker, String name, long totalCount )
    {
        if ( !checker.shouldBeChecked( consistencyFlags ) )
        {
            return ProgressListener.NONE;
        }
        return progress.progressForPart( name, totalCount );
    }
}
