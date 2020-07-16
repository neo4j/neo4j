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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.FeatureToggles;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Scans the entire tree and checks all GSPPs, replacing all CRASH gen GSPs with zeros.
 */
class CrashGenerationCleaner
{
    private static final String INDEX_CLEANER_TAG = "indexCleaner";
    private static final String NUMBER_OF_WORKERS_NAME = "number_of_workers";
    private static final int NUMBER_OF_WORKERS_DEFAULT = min( 8, Runtime.getRuntime().availableProcessors() );
    private static final int NUMBER_OF_WORKERS = FeatureToggles.getInteger( CrashGenerationCleaner.class, NUMBER_OF_WORKERS_NAME, NUMBER_OF_WORKERS_DEFAULT );

    private static final long MIN_BATCH_SIZE = 10;
    static final long MAX_BATCH_SIZE = 100;
    private final PagedFile pagedFile;
    private final TreeNode<?,?> treeNode;
    private final long lowTreeNodeId;
    private final long highTreeNodeId;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final Monitor monitor;
    private final PageCacheTracer pageCacheTracer;
    private final String treeName;

    CrashGenerationCleaner( PagedFile pagedFile, TreeNode<?,?> treeNode, long lowTreeNodeId, long highTreeNodeId,
            long stableGeneration, long unstableGeneration, Monitor monitor, PageCacheTracer pageCacheTracer, String treeName )
    {
        this.pagedFile = pagedFile;
        this.treeNode = treeNode;
        this.lowTreeNodeId = lowTreeNodeId;
        this.highTreeNodeId = highTreeNodeId;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.monitor = monitor;
        this.pageCacheTracer = pageCacheTracer;
        this.treeName = treeName;
    }

    private static long batchSize( long pagesToClean, int threads )
    {
        // Batch size at most maxBatchSize, at least minBatchSize and trying to give each thread 100 batches each
        return min( MAX_BATCH_SIZE, max( MIN_BATCH_SIZE, pagesToClean / (100L * threads) ) );
    }

    // === Methods about the execution and threading ===

    public void clean( CleanupJob.Executor executor )
    {
        monitor.cleanupStarted();
        assert unstableGeneration > stableGeneration : unexpectedGenerations();
        assert unstableGeneration - stableGeneration > 1 : unexpectedGenerations();

        Stopwatch startTime = Stopwatch.start();
        long pagesToClean = highTreeNodeId - lowTreeNodeId;
        int threads = NUMBER_OF_WORKERS;
        long batchSize = batchSize( pagesToClean, threads );
        AtomicLong nextId = new AtomicLong( lowTreeNodeId );
        AtomicBoolean stopFlag = new AtomicBoolean();
        LongAdder cleanedPointers = new LongAdder();
        LongAdder numberOfTreeNodes = new LongAdder();
        List<CleanupJob.JobResult<?>> jobResults = new ArrayList<>();
        for ( int i = 0; i < threads; i++ )
        {
            Callable<?> cleanerTask = cleaner( nextId, batchSize, numberOfTreeNodes, cleanedPointers, stopFlag, pageCacheTracer );
            CleanupJob.JobResult<?> jobHandle = executor.submit( "Recovery clean up of '" + treeName + "'", cleanerTask );
            jobResults.add( jobHandle );
        }

        awaitAll( jobResults );

        monitor.cleanupFinished( pagesToClean, numberOfTreeNodes.sum(), cleanedPointers.sum(), startTime.elapsed( MILLISECONDS ) );
    }

    private Callable<?> cleaner( AtomicLong nextId, long batchSize, LongAdder numberOfTreeNodes, LongAdder cleanedPointers, AtomicBoolean stopFlag,
            PageCacheTracer pageCacheTracer )
    {
        return () ->
        {
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_CLEANER_TAG );
                  PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK, cursorTracer );
                  PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer ) )
            {
                long localNextId;
                while ( ( localNextId = nextId.getAndAdd( batchSize )) < highTreeNodeId )
                {
                    int localNumberOfTreeNodes = 0;
                    for ( int i = 0; i < batchSize && localNextId < highTreeNodeId; i++, localNextId++ )
                    {
                        PageCursorUtil.goTo( cursor, "clean", localNextId );

                        boolean isTreeNode = isTreeNode( cursor );
                        if ( isTreeNode )
                        {
                            localNumberOfTreeNodes++;
                            if ( hasCrashedGSPP( this.treeNode, cursor ) )
                            {
                                writeCursor.next( cursor.getCurrentPageId() );
                                cleanTreeNode( this.treeNode, writeCursor, cleanedPointers );
                            }
                        }
                    }
                    numberOfTreeNodes.add( localNumberOfTreeNodes );

                    if ( stopFlag.get() )
                    {
                        break;
                    }
                }
            }
            catch ( Throwable e )
            {
                stopFlag.set( true );
                throw e;
            }
            return null;
        };
    }

    // === Methods about checking if a tree node has crashed pointers ===

    private boolean isTreeNode( PageCursor cursor ) throws IOException
    {
        boolean isTreeNode;
        do
        {
            isTreeNode = TreeNode.nodeType( cursor ) == TreeNode.NODE_TYPE_TREE_NODE;
        }
        while ( cursor.shouldRetry() );
        PageCursorUtil.checkOutOfBounds( cursor );
        return isTreeNode;
    }

    private boolean hasCrashedGSPP( TreeNode<?,?> treeNode, PageCursor cursor ) throws IOException
    {
        int keyCount;
        do
        {
            keyCount = TreeNode.keyCount( cursor );
        }
        while ( cursor.shouldRetry() );
        PageCursorUtil.checkOutOfBounds( cursor );

        boolean hasCrashed;
        do
        {
            hasCrashed =
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_SUCCESSOR ) ||
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_LEFTSIBLING ) ||
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_RIGHTSIBLING );

            if ( !hasCrashed && TreeNode.isInternal( cursor ) )
            {
                for ( int i = 0; i <= keyCount && treeNode.reasonableChildCount( i ) && !hasCrashed; i++ )
                {
                    hasCrashed = hasCrashedGSPP( cursor, treeNode.childOffset( i ) );
                }
            }
        }
        while ( cursor.shouldRetry() );
        PageCursorUtil.checkOutOfBounds( cursor );
        return hasCrashed;
    }

    private boolean hasCrashedGSPP( PageCursor cursor, int gsppOffset )
    {
        return hasCrashedGSP( cursor, gsppOffset ) || hasCrashedGSP( cursor, gsppOffset + GenerationSafePointer.SIZE );
    }

    private boolean hasCrashedGSP( PageCursor cursor, int offset )
    {
        cursor.setOffset( offset );
        long generation = GenerationSafePointer.readGeneration( cursor );
        return generation > stableGeneration && generation < unstableGeneration;
    }

    // === Methods about actually cleaning a discovered crashed tree node ===

    private void cleanTreeNode( TreeNode<?,?> treeNode, PageCursor cursor, LongAdder cleanedPointers )
    {
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_SUCCESSOR, cleanedPointers );
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_LEFTSIBLING, cleanedPointers );
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_RIGHTSIBLING, cleanedPointers );

        if ( TreeNode.isInternal( cursor ) )
        {
            int keyCount = TreeNode.keyCount( cursor );
            for ( int i = 0; i <= keyCount && treeNode.reasonableChildCount( i ); i++ )
            {
                cleanCrashedGSPP( cursor, treeNode.childOffset( i ), cleanedPointers );
            }
        }
    }

    private void cleanCrashedGSPP( PageCursor cursor, int gsppOffset, LongAdder cleanedPointers )
    {
        cleanCrashedGSP( cursor, gsppOffset, cleanedPointers );
        cleanCrashedGSP( cursor, gsppOffset + GenerationSafePointer.SIZE, cleanedPointers );
    }

    /**
     * NOTE: No shouldRetry is used because cursor is assumed to be a write cursor.
     */
    private void cleanCrashedGSP( PageCursor cursor, int gspOffset, LongAdder cleanedPointers )
    {
        if ( hasCrashedGSP( cursor, gspOffset ) )
        {
            cursor.setOffset( gspOffset );
            GenerationSafePointer.clean( cursor );
            cleanedPointers.increment();
        }
    }

    private String unexpectedGenerations( )
    {
        return "Unexpected generations, stableGeneration=" + stableGeneration + ", unstableGeneration=" + unstableGeneration;
    }

    private void awaitAll( Iterable<? extends CleanupJob.JobResult<?>> jobHandles )
    {
        Throwable finalError = null;
        for ( var jobHandle : jobHandles )
        {
            try
            {
                jobHandle.get();
            }
            catch ( Throwable e )
            {
                finalError = Exceptions.chain( finalError, e );
            }
        }
        if ( finalError != null )
        {
            Exceptions.throwIfUnchecked( finalError );
            throw new RuntimeException( finalError );
        }
    }
}
