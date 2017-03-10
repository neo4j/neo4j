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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * Scans the entire tree and checks all GSPPs, replacing all CRASH gen GSPs with zeros.
 */
class CrashGenCleaner
{
    private final PagedFile pagedFile;
    private final TreeNode<?,?> treeNode;
    private final long lowTreeNodeId;
    private final long highTreeNodeId;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final Monitor monitor;
    private final long maxKeyCount;

    public CrashGenCleaner( PagedFile pagedFile, TreeNode<?,?> treeNode, long lowTreeNodeId, long highTreeNodeId,
            long stableGeneration, long unstableGeneration, Monitor monitor )
    {
        this.pagedFile = pagedFile;
        this.treeNode = treeNode;
        this.lowTreeNodeId = lowTreeNodeId;
        this.highTreeNodeId = highTreeNodeId;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.monitor = monitor;
        this.maxKeyCount = treeNode.internalMaxKeyCount();
    }

    // === Methods about the execution and threading ===

    public void clean() throws IOException
    {
        assert unstableGeneration > stableGeneration;
        assert unstableGeneration - stableGeneration > 1;

        long startTime = currentTimeMillis();
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool( threads );
        AtomicLong nextId = new AtomicLong( lowTreeNodeId);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger cleanedPointers = new AtomicInteger();
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( cleaner( nextId, error, cleanedPointers ) );
        }
        executor.shutdown();

        try
        {
            long lastProgression = nextId.get();
            // Have max no-progress-timeout quite high to be able to cope with huge
            // I/O congestion spikes w/o failing in vain.
            while ( !executor.awaitTermination( 30, SECONDS ) )
            {
                if ( lastProgression == nextId.get() )
                {
                    // No progression at all, abort?
                    error.compareAndSet( null, new IOException( "No progress, so forcing abort" ) );
                }
                lastProgression = nextId.get();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }

        Throwable finalError = error.get();
        if ( finalError != null )
        {
            throw launderedException( IOException.class, finalError );
        }

        long endTime = currentTimeMillis();
        monitor.recoveryCompleted( highTreeNodeId - lowTreeNodeId, cleanedPointers.get(), endTime - startTime );
    }

    private Runnable cleaner( AtomicLong nextId, AtomicReference<Throwable> error, AtomicInteger cleanedPointers )
    {
        return () ->
        {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK );
                    PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                while ( nextId.get() < highTreeNodeId )
                {
                    // Do a batch of max 1000
                    int batchCount = 0;
                    for ( ; batchCount < 1_000; batchCount++ )
                    {
                        long id = nextId.getAndIncrement();
                        if ( id >= highTreeNodeId )
                        {
                            break;
                        }

                        // TODO: we should try to figure out if the disk backing this paged file
                        // is good at concurrent random reads (i.e. if it's an SSD).
                        // If it's not it'd likely be beneficial to let other threads wait until
                        // goTo completes before letting others read. This will have reads be
                        // 100% sequential.
                        PageCursorUtil.goTo( cursor, "clean", id );

                        if ( hasCrashedGSPP( treeNode, cursor ) )
                        {
                            writeCursor.next( cursor.getCurrentPageId() );
                            cleanTreeNode( treeNode, writeCursor, cleanedPointers );
                        }
                    }

                    // Check error status after a batch, to reduce volatility overhead.
                    // Is this over thinking things? Perhaps
                    if ( error.get() != null )
                    {
                        break;
                    }
                }
            }
            catch ( Throwable e )
            {
                error.compareAndSet( null, e );
            }
        };
    }

    // === Methods about checking if a tree node has crashed pointers ===

    private boolean hasCrashedGSPP( TreeNode<?,?> treeNode, PageCursor cursor ) throws IOException
    {
        boolean isTreeNode;
        int keyCount;
        do
        {
            isTreeNode = TreeNode.nodeType( cursor ) == TreeNode.NODE_TYPE_TREE_NODE;
            keyCount = treeNode.keyCount( cursor );
        }
        while ( cursor.shouldRetry() );

        if ( !isTreeNode )
        {
            return false;
        }

        boolean hasCrashed;
        do
        {
            hasCrashed =
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_NEWGEN ) ||
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_LEFTSIBLING ) ||
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_RIGHTSIBLING );

            if ( !hasCrashed && TreeNode.isInternal( cursor ) )
            {
                for ( int i = 0; i <= keyCount && i <= maxKeyCount && !hasCrashed; i++ )
                {
                    hasCrashed |= hasCrashedGSPP( cursor, treeNode.childOffset( i ) );
                }
            }
        }
        while ( cursor.shouldRetry() );
        return hasCrashed;
    }

    private boolean hasCrashedGSPP( PageCursor cursor, int gsppOffset )
    {
        return hasCrashedGSP( cursor, gsppOffset ) || hasCrashedGSP( cursor, gsppOffset + GenSafePointer.SIZE );
    }

    private boolean hasCrashedGSP( PageCursor cursor, int offset )
    {
        cursor.setOffset( offset );
        long generation = GenSafePointer.readGeneration( cursor );
        return generation > stableGeneration && generation < unstableGeneration;
    }

    // === Methods about actually cleaning a discovered crashed tree node ===

    private void cleanTreeNode( TreeNode<?,?> treeNode, PageCursor cursor, AtomicInteger cleanedPointers )
    {
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_NEWGEN, cleanedPointers );
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_LEFTSIBLING, cleanedPointers );
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_RIGHTSIBLING, cleanedPointers );

        if ( TreeNode.isInternal( cursor ) )
        {
            int keyCount = treeNode.keyCount( cursor );
            for ( int i = 0; i <= keyCount && i <= maxKeyCount; i++ )
            {
                cleanCrashedGSPP( cursor, treeNode.childOffset( i ), cleanedPointers );
            }
        }
    }

    private void cleanCrashedGSPP( PageCursor cursor, int gsppOffset, AtomicInteger cleanedPointers )
    {
        cleanCrashedGSP( cursor, gsppOffset, cleanedPointers );
        cleanCrashedGSP( cursor, gsppOffset + GenSafePointer.SIZE, cleanedPointers );
    }

    private void cleanCrashedGSP( PageCursor cursor, int gspOffset, AtomicInteger cleanedPointers )
    {
        if ( hasCrashedGSP( cursor, gspOffset ) )
        {
            cursor.setOffset( gspOffset );
            GenSafePointer.clean( cursor );
            cleanedPointers.incrementAndGet();
        }
    }
}
