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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.StandardOpenOption;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;

import static java.lang.String.format;
import static org.neo4j.graphdb.config.Configuration.EMPTY;
import static org.neo4j.index.internal.gbptree.ConsistencyChecker.assertOnTreeNode;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

/**
 * Utility class for printing a {@link GBPTree}, either whole or sub-tree.
 *
 * @param <KEY> type of keys in the tree.
 * @param <VALUE> type of values in the tree.
 */
public class TreePrinter<KEY, VALUE>
{
    private final TreeNode<KEY,VALUE> node;
    private final Layout<KEY,VALUE> layout;
    private final long stableGeneration;
    private final long unstableGeneration;

    TreePrinter( TreeNode<KEY,VALUE> node, Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    /**
     * Prints the header, that is tree state and meta information, about the tree present in the given {@code file}.
     *
     * @param fs {@link FileSystemAbstraction} where the {@code file} exists.
     * @param file {@link File} containing the tree to print header for.
     * @param out {@link PrintStream} to print at.
     * @throws IOException on I/O error.
     */
    public static void printHeader( FileSystemAbstraction fs, File file, PrintStream out ) throws IOException
    {
        SingleFilePageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        swapper.open( fs, EMPTY );
        PageCursorTracerSupplier cursorTracerSupplier = PageCursorTracerSupplier.NULL;
        try ( PageCache pageCache = new MuninnPageCache( swapper, 100, NULL, cursorTracerSupplier,
                EmptyVersionContextSupplier.EMPTY ) )
        {
            printHeader( pageCache, file, out );
        }
    }

    /**
     * Prints the header, that is tree state and meta information, about the tree present in the given {@code file}.
     *
     * @param pageCache {@link PageCache} able to map tree contained in {@code file}.
     * @param file {@link File} containing the tree to print header for.
     * @param out {@link PrintStream} to print at.
     * @throws IOException on I/O error.
     */
    public static void printHeader( PageCache pageCache, File file, PrintStream out ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.READ ) )
        {
            try ( PageCursor cursor = pagedFile.io( IdSpace.STATE_PAGE_A, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                // TODO add printing of meta information here when that abstraction has been merged.
                printTreeState( cursor, out );
            }
        }
    }

    private static void printTreeState( PageCursor cursor, PrintStream out ) throws IOException
    {
        Pair<TreeState,TreeState> statePair = TreeStatePair.readStatePages(
                cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
        out.println( "StateA: " + statePair.getLeft() );
        out.println( "StateB: " + statePair.getRight() );
    }

    /**
     * Prints a {@link GBPTree} in human readable form, very useful for debugging.
     * Let the passed in {@code cursor} point to the root or sub-tree (internal node) of what to print.
     * Will print sub-tree from that point. Leaves cursor at same page as when called. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} placed at root of tree or sub-tree.
     * @param writeCursor Currently active {@link PageCursor write cursor} in tree.
     * @param out target to print tree at.
     * @param printPosition whether or not to include positional (slot number) information.
     * @param printState whether or not to also print state pages
     * @param printHeader whether or not to also print header (type, generation, keyCount) of every node
     * @throws IOException on page cache access error.
     */
    void printTree( PageCursor cursor, PageCursor writeCursor, PrintStream out, boolean printValues, boolean printPosition,
            boolean printState, boolean printHeader ) throws IOException
    {
        if ( printState )
        {
            long currentPage = cursor.getCurrentPageId();
            printTreeState( cursor, out );
            TreeNode.goTo( cursor, "back to tree node from reading state", currentPage );
        }
        assertOnTreeNode( select( cursor, writeCursor ) );

        // Traverse the tree
        int level = 0;
        do
        {
            // One level at the time
            out.println( "Level " + level++ );
            long leftmostSibling = cursor.getCurrentPageId();

            // Go right through all siblings
            printLevel( cursor, writeCursor, out, printValues, printPosition, printHeader );

            // Then go back to the left-most node on this level
            TreeNode.goTo( cursor, "back", leftmostSibling );
        }
        // And continue down to next level if this level was an internal level
        while ( goToLeftmostChild( cursor, writeCursor ) );
    }

    void printTreeNode( PageCursor cursor, PrintStream out, boolean printValues, boolean printPosition,
            boolean printHeader ) throws IOException
    {
        boolean isLeaf;
        int keyCount;
        do
        {
            isLeaf = TreeNode.isLeaf( cursor );
            keyCount = TreeNode.keyCount( cursor );
            if ( !node.reasonableKeyCount( keyCount ) )
            {
                cursor.setCursorException( "Unexpected keyCount " + keyCount );
            }
        }
        while ( cursor.shouldRetry() );

        if ( printHeader )
        {
            //[TYPE][GEN][KEYCOUNT] ([RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]))
            long generation = -1;
            do
            {
                generation = TreeNode.generation( cursor );

            }
            while ( cursor.shouldRetry() );
            String treeNodeType = isLeaf ? "leaf" : "internal";
            out.print( format( "{%d,%s,generation=%d,keyCount=%d}",
                    cursor.getCurrentPageId(), treeNodeType, generation, keyCount ) );
        }
        else
        {
            out.print( "{" + cursor.getCurrentPageId() + "} " );
        }
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        for ( int i = 0; i < keyCount; i++ )
        {
            long child = -1;
            do
            {
                node.keyAt( cursor, key, i, isLeaf ? LEAF : INTERNAL );
                if ( isLeaf )
                {
                    node.valueAt( cursor, value, i );
                }
                else
                {
                    child = pointer( node.childAt( cursor, i, stableGeneration, unstableGeneration ) );
                }
            }
            while ( cursor.shouldRetry() );

            if ( printPosition )
            {
                out.print( "#" + i + " " );
            }

            if ( isLeaf )
            {
                out.print( key );
                if ( printValues )
                {
                    out.print( "=" + value );
                }
            }
            else
            {
                out.print( "/" + child + "\\ [" + key + "]" );
            }
            out.print( " " );
        }
        if ( !isLeaf )
        {
            long child;
            do
            {
                child = pointer( node.childAt( cursor, keyCount, stableGeneration, unstableGeneration ) );
            }
            while ( cursor.shouldRetry() );

            if ( printPosition )
            {
                out.print( "#" + keyCount + " " );
            }
            out.print( "/" + child + "\\" );
        }
        out.println();
    }

    private boolean goToLeftmostChild( PageCursor readCursor, PageCursor writeCursor ) throws IOException
    {
        boolean isInternal;
        long leftmostSibling = -1;
        PageCursor cursor = select( readCursor, writeCursor );
        do
        {
            isInternal = TreeNode.isInternal( cursor );
            if ( isInternal )
            {
                leftmostSibling = node.childAt( cursor, 0, stableGeneration, unstableGeneration );
            }
        }
        while ( cursor.shouldRetry() );

        if ( isInternal )
        {
            TreeNode.goTo( readCursor, "child", leftmostSibling );
        }
        return isInternal;
    }

    private void printLevel( PageCursor readCursor, PageCursor writeCursor, PrintStream out, boolean printValues, boolean printPosition,
            boolean printHeader ) throws IOException
    {
        long rightSibling = -1;
        do
        {
            PageCursor cursor = select( readCursor, writeCursor );
            printTreeNode( cursor, out, printValues, printPosition, printHeader );

            do
            {
                rightSibling = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
            }
            while ( cursor.shouldRetry() );

            if ( TreeNode.isNode( rightSibling ) )
            {
                TreeNode.goTo( readCursor, "right sibling", rightSibling );
            }
        }
        while ( TreeNode.isNode( rightSibling ) );
    }

    private static PageCursor select( PageCursor readCursor, PageCursor writeCursor )
    {
        return writeCursor == null ? readCursor :
               readCursor.getCurrentPageId() == writeCursor.getCurrentPageId() ? writeCursor : readCursor;
    }
}
