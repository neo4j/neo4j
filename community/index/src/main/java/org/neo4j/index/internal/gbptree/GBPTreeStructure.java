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

import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

/**
 * Utility class for printing a {@link GBPTree}, either whole or sub-tree.
 *
 * @param <KEY> type of keys in the tree.
 * @param <VALUE> type of values in the tree.
 */
public class GBPTreeStructure<KEY, VALUE>
{
    private final TreeNode<KEY,VALUE> node;
    private final Layout<KEY,VALUE> layout;
    private final long stableGeneration;
    private final long unstableGeneration;

    GBPTreeStructure( TreeNode<KEY,VALUE> node, Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    /**
     * Visit the header, that is tree state and meta information, about the tree present in the given {@code file}.
     *
     * @param pageCache {@link PageCache} able to map tree contained in {@code file}.
     * @param file {@link File} containing the tree to print header for.
     * @param visitor {@link GBPTreeVisitor} that shall visit header.
     * @throws IOException on I/O error.
     */
    public static void visitHeader( PageCache pageCache, File file, GBPTreeVisitor visitor ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.READ ) )
        {
            try ( PageCursor cursor = pagedFile.io( IdSpace.STATE_PAGE_A, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                visitMeta( cursor, visitor );
                visitTreeState( cursor, visitor );
            }
        }
    }

    private static void visitMeta( PageCursor cursor, GBPTreeVisitor visitor ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "meta page", IdSpace.META_PAGE_ID );
        Meta meta = Meta.read( cursor, null );
        visitor.meta( meta );
    }

    static void visitTreeState( PageCursor cursor, GBPTreeVisitor visitor ) throws IOException
    {
        Pair<TreeState,TreeState> statePair = TreeStatePair.readStatePages(
                cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
        visitor.treeState( statePair );
    }

    /**
     * Let the passed in {@code cursor} point to the root or sub-tree (internal node) of what to visit.
     *
     * @param cursor {@link PageCursor} placed at root of tree or sub-tree.
     * @param writeCursor Currently active {@link PageCursor write cursor} in tree.
     * @param visitor {@link GBPTreeVisitor} that should visit the tree.
     * @throws IOException on page cache access error.
     */
    void visitTree( PageCursor cursor, PageCursor writeCursor, GBPTreeVisitor<KEY,VALUE> visitor ) throws IOException
    {
        // TreeState
        long currentPage = cursor.getCurrentPageId();
        Pair<TreeState,TreeState> statePair = TreeStatePair.readStatePages(
                cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
        visitor.treeState( statePair );
        TreeNode.goTo( cursor, "back to tree node from reading state", currentPage );

        assertOnTreeNode( select( cursor, writeCursor ) );

        // Traverse the tree
        int level = 0;
        do
        {
            // One level at the time
            visitor.beginLevel( level );
            long leftmostSibling = cursor.getCurrentPageId();

            // Go right through all siblings
            visitLevel( cursor, writeCursor, visitor );

            visitor.endLevel( level );
            level++;

            // Then go back to the left-most node on this level
            TreeNode.goTo( cursor, "back", leftmostSibling );
        }
        // And continue down to next level if this level was an internal level
        while ( goToLeftmostChild( cursor, writeCursor ) );
    }

    private static void assertOnTreeNode( PageCursor cursor ) throws IOException
    {
        byte nodeType;
        boolean isInternal;
        boolean isLeaf;
        do
        {
            nodeType = TreeNode.nodeType( cursor );
            isInternal = TreeNode.isInternal( cursor );
            isLeaf = TreeNode.isLeaf( cursor );
        }
        while ( cursor.shouldRetry() );

        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a tree node page. pageId:" +
                    cursor.getCurrentPageId() );
        }
        if ( !isInternal && !isLeaf )
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a page containing a tree node. pageId:" +
                    cursor.getCurrentPageId() );
        }
    }

    void visitTreeNode( PageCursor cursor, GBPTreeVisitor<KEY,VALUE> visitor ) throws IOException
    {
        //[TYPE][GEN][KEYCOUNT] ([RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]))
        boolean isLeaf;
        int keyCount;
        long generation = -1;
        do
        {
            isLeaf = TreeNode.isLeaf( cursor );
            keyCount = TreeNode.keyCount( cursor );
            if ( !node.reasonableKeyCount( keyCount ) )
            {
                cursor.setCursorException( "Unexpected keyCount " + keyCount );
            }
            generation = TreeNode.generation( cursor );
        }
        while ( cursor.shouldRetry() );
        visitor.beginNode( cursor.getCurrentPageId(), isLeaf, generation, keyCount );

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

            visitor.position( i );
            if ( isLeaf )
            {
                visitor.key( key, isLeaf );
                visitor.value( value );
            }
            else
            {
                visitor.child( child );
                visitor.key( key, isLeaf );
            }
        }
        if ( !isLeaf )
        {
            long child;
            do
            {
                child = pointer( node.childAt( cursor, keyCount, stableGeneration, unstableGeneration ) );
            }
            while ( cursor.shouldRetry() );
            visitor.position( keyCount );
            visitor.child( child );
        }
        visitor.endNode( cursor.getCurrentPageId() );
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

    private void visitLevel( PageCursor readCursor, PageCursor writeCursor, GBPTreeVisitor<KEY,VALUE> visitor ) throws IOException
    {
        long rightSibling = -1;
        do
        {
            PageCursor cursor = select( readCursor, writeCursor );
            visitTreeNode( cursor, visitor );

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
