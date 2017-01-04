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
package org.neo4j.index.gbptree;

import java.io.IOException;
import java.io.PrintStream;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.gbptree.ConsistencyChecker.assertOnTreeNode;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointer;

/**
 * Utility class for printing a {@link GBPTree}, either whole or sub-tree.
 */
class TreePrinter<KEY,VALUE>
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
     * Prints a {@link GBPTree} in human readable form, very useful for debugging.
     * Let the passed in {@code cursor} point to the root or sub-tree (internal node) of what to print.
     * Will print sub-tree from that point. Leaves cursor at same page as when called. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} placed at root of tree or sub-tree.
     * @param treeNode {@link TreeNode} knowing about how to read keys, values and children.
     * @param layout {@link Layout} for key/value.
     * @param stableGeneration stable generation.
     * @param unstableGeneration unstable generation.
     * @param out target to print tree at.
     * @throws IOException on page cache access error.
     */
    void printTree( PageCursor cursor, PrintStream out, boolean printValues ) throws IOException
    {
        assertOnTreeNode( cursor );

        // Traverse the tree
        int level = 0;
        do
        {
            // One level at the time
            out.println( "Level " + level++ );
            long leftmostSibling = cursor.getCurrentPageId();

            // Go right through all siblings
            printLevel( cursor, out, printValues );

            // Then go back to the left-most node on this level
            node.goTo( cursor, "back", leftmostSibling );
        }
        // And continue down to next level if this level was an internal level
        while ( goToLeftmostChild( cursor ) );
    }

    private void printTreeNode( PageCursor cursor, int keyCount, boolean isLeaf, PrintStream out, boolean printValues )
            throws IOException
    {
        out.print( (isLeaf ? "[" : "|") + "{" + cursor.getCurrentPageId() + "}" );
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        for ( int i = 0; i < keyCount; i++ )
        {
            long child = -1;
            do
            {
                node.keyAt( cursor, key, i );
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

            if ( i > 0 )
            {
                out.print( "," );
            }

            if ( isLeaf )
            {
                out.print( "#" + i + ":" + key );
                if ( printValues )
                {
                    out.print( "=" + value );
                }
            }
            else
            {
                out.print( "#" + i + ":" + "|" + child + "|" + key + "|" );

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

            out.print( "#" + keyCount + ":|" + child + "|" );
        }
        out.println( (isLeaf ? "]" : "|") );
    }

    private boolean goToLeftmostChild( PageCursor cursor ) throws IOException
    {
        boolean isInternal;
        long leftmostSibling = -1;
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
            node.goTo( cursor, "child", leftmostSibling );
        }
        return isInternal;
    }

    private void printLevel( PageCursor cursor, PrintStream out, boolean printValues )
            throws IOException
    {
        int keyCount;
        long rightSibling = -1;
        boolean isLeaf;
        do
        {
            do
            {
                isLeaf = TreeNode.isLeaf( cursor );
                keyCount = node.keyCount( cursor );
                if ( keyCount < 0 || (keyCount > node.internalMaxKeyCount() && keyCount > node.leafMaxKeyCount()) )
                {
                    cursor.setCursorException( "Unexpected keyCount " + keyCount );
                    continue;
                }
                rightSibling = node.rightSibling( cursor, stableGeneration, unstableGeneration );
            }
            while ( cursor.shouldRetry() );

            printTreeNode( cursor, keyCount, isLeaf, out, printValues );

            if ( TreeNode.isNode( rightSibling ) )
            {
                node.goTo( cursor, "right sibling", rightSibling );
            }
        }
        while ( TreeNode.isNode( rightSibling ) );
    }
}
