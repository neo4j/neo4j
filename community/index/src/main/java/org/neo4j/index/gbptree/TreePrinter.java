/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import static org.neo4j.index.gbptree.GenSafePointerPair.pointer;

/**
 * Utility class for printing a {@link GBPTree}, either whole or sub-tree.
 */
class TreePrinter
{
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
    static <KEY,VALUE> void printTree( PageCursor cursor, TreeNode<KEY,VALUE> treeNode,
            Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration, PrintStream out,
            boolean printValues ) throws IOException
    {
        int level = 0;
        long firstId = cursor.getCurrentPageId();
        long leftmostOnLevel;
        while ( treeNode.isInternal( cursor ) )
        {
            out.println( "Level " + level++ );
            leftmostOnLevel = cursor.getCurrentPageId();
            printKeysOfSiblings( cursor, treeNode, layout, stableGeneration, unstableGeneration, out, printValues );
            out.println();
            cursor.next( leftmostOnLevel );

            cursor.next( pointer( treeNode.childAt( cursor, 0, stableGeneration, unstableGeneration ) ) );
        }

        out.println( "Level " + level );
        printKeysOfSiblings( cursor, treeNode, layout, stableGeneration, unstableGeneration, out, printValues );
        out.println();
        cursor.next( firstId );
    }

    private static <KEY,VALUE> void printKeysOfSiblings( PageCursor cursor,
            TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration,
            PrintStream out, boolean printValues ) throws IOException
    {
        while ( true )
        {
            printKeys( cursor, bTreeNode, layout, stableGeneration, unstableGeneration, out, printValues );
            long rightSibling = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
            if ( !TreeNode.isNode( rightSibling ) )
            {
                break;
            }
            cursor.next( pointer( rightSibling ) );
        }
    }

    private static <KEY,VALUE> void printKeys( PageCursor cursor, TreeNode<KEY,VALUE> bTreeNode,
            Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration, PrintStream out,
            boolean printValues )
    {
        boolean isLeaf = bTreeNode.isLeaf( cursor );
        int keyCount = bTreeNode.keyCount( cursor );
        out.print( (isLeaf ? "[" : "|") + "{" + cursor.getCurrentPageId() + "}" );
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        for ( int i = 0; i < keyCount; i++ )
        {
            if ( i > 0 )
            {
                out.print( "," );
            }

            if ( isLeaf )
            {
                out.print( "#" + i + ":" +
                        bTreeNode.keyAt( cursor, key, i ) );
                if ( printValues )
                {
                    out.print( "=" + bTreeNode.valueAt( cursor, value, i ) );
                }
            }
            else
            {
                out.print( "#" + i + ":" +
                        "|" + pointer( bTreeNode.childAt( cursor, i, stableGeneration, unstableGeneration ) ) + "|" +
                        bTreeNode.keyAt( cursor, key, i ) + "|" );

            }
        }
        if ( !isLeaf )
        {
            out.print( "#" + keyCount + ":|" +
                    pointer( bTreeNode.childAt( cursor, keyCount, stableGeneration, unstableGeneration ) ) + "|" );
        }
        out.println( (isLeaf ? "]" : "|") );
    }
}
