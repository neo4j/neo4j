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
package org.neo4j.index.bptree;

import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.io.pagecache.PageCursor;

public class TreePrinter
{
    /**
     * Prints a {@link BPTreeIndex} in human readable form, very useful for debugging.
     *
     * @param cursor {@link PageCursor} placed at root.
     * @param treeNode {@link TreeNode} knowing about how to read keys, values and children.
     * @param layout {@link Layout} for key/value.
     * @param out target to print tree at.
     * @throws IOException on page cache access error.
     */
    public static <KEY,VALUE> void printTree( PageCursor cursor, TreeNode<KEY,VALUE> treeNode,
            Layout<KEY,VALUE> layout, PrintStream out ) throws IOException
    {
        int level = 0;
        long id;
        while ( treeNode.isInternal( cursor ) )
        {
            out.println( "Level " + level++ );
            id = cursor.getCurrentPageId();
            printKeysOfSiblings( cursor, treeNode, layout, out );
            out.println();
            cursor.next( id );

            cursor.next( treeNode.childAt( cursor, 0 ) );
        }

        out.println( "Level " + level );
        printKeysOfSiblings( cursor, treeNode, layout, out );
        out.println();
    }

    private static <KEY,VALUE> void printKeysOfSiblings( PageCursor cursor,
            TreeNode<KEY,VALUE> bTreeNode, Layout<KEY,VALUE> layout, PrintStream out ) throws IOException
    {
        while ( true )
        {
            printKeys( cursor, bTreeNode, layout, out );
            long rightSibling = bTreeNode.rightSibling( cursor );
            if ( !bTreeNode.isNode( rightSibling ) )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    private static <KEY,VALUE> void printKeys( PageCursor cursor, TreeNode<KEY,VALUE> bTreeNode,
            Layout<KEY,VALUE> layout, PrintStream out )
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
                        bTreeNode.keyAt( cursor, key, i ) + "=" +
                        bTreeNode.valueAt( cursor, value, i ) );
            }
            else
            {
                out.print( "#" + i + ":" +
                        "|" + bTreeNode.childAt( cursor, i ) + "|" +
                        bTreeNode.keyAt( cursor, key, i ) + "|" );

            }
        }
        if ( !isLeaf )
        {
            out.print( "#" + keyCount + ":|" + bTreeNode.childAt( cursor, keyCount ) + "|" );
        }
        out.println( (isLeaf ? "]" : "|") );
    }
}
