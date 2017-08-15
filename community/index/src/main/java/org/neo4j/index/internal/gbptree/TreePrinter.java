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

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.index.internal.gbptree.TreeNode.Section;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.ConsistencyChecker.assertOnTreeNode;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;

/**
 * Utility class for printing a {@link GBPTree}, either whole or sub-tree.
 */
class TreePrinter<KEY,VALUE>
{
    private final TreeNode<KEY,VALUE> node;
    private final Layout<KEY,VALUE> layout;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final Section<KEY,VALUE> mainContent;
    private final Section<KEY,VALUE> deltaContent;

    TreePrinter( TreeNode<KEY,VALUE> node, Layout<KEY,VALUE> layout, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.mainContent = node.main();
        this.deltaContent = node.delta();
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
     * @param out target to print tree at.
     * @param printPosition whether or not to include positional (slot number) information.
     * @param printState whether or not to also print state pages
     * @throws IOException on page cache access error.
     */
    void printTree( PageCursor cursor, PrintStream out, boolean printValues, boolean printPosition, boolean printState )
            throws IOException
    {
        if ( printState )
        {
            long currentPage = cursor.getCurrentPageId();
            Pair<TreeState,TreeState> statePair = TreeStatePair.readStatePages(
                    cursor, IdSpace.STATE_PAGE_A, IdSpace.STATE_PAGE_B );
            node.goTo( cursor, "back to tree node from reading state", currentPage );
            out.println( "StateA: " + statePair.getLeft() );
            out.println( "StateB: " + statePair.getRight() );
        }
        assertOnTreeNode( node, cursor );

        // Traverse the tree
        int level = 0;
        do
        {
            // One level at the time
            out.println( "Level " + level++ );
            long leftmostSibling = cursor.getCurrentPageId();

            // Go right through all siblings
            printLevel( cursor, out, printValues, printPosition );

            // Then go back to the left-most node on this level
            node.goTo( cursor, "back", leftmostSibling );
        }
        // And continue down to next level if this level was an internal level
        while ( goToLeftmostChild( cursor ) );
    }

    void printTreeNode( PageCursor cursor, PrintStream out, boolean printValues, boolean printPosition,
            boolean printHeader ) throws IOException
    {
        boolean isLeaf;
        int keyCount;
        do
        {
            isLeaf = node.isLeaf( cursor );
            keyCount = mainContent.keyCount( cursor );
            if ( keyCount < 0 || (keyCount > mainContent.internalMaxKeyCount() && keyCount > mainContent.leafMaxKeyCount()) )
            {
                cursor.setCursorException( "Unexpected keyCount " + keyCount );
            }
        } while ( cursor.shouldRetry() );

        if ( printHeader )
        {
            //[TYPE][GEN][KEYCOUNT] ([RIGHTSIBLING][LEFTSIBLING][SUCCESSOR]))
            long generation = -1;
            do
            {
                generation = node.generation( cursor );

            } while ( cursor.shouldRetry() );
            String treeNodeType = isLeaf ? "leaf" : "internal";
            out.print( format( "{%d,%s,generation=%d,keyCount=%d}",
                    cursor.getCurrentPageId(), treeNodeType, generation, keyCount ) );
        }
        else
        {
            out.print( "{" + cursor.getCurrentPageId() + "} " );
        }
        printKeysAndValues( cursor, out, printValues, printPosition, isLeaf, mainContent );
        out.print( " DELTA " );
        printKeysAndValues( cursor, out, printValues, printPosition, isLeaf, deltaContent );
        if ( !isLeaf )
        {
            long child;
            do
            {
                child = pointer( mainContent.childAt( cursor, keyCount, stableGeneration, unstableGeneration ) );
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

    private void printKeysAndValues( PageCursor cursor, PrintStream out, boolean printValues, boolean printPosition,
            boolean isLeaf, Section<KEY,VALUE> section ) throws IOException
    {
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        int keyCount = section.keyCount( cursor );
        for ( int i = 0; i < keyCount; i++ )
        {
            long child = -1;
            do
            {
                section.keyAt( cursor, key, i );
                if ( isLeaf )
                {
                    section.valueAt( cursor, value, i );
                }
                else
                {
                    child = pointer( section.childAt( cursor, i, stableGeneration, unstableGeneration ) );
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
    }

    private boolean goToLeftmostChild( PageCursor cursor ) throws IOException
    {
        boolean isInternal;
        long leftmostSibling = -1;
        do
        {
            isInternal = node.isInternal( cursor );
            if ( isInternal )
            {
                leftmostSibling = mainContent.childAt( cursor, 0, stableGeneration, unstableGeneration );
            }
        }
        while ( cursor.shouldRetry() );

        if ( isInternal )
        {
            node.goTo( cursor, "child", leftmostSibling );
        }
        return isInternal;
    }

    private void printLevel( PageCursor cursor, PrintStream out, boolean printValues, boolean printPosition )
            throws IOException
    {
        long rightSibling = -1;
        do
        {

            printTreeNode( cursor, out, printValues, printPosition, false );

            do
            {
                rightSibling = node.rightSibling( cursor, stableGeneration, unstableGeneration );
            }
            while ( cursor.shouldRetry() );

            if ( TreeNode.isNode( rightSibling ) )
            {
                node.goTo( cursor, "right sibling", rightSibling );
            }
        }
        while ( TreeNode.isNode( rightSibling ) );
    }
}
