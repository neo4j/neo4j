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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Check order of keys in isolated nodes
 * Check keys fit inside range given by parent node
 * Check sibling pointers match
 */
class BPTreeConsistencyChecker<KEY>
{
    private final TreeNode<KEY,?> node;
    private final KEY readKey;
    private final Comparator<KEY> comparator;
    private final Layout<KEY,?> layout;
    private final List<RightmostInChain> rightmostPerLevel = new ArrayList<>();

    BPTreeConsistencyChecker( TreeNode<KEY,?> node, Layout<KEY,?> layout )
    {
        this.node = node;
        this.readKey = layout.newKey();
        this.comparator = node.keyComparator();
        this.layout = layout;
    }

    public boolean check( PageCursor cursor ) throws IOException
    {
        KEY leftSideOfRange = layout.newKey();
        layout.minKey( leftSideOfRange );
        KEY rightSideOfRange = layout.newKey();
        layout.maxKey( rightSideOfRange );
        KeyRange openRange = new KeyRange( comparator, leftSideOfRange, rightSideOfRange, layout );
        boolean result = checkSubtree( cursor, openRange, 0 );

        // Assert that rightmost node on each level has empty right sibling.
        rightmostPerLevel.forEach( RightmostInChain::assertLast );
        return result;
    }

    private boolean checkSubtree( PageCursor cursor, KeyRange range, int level ) throws IOException
    {
        long pageId = assertSiblings( cursor, level );

        if ( node.isInternal( cursor ) )
        {
            assertKeyOrderAndSubtrees( cursor, pageId, range, level );
        }
        else if ( node.isLeaf( cursor ) )
        {
            assertKeyOrder( cursor, range );
        }
        else
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a page containing a tree node." );
        }
        return true;
    }

    // Assumption: We traverse the tree from left to right on every level
    private long assertSiblings( PageCursor cursor, int level )
    {
        // If this is the first time on this level, we will add a new entry
        for ( int i = rightmostPerLevel.size(); i <= level; i++ )
        {
            RightmostInChain<KEY> first = new RightmostInChain<>( node );
            rightmostPerLevel.add( i, first );
        }
        RightmostInChain rightmost = rightmostPerLevel.get( level );

        return rightmost.assertNext( cursor );
    }

    private void assertKeyOrderAndSubtrees( PageCursor cursor, final long pageId, KeyRange range, int level )
            throws IOException
    {
        int keyCount = node.keyCount( cursor );
        KEY prev = layout.newKey();
        KeyRange childRange;

        int pos = 0;
        while ( pos < keyCount )
        {
            node.keyAt( cursor, readKey, pos );
            assert range.inRange( readKey );
            if ( pos > 0 )
            {
                assert comparator.compare( prev, readKey ) < 0; // Assume unique keys
            }

            long child = node.childAt( cursor, pos );
            cursor.next( child );

            if ( pos == 0 )
            {
                childRange = range.restrictRight( readKey );
            }
            else
            {
                childRange = range.restrictLeft( prev ).restrictRight( readKey );
            }
            checkSubtree( cursor, childRange, level + 1 );
            cursor.next( pageId );

            layout.copyKey( readKey, prev );
            pos++;
        }

        // Check last child
        long child = node.childAt( cursor, pos );
        cursor.next( child );
        childRange = range.restrictLeft( prev );
        checkSubtree( cursor, childRange, level + 1 );
        cursor.next( pageId );
    }

    private void assertKeyOrder( PageCursor cursor, KeyRange range )
    {
        int keyCount = node.keyCount( cursor );
        KEY prev = layout.newKey();
        boolean first = true;
        for ( int pos = 0; pos < keyCount; pos++ )
        {
            node.keyAt( cursor, readKey, pos );
            assert range.inRange( readKey );
            if ( !first )
            {
                assert comparator.compare( prev, readKey ) < 0; // Assume unique keys
            }
            else
            {
                first = false;
            }
            layout.copyKey( readKey, prev );
        }
    }

    private class KeyRange
    {
        private final Comparator<KEY> comparator;
        private final KEY fromInclusive;
        private final KEY toExclusive;
        private final Layout<KEY,?> layout;

        private KeyRange( Comparator<KEY> comparator, KEY fromInclusive, KEY toExclusive, Layout<KEY,?> layout )
        {
            this.comparator = comparator;
            this.fromInclusive = layout.newKey();
            layout.copyKey( fromInclusive, this.fromInclusive );
            this.toExclusive = layout.newKey();
            layout.copyKey( toExclusive, this.toExclusive );
            this.layout = layout;
        }

        boolean inRange( KEY key )
        {
            if ( fromInclusive != null )
            {
                if ( toExclusive != null )
                {
                    return comparator.compare( key, fromInclusive ) >= 0 && comparator.compare( key, toExclusive ) < 0;
                }
                return comparator.compare( key, fromInclusive ) >= 0;
            }
            return toExclusive == null || comparator.compare( key, toExclusive ) < 0;
        }

        KeyRange restrictLeft( KEY left )
        {
            if ( comparator.compare( fromInclusive, left ) < 0 )
            {
                return new KeyRange( comparator, left, toExclusive, layout );
            }
            return new KeyRange( comparator, fromInclusive, toExclusive, layout );
        }

        KeyRange restrictRight( KEY right )
        {
            if ( comparator.compare( toExclusive, right ) > 0 )
            {
                return new KeyRange( comparator, fromInclusive, right, layout );
            }
            return new KeyRange( comparator, fromInclusive, toExclusive, layout );
        }
    }
}
