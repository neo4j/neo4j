package org.neo4j.index.bptree;

import java.io.IOException;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Check order of keys in isolated nodes
 * Check keys fit inside range given by parent node
 */
class BPTreeConsistencyChecker<KEY>
{
    private final TreeNode<KEY,?> node;
    private final KEY readKey;
    private final Comparator<KEY> comparator;
    private final Layout<KEY,?> layout;

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
        return checkSubtree( cursor, new KeyRange<>( comparator, leftSideOfRange, rightSideOfRange, layout ), 0 );
    }

    private boolean checkSubtree( PageCursor cursor, KeyRange<KEY> range, int level ) throws IOException
    {
        if ( node.isInternal( cursor ) )
        {
            assertKeyOrderAndSubtrees( cursor, range, level );
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

    private void assertKeyOrderAndSubtrees( PageCursor cursor, KeyRange<KEY> range, int level ) throws IOException
    {
        int keyCount = node.keyCount( cursor );
        long pageId = cursor.getCurrentPageId();
        KEY prev = layout.newKey();
        KeyRange<KEY> childRange;

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
                childRange = range.restrivtRight( readKey );
            }
            else
            {
                childRange = range.restrictLeft( prev ).restrivtRight( readKey );
            }
            checkSubtree( cursor, childRange, level+1 );
            cursor.next( pageId );

            layout.copyKey( readKey, prev );
            pos++;
        }

        // Check last child
        long child = node.childAt( cursor, pos );
        cursor.next( child );
        childRange = range.restrictLeft( prev );
        checkSubtree( cursor, childRange, level+1 );
        cursor.next( pageId );
    }

    private void assertKeyOrder( PageCursor cursor, KeyRange<KEY> range )
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

    private class KeyRange<KEY>
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
            if ( toExclusive != null )
            {
                return comparator.compare( key, toExclusive ) < 0;
            }
            return true;
        }

        KeyRange<KEY> restrictLeft( KEY left )
        {
            if ( comparator.compare( fromInclusive, left ) < 0 )
            {
                return new KeyRange<>( comparator, left, toExclusive, layout );
            }
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout );
        }

        KeyRange<KEY> restrivtRight( KEY right )
        {
            if ( comparator.compare( toExclusive, right ) > 0 )
            {
                return new KeyRange<>( comparator, fromInclusive, right, layout );
            }
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout );
        }
    }
}
