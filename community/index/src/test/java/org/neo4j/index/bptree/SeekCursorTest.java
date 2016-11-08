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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class SeekCursorTest
{
    private static final int PAGE_SIZE = 256;
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> treeNode = new TreeNode<>( PAGE_SIZE, layout );
    private final PageAwareByteArrayCursor delegate = new PageAwareByteArrayCursor( PAGE_SIZE );
    private final TestPageCursor pageCursor = new TestPageCursor( delegate );
    private final int maxKeyCount = treeNode.leafMaxKeyCount();
    private final byte[] tmp = new byte[PAGE_SIZE];

    private final MutableLong key = layout.newKey();
    private final MutableLong value = layout.newValue();
    private final MutableLong from = layout.newKey();
    private final MutableLong to = layout.newKey();


    @Before
    public void setUp() throws IOException
    {
        delegate.initialize();
        pageCursor.next( 0L );
        treeNode.initializeLeaf( pageCursor );
    }

    /* Singe thread */
    @Test
    public void mustFindEntriesWithinRangeInBeginningOfSingleLeaf() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount / 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, fromInclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInEndOfSingleLeaf() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }
        int fromInclusive = maxKeyCount / 2;
        int toExclusive = this.maxKeyCount;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, fromInclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInMiddleOfSingleLeaf() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }
        int middle = maxKeyCount / 2;
        int fromInclusive = middle / 2;
        int toExclusive = (middle + maxKeyCount) / 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, fromInclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesSpanningTwoLeaves() throws Exception
    {
        // GIVEN
        int i = 0;
        while ( i < maxKeyCount )
        {
            insert( i, i );
            i++;
        }
        long left = createRightSibling( pageCursor );
        while ( i < maxKeyCount * 2 )
        {
            insert( i, i );
            i++;
        }
        pageCursor.next( left );

        int fromInclusive = 0;
        int toExclusive = maxKeyCount * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, fromInclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesOnSecondLeafWhenStartingFromFirstLeaf() throws Exception
    {
        // GIVEN
        int i = 0;
        long left = pageCursor.getCurrentPageId();
        while ( i < maxKeyCount * 2 )
        {
            if ( i == maxKeyCount )
            {
                createRightSibling( pageCursor );
            }
            insert( i, i );
            i++;
        }
        pageCursor.next( left );

        int fromInclusive = maxKeyCount;
        int toExclusive = maxKeyCount * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, 0 ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustNotContinueToSecondLeafAfterFindingEndOfRangeInFirst() throws Exception
    {
        PageCursor pageCursorSpy = spy( pageCursor );
        // GIVEN
        int i = 0;
        long left = pageCursor.getCurrentPageId();
        while ( i < maxKeyCount * 2 )
        {
            if ( i == maxKeyCount )
            {
                createRightSibling( pageCursorSpy );
            }
            insert( i, i );
            i++;
        }
        pageCursorSpy.next( left );

        int fromInclusive = 0;
        int toExclusive = maxKeyCount - 1;

        reset( pageCursorSpy );

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, 0, pageCursorSpy ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
        verify( pageCursorSpy, never() ).next( anyLong() );
    }

    /* INSERT */
    // todo mustFindNewKeyInsertedRightOfSeekPoint
    // todo mustNotFindKeyInsertedLeftOfSeekPoint
    // todo mustFindKeyInsertedOnSeekPosition

    /* SPLIT */
    // todo mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToLeft
    // todo mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToRight

    /* REMOVE */
    // todo mustNotFindKeyRemovedRightOfSeekPoint
    // todo mustFindKeyMovedToLeftOfSeekPointBecauseOfRemove

    /* REBALANCE (when rebalance is implemented) */
    // todo mustFindRangeWhenCompletelyRebalancedToTheRightAndSeekPointOutsideRange
    // todo mustFindRangeWhenCompletelyRebalancedToTheRightAndSeekPointInRange
    // todo mustFindEntireRangeWhenPartlyRebalancedToTheRightAndSeekPointToTheLeft
    // todo mustFindEntireRangeWhenPartlyRebalancedToTheRightAndSeekPointToTheRight

    /* MERGE (when merge is implemented) */
    // todo mustFindRangeWhenRemoveInLeftSiblingCausesMerge
    // todo mustFindRangeWhenRemoveInRightSiblingCausesMerge
    // todo mustFindRangeWhenRemoveInSeekNodeCauseMergeWithLeft
    // todo mustFindRangeWhenRemoveInSeekNodeCauseMergeWithRight
    // todo mustFindRangeWhenRemoveTwoNodesToRightCauseMergeWithNodeOneToTheRight

    @SuppressWarnings( "unchecked" )
    @Test
    public void mustRereadHeadersOnRetry() throws Exception
    {
        // GIVEN
        int keyCount = 10;
        insertKeysAndValues( keyCount );
        MutableLong from = layout.newKey();
        MutableLong to = layout.newKey();
        from.setValue( 2 );
        to.setValue( keyCount + 1 ); // +1 because we're adding one more down below

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = new SeekCursor<>( pageCursor, key, value,
                treeNode, from, to, layout, 2, keyCount ) )
        {
            // reading a couple of keys
            assertTrue( cursor.next() );
            assertEquals( 2, cursor.get().key().longValue() );
            assertTrue( cursor.next() );
            assertEquals( 3, cursor.get().key().longValue() );

            // and WHEN a change happens
            insert( keyCount, valueForKey( keyCount ) );
            pageCursor.changed();

            // THEN at least keyCount should be re-read on next()
            assertTrue( cursor.next() );

            // and the new key should be found in the end as well
            assertEquals( 4, cursor.get().key().longValue() );
            long lastFoundKey = 4;
            while ( cursor.next() )
            {
                assertEquals( lastFoundKey + 1, cursor.get().key().longValue() );
                lastFoundKey = cursor.get().key().longValue();
            }
            assertEquals( keyCount, lastFoundKey );
        }
    }

    /**
     * Create a right sibling to node pointed to by pageCursor. Leave cursor on new right sibling when done,
     * and return id of left sibling.
     */
    private long createRightSibling( PageCursor pageCursor ) throws IOException
    {
        long left = pageCursor.getCurrentPageId();
        long right = left + 1;

        treeNode.setRightSibling( pageCursor, right );

        pageCursor.next( right );
        treeNode.initializeLeaf( pageCursor );
        treeNode.setLeftSibling( pageCursor, left );
        return left;
    }

    private void assertRangeInSingleLeaf( int fromInclusive, int toExclusive,
            SeekCursor<MutableLong,MutableLong> cursor ) throws IOException
    {
        long expectedKey = fromInclusive;
        while ( cursor.next() )
        {
            assertKeyAndValue( cursor, expectedKey, expectedKey );
            expectedKey++;
        }
        assertEquals( toExclusive, expectedKey );
    }

    private void assertKeyAndValue( SeekCursor<MutableLong,MutableLong> cursor, long expectedKey, long expectedValue )
    {
        MutableLong foundKey = cursor.get().key();
        MutableLong foundValue = cursor.get().value();
        assertEquals( expectedKey, foundKey.longValue() );
        assertEquals( expectedValue, foundValue.longValue() );
    }

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive, int pos )
    {
        return seekCursor( fromInclusive, toExclusive, pos, pageCursor );
    }

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive, int pos,
            PageCursor pageCursor )
    {
        from.setValue( fromInclusive );
        to.setValue( toExclusive );
        return new SeekCursor<>( pageCursor, key, value, treeNode, from, to, layout, pos, maxKeyCount );
    }

    private static long valueForKey( long key )
    {
        return key * 10;
    }

    private void insertKeysAndValues( int keyCount )
    {
        for ( int i = 0; i < keyCount; i++ )
        {
            key.setValue( i );
            value.setValue( valueForKey( i ) );
            insert( i, valueForKey( i ) );
        }
    }

    private void insert( long k, long v )
    {
        int keyCount = treeNode.keyCount( pageCursor );
        key.setValue( k );
        value.setValue( v );
        treeNode.insertKeyAt( pageCursor, key, keyCount, keyCount, tmp );
        treeNode.insertValueAt( pageCursor, value, keyCount, keyCount, tmp );
        treeNode.setKeyCount( pageCursor, keyCount + 1 );
    }
}
