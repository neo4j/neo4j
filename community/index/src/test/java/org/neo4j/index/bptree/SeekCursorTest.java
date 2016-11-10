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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    /* NO CONCURRENT INSERT */

    @Test
    public void mustFindEntriesWithinRangeInBeginningOfSingleLeaf() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount / 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
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
            append( i );
        }
        int fromInclusive = maxKeyCount / 2;
        int toExclusive = this.maxKeyCount;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
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
            append( i );
        }
        int middle = maxKeyCount / 2;
        int fromInclusive = middle / 2;
        int toExclusive = (middle + maxKeyCount) / 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
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
            append( i );
            i++;
        }
        long left = createRightSibling( pageCursor );
        while ( i < maxKeyCount * 2 )
        {
            append( i );
            i++;
        }
        pageCursor.next( left );

        int fromInclusive = 0;
        int toExclusive = maxKeyCount * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive,maxKeyCount ) )
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
            append( i );
            i++;
        }
        pageCursor.next( left );

        int fromInclusive = maxKeyCount;
        int toExclusive = maxKeyCount * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, 0, maxKeyCount ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustNotContinueToSecondLeafAfterFindingEndOfRangeInFirst() throws Exception
    {
        AtomicBoolean nextCalled = new AtomicBoolean();
        PageCursor pageCursorSpy = new DelegatingPageCursor( pageCursor )
        {
            @Override
            public boolean next( long pageId ) throws IOException
            {
                nextCalled.set( true );
                return super.next( pageId );
            }
        };

        // GIVEN
        int i = 0;
        long left = pageCursor.getCurrentPageId();
        while ( i < maxKeyCount * 2 )
        {
            if ( i == maxKeyCount )
            {
                createRightSibling( pageCursorSpy );
            }
            append( i );
            i++;
        }
        pageCursorSpy.next( left );

        int fromInclusive = 0;
        int toExclusive = maxKeyCount - 1;

        // Reset
        nextCalled.set( false );

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, 0, pageCursorSpy, maxKeyCount ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
        assertFalse( "Cursor continued to next leaf even though end of range is within first leaf", nextCalled.get() );
    }

    /* INSERT */

    @Test
    public void mustFindNewKeyInsertedRightOfSeekPoint() throws Exception
    {
        // GIVEN
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < middle; i++ )
        {
            append( i );
        }
        int fromInclusive = 0;
        int toExclusive = middle + 1; // Will insert middle later

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, middle ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                assertKeyAndValue( cursor, readKeys, readKeys );
                readKeys++;
            }

            // Seeker pauses and writer insert new key at the end of leaf
            append( middle );
            pageCursor.changed();

            // Seeker continue
            while ( cursor.next() )
            {
                assertKeyAndValue( cursor, readKeys, readKeys );
                readKeys++;
            }
            assertEquals( toExclusive, readKeys );
        }
    }

    @Test
    public void mustFindKeyInsertedOnSeekPosition() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < middle; i++ )
        {
            long key = i * 2;
            append( key );
            expected.add( key );
        }
        int fromInclusive = 0;
        long toExclusive = middle * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, middle ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key in position where seeker will read next
            long midInsert = (stopPoint * 2) - 1;
            insertIn( stopPoint, midInsert );
            expected.add( readKeys, midInsert );
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustNotFindKeyInsertedLeftOfSeekPoint() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < middle; i++ )
        {
            long key = i * 2;
            append( key );
            expected.add( key );
        }
        int fromInclusive = 0;
        long toExclusive = middle * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, middle ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key to the left of seekers next position
            long midInsert = ((stopPoint - 1) * 2) - 1;
            insertIn( stopPoint - 1, midInsert );
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    /* INSERT INTO SPLIT */

    @Test
    public void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToLeft() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i;
            append( key );
            expected.add( key );
        }
        int fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
        {
            int middle = maxKeyCount / 2;
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( (long) maxKeyCount );

            // Add rightmost keys to right sibling
            long left = createRightSibling( pageCursor );
            for ( int i = middle; i <= maxKeyCount; i++ )
            {
                Long key = expected.get( i );
                append( key );
            }
            // Update keycount in left sibling
            pageCursor.next( left );
            treeNode.setKeyCount( pageCursor, middle );
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToRight() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i;
            append( key );
            expected.add( key );
        }
        int fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
        {
            int middle = maxKeyCount / 2;
            int stopPoint = middle + (middle / 2);
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( (long) maxKeyCount );

            // Add rightmost keys to right sibling
            long left = createRightSibling( pageCursor );
            for ( int i = middle; i <= maxKeyCount; i++ )
            {
                Long key = expected.get( i );
                append( key );
            }
            // Update keycount in left sibling
            pageCursor.next( left );
            treeNode.setKeyCount( pageCursor, middle );
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    /* REMOVE */

    @Test
    public void mustNotFindKeyRemovedRightOfSeekPoint() throws Exception
    {
        // GIVEN
        // [0 1 ... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount;


        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [0 1 ... maxKeyCount-2]
            remove( maxKeyCount - 1);
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( maxKeyCount - 1, readKeys );
        }
    }

    @Test
    public void mustFindKeyMovedToLeftOfSeekPointBecauseOfRemove() throws Exception
    {
        // GIVEN
        // [0 1 ... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount;


        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [1 ... maxKeyCount-1]
            remove( 0 );
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( maxKeyCount, readKeys );
        }
    }

    @Test
    public void mustFindKeyMovedToLeftOfSeekPointBecauseOfRemoveOfPreviouslyReturnedKey() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount;


        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor =
                      seekCursor( fromInclusive, toExclusive, fromInclusive, maxKeyCount ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            remove( middle - 1 );
            pageCursor.changed();

            while ( cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key, key );
                readKeys++;
            }
            assertEquals( maxKeyCount, readKeys );
        }
    }

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
            append( keyCount );
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

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive, int pos, int keyCount )
    {
        return seekCursor( fromInclusive, toExclusive, pos, pageCursor, keyCount );
    }

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive, int pos,
            PageCursor pageCursor, int keyCount )
    {
        from.setValue( fromInclusive );
        to.setValue( toExclusive );
        return new SeekCursor<>( pageCursor, key, value, treeNode, from, to, layout, pos, keyCount );
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
            append( i );
        }
    }

    private void append( long k )
    {
        int keyCount = treeNode.keyCount( pageCursor );
        key.setValue( k );
        value.setValue( valueForKey( k ) );
        treeNode.insertKeyAt( pageCursor, key, keyCount, keyCount, tmp );
        treeNode.insertValueAt( pageCursor, value, keyCount, keyCount, tmp );
        treeNode.setKeyCount( pageCursor, keyCount + 1 );
    }

    private void insertIn( int pos, long k )
    {
        int keyCount = treeNode.keyCount( pageCursor );
        if ( keyCount + 1 > maxKeyCount )
        {
            throw new IllegalStateException( "Can not insert another key in current node" );
        }
        key.setValue( k );
        value.setValue( valueForKey( k ) );
        treeNode.insertKeyAt( pageCursor, key, pos, keyCount, tmp );
        treeNode.insertValueAt( pageCursor, value, pos, keyCount, tmp );
        treeNode.setKeyCount( pageCursor, keyCount + 1 );
    }

    private void remove( int pos )
    {
        int keyCount = treeNode.keyCount( pageCursor );
        treeNode.removeKeyAt( pageCursor, pos, keyCount, tmp );
        treeNode.removeValueAt( pageCursor, pos, keyCount, tmp );
        treeNode.setKeyCount( pageCursor, keyCount - 1 );
    }
}
