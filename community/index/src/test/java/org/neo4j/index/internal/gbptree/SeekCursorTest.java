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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;

public class SeekCursorTest
{
    private static final int PAGE_SIZE = 256;
    private static final LongSupplier generationSupplier = new LongSupplier()
    {
        @Override
        public long getAsLong()
        {
            return Generation.generation( stableGeneration, unstableGeneration );
        }
    };
    private static final Supplier<Root> failingRootCatchup = () ->
    {
        throw new AssertionError( "Should not happen" );
    };

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( PAGE_SIZE, layout );
    private final InternalTreeLogic<MutableLong,MutableLong> treeLogic = new InternalTreeLogic<>( id, node, layout );
    private final StructurePropagation<MutableLong> structurePropagation =
            new StructurePropagation<>( layout.newKey(), layout.newKey(), layout.newKey() );
    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
    private final int maxKeyCount = node.leafMaxKeyCount();

    private final MutableLong insertKey = layout.newKey();
    private final MutableLong insertValue = layout.newValue();

    private final MutableLong readKey = layout.newKey();

    private final MutableLong from = layout.newKey();
    private final MutableLong to = layout.newKey();

    private static long stableGeneration = GenerationSafePointer.MIN_GENERATION;
    private static long unstableGeneration = stableGeneration + 1;

    private long rootId;
    private int numberOfRootSplits;

    @Before
    public void setUp() throws IOException
    {
        cursor.next( id.acquireNewId( stableGeneration, unstableGeneration ) );
        TreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        updateRoot();
    }

    private void updateRoot()
    {
        rootId = cursor.getCurrentPageId();
        treeLogic.initialize( cursor );
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInBeginningOfSingleLeafBackwards() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int fromInclusive = maxKeyCount / 2;
        int toExclusive = -1;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInEndOfSingleLeafBackwards() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int fromInclusive = this.maxKeyCount - 1;
        int toExclusive = maxKeyCount / 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInMiddleOfSingleLeafBackwards() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            append( i );
        }
        int middle = maxKeyCount / 2;
        int fromInclusive = (middle + maxKeyCount) / 2;
        int toExclusive = middle / 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        long left = createRightSibling( cursor );
        while ( i < maxKeyCount * 2 )
        {
            append( i );
            i++;
        }
        cursor.next( left );

        int fromInclusive = 0;
        int toExclusive = maxKeyCount * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesSpanningTwoLeavesBackwards() throws Exception
    {
        // GIVEN
        int i = 0;
        while ( i < maxKeyCount )
        {
            append( i );
            i++;
        }
        createRightSibling( cursor );
        while ( i < maxKeyCount * 2 )
        {
            append( i );
            i++;
        }

        int fromInclusive = maxKeyCount * 2 - 1;
        int toExclusive = -1;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        long left = cursor.getCurrentPageId();
        while ( i < maxKeyCount * 2 )
        {
            if ( i == maxKeyCount )
            {
                createRightSibling( cursor );
            }
            append( i );
            i++;
        }
        cursor.next( left );

        int fromInclusive = maxKeyCount;
        int toExclusive = maxKeyCount * 2;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesOnSecondLeafWhenStartingFromFirstLeafBackwards() throws Exception
    {
        // GIVEN
        int i = 0;
        while ( i < maxKeyCount * 2 )
        {
            if ( i == maxKeyCount )
            {
                createRightSibling( cursor );
            }
            append( i );
            i++;
        }

        int fromInclusive = maxKeyCount - 1;
        int toExclusive = -1;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustNotContinueToSecondLeafAfterFindingEndOfRangeInFirst() throws Exception
    {
        AtomicBoolean nextCalled = new AtomicBoolean();
        PageCursor pageCursorSpy = new DelegatingPageCursor( cursor )
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
        while ( i < maxKeyCount * 2 )
        {
            if ( i == maxKeyCount )
            {
                createRightSibling( pageCursorSpy );
            }
            append( i );
            i++;
        }

        int fromInclusive = maxKeyCount * 2 - 1;
        int toExclusive = maxKeyCount;

        // Reset
        nextCalled.set( false );

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, pageCursorSpy ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
        assertFalse( "Cursor continued to next leaf even though end of range is within first leaf", nextCalled.get() );
    }

    @Test
    public void mustFindKeysWhenGivenRangeStartingOutsideStartOfData() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i );
        }

        long expectedkey = 0;
        try ( SeekCursor<MutableLong,MutableLong> seekCursor = seekCursor( -1, maxKeyCount - 1 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedkey );
                expectedkey++;
            }
        }
        assertEquals( expectedkey, maxKeyCount - 1 );
    }

    @Test
    public void mustFindKeysWhenGivenRangeStartingOutsideStartOfDataBackwards() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i );
        }

        long expectedkey = maxKeyCount - 1;
        try ( SeekCursor<MutableLong,MutableLong> seekCursor = seekCursor( maxKeyCount, 0 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedkey );
                expectedkey--;
            }
        }
        assertEquals( expectedkey, 0 );
    }

    @Test
    public void mustFindKeysWhenGivenRangeEndingOutsideEndOfData() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i );
        }

        long expectedkey = 0;
        try ( SeekCursor<MutableLong,MutableLong> seekCursor = seekCursor( 0, maxKeyCount + 1 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedkey );
                expectedkey++;
            }
        }
        assertEquals( expectedkey, maxKeyCount );
    }

    @Test
    public void mustFindKeysWhenGivenRangeEndingOutsideEndOfDataBackwards() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i );
        }

        long expectedkey = maxKeyCount - 1;
        try ( SeekCursor<MutableLong,MutableLong> seekCursor = seekCursor( maxKeyCount - 1, -2 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedkey );
                expectedkey--;
            }
        }
        assertEquals( expectedkey, -1 );
    }

    @Test
    public void mustStartReadingFromCorrectLeafWhenRangeStartWithKeyEqualToPrimKey() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount + 1; i++ )
        {
            insert( i );
        }
        MutableLong primKey = layout.newKey();
        node.keyAt( cursor, primKey, 0 );
        long expectedNext = primKey.longValue();
        long rightChild = GenerationSafePointerPair.pointer( node.childAt( cursor, 1, stableGeneration,
                unstableGeneration ) );

        // when
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( expectedNext, maxKeyCount + 1 ) )
        {
            assertEquals( rightChild, cursor.getCurrentPageId() );
            while ( seek.next() )
            {
                assertKeyAndValue( seek, expectedNext );
                expectedNext++;
            }
        }

        // then
        assertEquals( maxKeyCount + 1, expectedNext );
    }

    @Test
    public void mustStartReadingFromCorrectLeafWhenRangeStartWithKeyEqualToPrimKeyBackwards() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount + 1; i++ )
        {
            insert( i );
        }
        MutableLong primKey = layout.newKey();
        node.keyAt( cursor, primKey, 0 );
        long expectedNext = primKey.longValue();
        long rightChild = GenerationSafePointerPair.pointer( node.childAt( cursor, 1, stableGeneration,
                unstableGeneration ) );

        // when
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( expectedNext, -1 ) )
        {
            assertEquals( rightChild, cursor.getCurrentPageId() );
            while ( seek.next() )
            {
                assertKeyAndValue( seek, expectedNext );
                expectedNext--;
            }
        }

        // then
        assertEquals( -1, expectedNext );
    }

    /* INSERT */

    @Test
    public void mustFindNewKeyInsertedAfterOfSeekPoint() throws Exception
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                assertKeyAndValue( cursor, readKeys );
                readKeys++;
            }

            // Seeker pauses and writer insert new key at the end of leaf
            append( middle );
            this.cursor.forceRetry();

            // Seeker continue
            while ( cursor.next() )
            {
                assertKeyAndValue( cursor, readKeys );
                readKeys++;
            }
            assertEquals( toExclusive, readKeys );
        }
    }

    @Test
    public void mustFindNewKeyInsertedAfterOfSeekPointBackwards() throws Exception
    {
        // GIVEN
        int middle = maxKeyCount / 2;
        for ( int i = 1; i <= middle; i++ )
        {
            append( i );
        }
        int fromInclusive = middle;
        int toExclusive = 0; // Will insert 0 later

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                assertKeyAndValue( cursor, middle - readKeys );
                readKeys++;
            }

            // Seeker pauses and writer insert new key at the end of leaf
            insertIn( 0, 0 );
            this.cursor.forceRetry();

            // Seeker continue
            while ( cursor.next() )
            {
                assertKeyAndValue( cursor, middle - readKeys );
                readKeys++;
            }
            assertEquals( toExclusive, middle - readKeys );
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key in position where seeker will read next
            long midInsert = expected.get( stopPoint ) - 1;
            insertIn( stopPoint, midInsert );
            expected.add( stopPoint, midInsert );
            this.cursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustFindKeyInsertedOnSeekPositionBackwards() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = maxKeyCount / 2;
        for ( int i = middle; i > 0; i-- )
        {
            long key = i * 2;
            insert( key, valueForKey( key ) );
            expected.add( key );
        }
        int fromInclusive = middle * 2;
        long toExclusive = 0;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key in position where seeker will read next
            long midInsert = expected.get( stopPoint ) + 1;
            insert( midInsert, valueForKey( midInsert ) );
            expected.add( stopPoint, midInsert );
            this.cursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustNotFindKeyInsertedBeforeOfSeekPoint() throws Exception
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key to the left of seekers next position
            long midInsert = expected.get( readKeys - 1 ) - 1;
            insertIn( stopPoint - 1, midInsert );
            this.cursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustNotFindKeyInsertedBeforeOfSeekPointBackwards() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        int middle = maxKeyCount / 2;
        for ( int i = middle; i > 0; i-- )
        {
            long key = i * 2;
            insert( key, valueForKey( key ) );
            expected.add( key );
        }
        int fromInclusive = middle * 2;
        long toExclusive = 0;

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key to the left of seekers next position
            long midInsert = expected.get( readKeys - 1 ) + 1;
            insert( midInsert, valueForKey( midInsert ) );
            this.cursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
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
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            int middle = maxKeyCount / 2;
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( (long) maxKeyCount );
            insert( maxKeyCount );

            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToRightBackwards() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        for ( int i = maxKeyCount; i > 0; i-- )
        {
            long key = i;
            insert( key );
            expected.add( key );
        }
        int fromInclusive = maxKeyCount;
        long toExclusive = -1; // We will add 0 later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            int middle = maxKeyCount / 2;
            int stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && seeker.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( seeker, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( 0L );
            insert( 0L );

            seekCursor.forceRetry();

            while ( seeker.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( seeker, key );
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
            insert( key );
            expected.add( key );
        }
        int fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            int middle = maxKeyCount / 2;
            int stopPoint = middle + (middle / 2);
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( (long) maxKeyCount );
            insert( maxKeyCount );
            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    @Test
    public void mustContinueToNextLeafWhenRangeIsSplitIntoRightLeafAndPosToLeftBackwards() throws Exception
    {
        // GIVEN
        List<Long> expected = new ArrayList<>();
        for ( int i = maxKeyCount; i > 0; i-- )
        {
            long key = i;
            insert( key );
            expected.add( key );
        }
        int fromInclusive = maxKeyCount;
        long toExclusive = -1; // We will add 0 later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            int middle = maxKeyCount / 2;
            int stopPoint = middle + (middle / 2);
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( 0L );
            insert( 0L );
            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( expected.size(), readKeys );
        }
    }

    /* REMOVE */

    @Test
    public void mustNotFindKeyRemovedInFrontOfSeeker() throws Exception
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [0 1 ... maxKeyCount-2]
            removeAtPos( maxKeyCount - 1 );
            this.cursor.forceRetry();

            while ( cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( maxKeyCount - 1, readKeys );
        }
    }

    @Test
    public void mustNotFindKeyRemovedInFrontOfSeekerBackwards() throws Exception
    {
        // GIVEN
        // [1 2 ... maxKeyCount]
        for ( int i = 1; i <= maxKeyCount; i++ )
        {
            insert( i );
        }
        int fromInclusive = maxKeyCount;
        int toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && seeker.next() )
            {
                assertKeyAndValue( seeker, maxKeyCount - readKeys );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [2 ... maxKeyCount]
            remove( 1 );
            seekCursor.forceRetry();

            while ( seeker.next() )
            {
                assertKeyAndValue( seeker, maxKeyCount - readKeys );
                readKeys++;
            }
            assertEquals( maxKeyCount - 1, readKeys );
        }
    }

    @Test
    public void mustFindKeyMovedPassedSeekerBecauseOfRemove() throws Exception
    {
        // GIVEN
        // [0 1 ... maxKeyCount-1]
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [1 ... maxKeyCount-1]
            removeAtPos( 0 );
            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key );
                readKeys++;
            }
            assertEquals( maxKeyCount, readKeys );
        }
    }

    @Test
    public void mustFindKeyMovedPassedSeekerBecauseOfRemoveBackwards() throws Exception
    {
        // GIVEN
        // [1 2... maxKeyCount]
        for ( int i = maxKeyCount; i > 0; i-- )
        {
            insert( i );
        }
        int fromInclusive = maxKeyCount;
        int toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                assertKeyAndValue( cursor, maxKeyCount - readKeys );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [1 ... maxKeyCount-1]
            remove( maxKeyCount );
            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                assertKeyAndValue( cursor, maxKeyCount - readKeys );
                readKeys++;
            }
            assertEquals( maxKeyCount, readKeys );
        }
    }

    @Test
    public void mustFindKeyMovedSeekerBecauseOfRemoveOfMostRecentReturnedKey() throws Exception
    {
        // GIVEN
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i );
        }
        int fromInclusive = 0;
        int toExclusive = maxKeyCount;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                assertKeyAndValue( cursor, readKeys );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            remove( readKeys - 1 );
            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                assertKeyAndValue( cursor, readKeys );
                readKeys++;
            }
            assertEquals( maxKeyCount, readKeys );
        }
    }

    @Test
    public void mustFindKeyMovedSeekerBecauseOfRemoveOfMostRecentReturnedKeyBackwards() throws Exception
    {
        // GIVEN
        for ( int i = maxKeyCount; i > 0; i-- )
        {
            insert( i );
        }
        int fromInclusive = maxKeyCount;
        int toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            int middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                assertKeyAndValue( cursor, maxKeyCount - readKeys );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            remove( maxKeyCount - readKeys + 1 );
            seekCursor.forceRetry();

            while ( cursor.next() )
            {
                assertKeyAndValue( cursor, maxKeyCount - readKeys );
                readKeys++;
            }
            assertEquals( maxKeyCount, readKeys );
        }
    }

    @Test
    public void mustRereadHeadersOnRetry() throws Exception
    {
        // GIVEN
        int keyCount = maxKeyCount - 1;
        insertKeysAndValues( keyCount );
        MutableLong from = layout.newKey();
        MutableLong to = layout.newKey();
        from.setValue( 2 );
        to.setValue( keyCount + 1 ); // +1 because we're adding one more down below

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = new SeekCursor<>( this.cursor,
                node, from, to, layout, stableGeneration, unstableGeneration, () -> 0L, failingRootCatchup,
                unstableGeneration ) )
        {
            // reading a couple of keys
            assertTrue( cursor.next() );
            assertEquals( 2, cursor.get().key().longValue() );
            assertTrue( cursor.next() );
            assertEquals( 3, cursor.get().key().longValue() );

            // and WHEN a change happens
            append( keyCount );
            this.cursor.forceRetry();

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

    /* REBALANCE (when rebalance is implemented) */

    @Test
    public void mustFindRangeWhenCompletelyRebalancedToTheRightBeforeCallToNext() throws Exception
    {
        // given
        long key = 10;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for ( long smallKey = 0; smallKey < 2; smallKey++ )
        {
            insert( smallKey );
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        readCursor.next( pointer( leftChild ) );
        int keyCount = TreeNode.keyCount( readCursor );
        node.keyAt( readCursor, from, keyCount - 1 );
        long fromInclusive = from.longValue();
        long toExclusive = fromInclusive + 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            triggerUnderflowAndSeekRange( seeker, seekCursor, fromInclusive, toExclusive, rightChild );
        }
    }

    @Test
    public void mustFindRangeWhenCompletelyRebalancedToTheRightBeforeCallToNextBackwards() throws Exception
    {
        // given
        long key = 10;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for ( long smallKey = 0; smallKey < 2; smallKey++ )
        {
            insert( smallKey );
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        readCursor.next( pointer( leftChild ) );
        int keyCount = TreeNode.keyCount( readCursor );
        node.keyAt( readCursor, from, keyCount - 1 );
        long fromInclusive = from.longValue();
        long toExclusive = fromInclusive - 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            triggerUnderflowAndSeekRange( seeker, seekCursor, fromInclusive, toExclusive, rightChild );
        }
    }

    @Test
    public void mustFindRangeWhenCompletelyRebalancedToTheRightAfterCallToNext() throws Exception
    {
        // given
        long key = 10;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for ( long smallKey = 0; smallKey < 2; smallKey++ )
        {
            insert( smallKey );
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        readCursor.next( pointer( leftChild ) );
        int keyCount = TreeNode.keyCount( readCursor );
        node.keyAt( readCursor, from, keyCount - 2 );
        node.keyAt( readCursor, to, keyCount - 1 );
        long fromInclusive = from.longValue();
        long toExclusive = to.longValue() + 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            seekRangeWithUnderflowMidSeek( seeker, seekCursor, fromInclusive, toExclusive, rightChild );
        }
    }

    @Test
    public void mustFindRangeWhenCompletelyRebalancedToTheRightAfterCallToNextBackwards() throws Exception
    {
        // given
        long key = 10;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        // ... enough keys in left child to be rebalanced to the right
        for ( long smallKey = 0; smallKey < 2; smallKey++ )
        {
            insert( smallKey );
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        readCursor.next( pointer( leftChild ) );
        int keyCount = TreeNode.keyCount( readCursor );
        node.keyAt( readCursor, from, keyCount - 1 );
        node.keyAt( readCursor, to, keyCount - 2 );
        long fromInclusive = from.longValue();
        long toExclusive = to.longValue() - 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            seekRangeWithUnderflowMidSeek( seeker, seekCursor, fromInclusive, toExclusive, rightChild );
        }
    }

    /* MERGE */

    @Test
    public void mustFindRangeWhenMergingFromCurrentSeekNode() throws Exception
    {
        // given
        long key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // from first key in left child
        readCursor.next( pointer( leftChild ) );
        node.keyAt( readCursor, from, 0 );
        long fromInclusive = from.longValue();
        long toExclusive = from.longValue() + 2;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            assertThat( seekCursor.getCurrentPageId(), is( leftChild ) );
            seekRangeWithUnderflowMidSeek( seeker, seekCursor, fromInclusive, toExclusive, rightChild );
            readCursor.next( rootId );
            assertTrue( TreeNode.isLeaf( readCursor ) );
        }
    }

    @Test
    public void mustFindRangeWhenMergingToCurrentSeekNode() throws Exception
    {
        // given
        long key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // from first key in left child
        readCursor.next( pointer( rightChild ) );
        int keyCount = TreeNode.keyCount( readCursor );
        long fromInclusive = keyAt( readCursor, keyCount - 3 );
        long toExclusive = keyAt( readCursor, keyCount - 1 );

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            assertThat( seekCursor.getCurrentPageId(), is( rightChild ) );
            seekRangeWithUnderflowMidSeek( seeker, seekCursor, fromInclusive, toExclusive, leftChild );
            readCursor.next( rootId );
            assertTrue( TreeNode.isLeaf( readCursor ) );
        }
    }

    @Test
    public void mustFindRangeWhenMergingToCurrentSeekNodeBackwards() throws Exception
    {
        // given
        long key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // from first key in left child
        readCursor.next( pointer( rightChild ) );
        int keyCount = TreeNode.keyCount( readCursor );
        long fromInclusive = keyAt( readCursor, keyCount - 1 );
        long toExclusive = keyAt( readCursor, keyCount - 3 );

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            assertThat( seekCursor.getCurrentPageId(), is( rightChild ) );
            seekRangeWithUnderflowMidSeek( seeker, seekCursor, fromInclusive, toExclusive, leftChild );
            readCursor.next( rootId );
            assertTrue( TreeNode.isLeaf( readCursor ) );
        }
    }

    @Test
    public void mustFindRangeWhenMergingFromCurrentSeekNodeBackwards() throws Exception
    {
        // given
        long key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( key );
            key++;
        }

        PageAwareByteArrayCursor readCursor = cursor.duplicate( rootId );
        readCursor.next();
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // from first key in left child
        readCursor.next( pointer( leftChild ) );
        node.keyAt( readCursor, from, 0 );
        long fromInclusive = from.longValue() + 2;
        long toExclusive = from.longValue();

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            assertThat( seekCursor.getCurrentPageId(), is( leftChild ) );
            seekRangeWithUnderflowMidSeek( seeker, seekCursor, fromInclusive, toExclusive, rightChild );
            readCursor.next( rootId );
            assertTrue( TreeNode.isLeaf( readCursor ) );
        }
    }

    /* POINTER GENERATION TESTING */

    @Test
    public void shouldRereadSiblingIfReadFailureCausedByConcurrentCheckpoint() throws Exception
    {
        // given
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, i, cursor ) )
        {
            // when right sibling gets an successor
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            insert( i, i * 10, duplicate );

            // then
            // we should not fail to read right sibling
            while ( seek.next() )
            {
                // ignore
            }
        }
    }

    @Test
    public void shouldFailOnSiblingReadFailureIfNotCausedByConcurrentCheckpoint() throws Exception
    {
        // given
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, i, cursor ) )
        {
            // when right sibling pointer is corrupt
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            long leftChild = childAt( duplicate, 0, stableGeneration, unstableGeneration );
            duplicate.next( leftChild );
            corruptGSPP( duplicate, TreeNode.BYTE_POS_RIGHTSIBLING );

            // even if we DO have a checkpoint
            checkpoint();

            // then
            // we should fail to read right sibling
            try
            {
                while ( seek.next() )
                {
                    // ignore
                }
                fail( "Expected to throw" );
            }
            catch ( TreeInconsistencyException e )
            {
                // Good
            }
        }
    }

    @Test
    public void shouldRereadSuccessorIfReadFailureCausedByCheckpointInLeaf() throws Exception
    {
        // given
        List<Long> expected = new ArrayList<>();
        List<Long> actual = new ArrayList<>();
        long i = 0L;
        for ( ; i < maxKeyCount / 2; i++ )
        {
            insert( i, i * 10 );
            expected.add( i );
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, maxKeyCount, cursor ) )
        {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            insert( i, i * 10, duplicate ); // Create successor of leaf
            expected.add( i );

            while ( seek.next() )
            {
                Hit<MutableLong,MutableLong> hit = seek.get();
                actual.add( hit.key().getValue() );
            }
        }

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldFailSuccessorIfReadFailureNotCausedByCheckpointInLeaf() throws Exception
    {
        // given
        long i = 0L;
        for ( ; i < maxKeyCount / 2; i++ )
        {
            insert( i, i * 10 );
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, maxKeyCount, cursor ) )
        {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            insert( i, i * 10, duplicate ); // Create successor of leaf

            // and corrupt successor pointer
            corruptGSPP( duplicate, TreeNode.BYTE_POS_SUCCESSOR );

            // then
            try
            {
                while ( seek.next() )
                {
                }
                fail( "Expected to throw" );
            }
            catch ( TreeInconsistencyException e )
            {
                // good
            }
        }
    }

    @Test
    public void shouldRereadSuccessorIfReadFailureCausedByCheckpointInInternal() throws Exception
    {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        // a checkpoint
        long oldRootId = rootId;
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();
        int keyCount = TreeNode.keyCount( cursor );

        // and update root with an insert in new generation
        while ( keyCount( rootId ) == keyCount )
        {
            insert( i, i * 10 );
            i++;
        }
        TreeNode.goTo( cursor, "root", rootId );
        long rightChild = childAt( cursor, 2, stableGeneration, unstableGeneration );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( oldRootId );
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor( pageCursorForSeeker );
        breadcrumbCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, breadcrumbCursor, oldStableGeneration, oldUnstableGeneration ) )
        {
            while ( seek.next() )
            {
            }
        }

        // then
        // make sure seek cursor went to successor of root node
        assertEquals( Arrays.asList( oldRootId, rootId, rightChild ), breadcrumbCursor.getBreadcrumbs() );
    }

    private int keyCount( long nodeId ) throws IOException
    {
        long prevId = cursor.getCurrentPageId();
        try
        {
            TreeNode.goTo( cursor, "supplied", nodeId );
            return TreeNode.keyCount( cursor );
        }
        finally
        {
            TreeNode.goTo( cursor, "prev", prevId );
        }
    }

    @Test
    public void shouldFailSuccessorIfReadFailureNotCausedByCheckpointInInternal() throws Exception
    {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();
        int keyCount = TreeNode.keyCount( cursor );

        // and update root with an insert in new generation
        while ( TreeNode.keyCount( cursor ) == keyCount )
        {
            insert( i, i * 10 );
            i++;
        }

        // and corrupt successor pointer
        cursor.next( rootId );
        corruptGSPP( cursor, TreeNode.BYTE_POS_SUCCESSOR );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        pageCursorForSeeker.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, pageCursorForSeeker, oldStableGeneration, oldUnstableGeneration ) )
        {
            fail( "Expected throw" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            // good
        }
    }

    @Test
    public void shouldRereadChildPointerIfReadFailureCausedByCheckpoint() throws Exception
    {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();

        // and an update to root with a child pointer in new generation
        insert( i, i * 10 );
        i++;
        long newRightChild = childAt( cursor, 1, stableGeneration, unstableGeneration );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor( pageCursorForSeeker );
        breadcrumbCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, breadcrumbCursor, oldStableGeneration, oldUnstableGeneration ) )
        {
            while ( seek.next() )
            {
            }
        }

        // then
        // make sure seek cursor went to successor of root node
        assertEquals( Arrays.asList( rootId, newRightChild ), breadcrumbCursor.getBreadcrumbs() );
    }

    @Test
    public void shouldFailChildPointerIfReadFailureNotCausedByCheckpoint() throws Exception
    {
        // given
        // a root with two leaves in old generation
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();

        // and update root with an insert in new generation
        insert( i, i * 10 );
        i++;

        // and corrupt successor pointer
        corruptGSPP( cursor, node.childOffset( 1 ) );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        pageCursorForSeeker.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, pageCursorForSeeker, oldStableGeneration, oldUnstableGeneration ) )
        {
            fail( "Expected throw" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            // good
        }
    }

    @Test
    public void shouldCatchupRootWhenRootNodeHasTooNewGeneration() throws Exception
    {
        // given
        long id = cursor.getCurrentPageId();
        long generation = TreeNode.generation( cursor );
        MutableBoolean triggered = new MutableBoolean( false );
        Supplier<Root> rootCatchup = () ->
        {
            triggered.setTrue();
            return new Root( id, generation );
        };

        // when
        try ( SeekCursor<MutableLong,MutableLong> seek = new SeekCursor<>( cursor, node, from, to, layout,
                stableGeneration, unstableGeneration, generationSupplier, rootCatchup, generation - 1 ) )
        {
            // do nothing
        }

        // then
        assertTrue( triggered.getValue() );
    }

    @Test
    public void shouldCatchupRootWhenNodeHasTooNewGenerationWhileTraversingDownTree() throws Exception
    {
        // given
        long generation = TreeNode.generation( cursor );
        MutableBoolean triggered = new MutableBoolean( false );

        // a newer leaf
        long leftChild = cursor.getCurrentPageId();
        TreeNode.initializeLeaf( cursor, stableGeneration + 1, unstableGeneration + 1 ); // A newer leaf
        cursor.next();

        // a root
        long rootId = cursor.getCurrentPageId();
        TreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
        long keyInRoot = 10L;
        insertKey.setValue( keyInRoot );
        node.insertKeyAt( cursor, insertKey, 0, 0 );
        TreeNode.setKeyCount( cursor, 1 );
        // with old pointer to child (simulating reuse of child node)
        node.setChildAt( cursor, leftChild, 0, stableGeneration, unstableGeneration );

        // a root catchup that records usage
        Supplier<Root> rootCatchup = () ->
        {
            try
            {
                triggered.setTrue();

                // and set child generation to match pointer
                cursor.next( leftChild );
                cursor.zapPage();
                TreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );

                cursor.next( rootId );
                return new Root( rootId, generation );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };

        // when
        from.setValue( 1L );
        to.setValue( 2L );
        try ( SeekCursor<MutableLong,MutableLong> seek = new SeekCursor<>( cursor, node, from, to, layout,
                stableGeneration, unstableGeneration, generationSupplier, rootCatchup, unstableGeneration ) )
        {
            // do nothing
        }

        // then
        assertTrue( triggered.getValue() );
    }

    @Test
    public void shouldCatchupRootWhenNodeHasTooNewGenerationWhileTraversingLeaves() throws Exception
    {
        // given
        MutableBoolean triggered = new MutableBoolean( false );

        // a newer right leaf
        long rightChild = cursor.getCurrentPageId();
        TreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        cursor.next();

        Supplier<Root> rootCatchup = () ->
        {
            try
            {
                // Use right child as new start over root to terminate test
                cursor.next( rightChild );
                triggered.setTrue();
                return new Root( cursor.getCurrentPageId(), TreeNode.generation( cursor ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };

        // a left leaf
        long leftChild = cursor.getCurrentPageId();
        TreeNode.initializeLeaf( cursor, stableGeneration - 1, unstableGeneration - 1 );
        // with an old pointer to right sibling
        TreeNode.setRightSibling( cursor, rightChild, stableGeneration - 1, unstableGeneration - 1 );
        cursor.next();

        // a root
        TreeNode.initializeInternal( cursor, stableGeneration - 1, unstableGeneration - 1 );
        long keyInRoot = 10L;
        insertKey.setValue( keyInRoot );
        node.insertKeyAt( cursor, insertKey, 0, 0 );
        TreeNode.setKeyCount( cursor, 1 );
        // with old pointer to child (simulating reuse of internal node)
        node.setChildAt( cursor, leftChild, 0, stableGeneration, unstableGeneration );

        // when
        from.setValue( 1L );
        to.setValue( 20L );
        try ( SeekCursor<MutableLong,MutableLong> seek = new SeekCursor<>( cursor, node, from, to, layout,
                stableGeneration - 1, unstableGeneration - 1, generationSupplier, rootCatchup, unstableGeneration ) )
        {
            while ( seek.next() )
            {
                seek.get();
            }
        }

        // then
        assertTrue( triggered.getValue() );
    }

    @Test
    public void shouldThrowTreeInconsistencyExceptionOnBadReadWithoutShouldRetryWhileTraversingTree() throws Exception
    {
        // GIVEN
        int keyCount = 10000;

        // WHEN
        cursor.setOffset( TreeNode.BYTE_POS_KEYCOUNT );
        cursor.putInt( keyCount ); // Bad key count

        // THEN
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, Long.MAX_VALUE ) )
        {
            // Do nothing
        }
        catch ( TreeInconsistencyException e )
        {
            assertThat( e.getMessage(), containsString( "keyCount:" + keyCount ) );
        }
    }

    @Test
    public void shouldThrowTreeInconsistencyExceptionOnBadReadWithoutShouldRetryWhileTraversingLeaves() throws Exception
    {
        // GIVEN
        // a root with two leaves in old generation
        int keyCount = 10000;
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }
        long rootId = cursor.getCurrentPageId();
        long leftChild = node.childAt( cursor, 0, stableGeneration, unstableGeneration );

        // WHEN
        PageCursorUtil.goTo( cursor, "test", GenerationSafePointerPair.pointer( leftChild ) );
        cursor.setOffset( TreeNode.BYTE_POS_KEYCOUNT );
        cursor.putInt( keyCount ); // Bad key count
        PageCursorUtil.goTo( cursor, "test", rootId );

        // THEN
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, Long.MAX_VALUE ) )
        {
            while ( seek.next() )
            {
                // Do nothing
            }
        }
        catch ( TreeInconsistencyException e )
        {
            assertThat( e.getMessage(), containsString( "keyCount:" + keyCount ) );
        }
    }

    private void triggerUnderflowAndSeekRange( SeekCursor<MutableLong,MutableLong> seeker,
            TestPageCursor seekCursor, long fromInclusive, long toExclusive, long rightChild ) throws IOException
    {
        // ... then seeker should still find range
        int stride = fromInclusive <= toExclusive ? 1 : -1;
        triggerUnderflowAndSeekRange( seeker, seekCursor, fromInclusive, toExclusive, rightChild, stride );
    }

    private void seekRangeWithUnderflowMidSeek( SeekCursor<MutableLong,MutableLong> seeker, TestPageCursor seekCursor,
            long fromInclusive, long toExclusive, long underflowNode ) throws IOException
    {
        // ... seeker has started seeking in range
        assertTrue( seeker.next() );
        assertThat( seeker.get().key().longValue(), is( fromInclusive ) );

        int stride = fromInclusive <= toExclusive ? 1 : -1;
        triggerUnderflowAndSeekRange( seeker, seekCursor, fromInclusive + stride, toExclusive, underflowNode, stride );
    }

    private void triggerUnderflowAndSeekRange( SeekCursor<MutableLong,MutableLong> seeker,
            TestPageCursor seekCursor, long fromInclusive, long toExclusive, long rightChild, int stride ) throws IOException
    {
        // ... rebalance happens before first call to next
        triggerUnderflow( rightChild );
        seekCursor.changed(); // ByteArrayPageCursor is not aware of should retry, so fake it here

        for ( long expected = fromInclusive; Long.compare( expected, toExclusive ) * stride < 0; expected += stride )
        {
            assertTrue( seeker.next() );
            assertThat( seeker.get().key().longValue(), is( expected ) );
        }
        assertFalse( seeker.next() );
    }

    private void triggerUnderflow( long nodeId ) throws IOException
    {
        PageCursor readCursor = cursor.duplicate( nodeId );
        readCursor.next();
        int underflowBoundary = (maxKeyCount + 1) / 2;
        int keyCount = TreeNode.keyCount( readCursor );
        long toRemove = keyAt( readCursor, 0 );
        while ( keyCount >= underflowBoundary )
        {
            remove( toRemove );
            toRemove++;
            keyCount--;
        }
    }

    private void checkpoint()
    {
        stableGeneration = unstableGeneration;
        unstableGeneration++;
    }

    private void newRootFromSplit( StructurePropagation<MutableLong> split ) throws IOException
    {
        assertTrue( split.hasRightKeyInsert );
        long rootId = id.acquireNewId( stableGeneration, unstableGeneration );
        cursor.next( rootId );
        TreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
        node.insertKeyAt( cursor, split.rightKey, 0, 0 );
        TreeNode.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.midChild, 0, stableGeneration, unstableGeneration );
        node.setChildAt( cursor, split.rightChild, 1, stableGeneration, unstableGeneration );
        split.hasRightKeyInsert = false;
        numberOfRootSplits++;
        updateRoot();
    }

    private void corruptGSPP( PageAwareByteArrayCursor duplicate, int offset )
    {
        int someBytes = duplicate.getInt( offset );
        duplicate.putInt( offset, ~someBytes );
        someBytes = duplicate.getInt( offset + GenerationSafePointer.SIZE );
        duplicate.putInt( offset + GenerationSafePointer.SIZE, ~someBytes );
    }

    private void insert( long key ) throws IOException
    {
        insert( key, valueForKey( key ) );
    }

    private void insert( long key, long value ) throws IOException
    {
        insert( key, value, cursor );
    }

    private void insert( long key, long value, PageCursor cursor ) throws IOException
    {
        insertKey.setValue( key );
        insertValue.setValue( value );
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, overwrite(), stableGeneration,
                unstableGeneration );
        handleAfterChange();
    }

    private void remove( long key ) throws IOException
    {
        insertKey.setValue( key );
        treeLogic.remove( cursor, structurePropagation, insertKey, insertValue, stableGeneration, unstableGeneration );
        handleAfterChange();
    }

    private void handleAfterChange() throws IOException
    {
        if ( structurePropagation.hasRightKeyInsert )
        {
            newRootFromSplit( structurePropagation );
        }
        if ( structurePropagation.hasMidChildUpdate )
        {
            structurePropagation.hasMidChildUpdate = false;
            updateRoot();
        }
    }

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive ) throws IOException
    {
        return seekCursor( fromInclusive, toExclusive, cursor );
    }

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive,
            PageCursor pageCursor ) throws IOException
    {
        return seekCursor( fromInclusive, toExclusive, pageCursor, stableGeneration, unstableGeneration );
    }

    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive,
            PageCursor pageCursor, long stableGeneration, long unstableGeneration ) throws IOException
    {
        from.setValue( fromInclusive );
        to.setValue( toExclusive );
        return new SeekCursor<>( pageCursor, node, from, to, layout, stableGeneration, unstableGeneration, generationSupplier,
                failingRootCatchup, unstableGeneration );
    }

    /**
     * Create a right sibling to node pointed to by cursor. Leave cursor on new right sibling when done,
     * and return id of left sibling.
     */
    private long createRightSibling( PageCursor pageCursor ) throws IOException
    {
        long left = pageCursor.getCurrentPageId();
        long right = left + 1;

        TreeNode.setRightSibling( pageCursor, right, stableGeneration, unstableGeneration );

        pageCursor.next( right );
        TreeNode.initializeLeaf( pageCursor, stableGeneration, unstableGeneration );
        TreeNode.setLeftSibling( pageCursor, left, stableGeneration, unstableGeneration );
        return left;
    }

    private static void assertRangeInSingleLeaf( int fromInclusive, int toExclusive,
            SeekCursor<MutableLong,MutableLong> cursor ) throws IOException
    {
        int stride = fromInclusive <= toExclusive ? 1 : -1;
        long expectedKey = fromInclusive;
        while ( cursor.next() )
        {
            assertKeyAndValue( cursor, expectedKey );
            expectedKey += stride;
        }
        assertEquals( toExclusive, expectedKey );
    }

    private static void assertKeyAndValue( SeekCursor<MutableLong,MutableLong> cursor, long expectedKey )
    {
        assertKeyAndValue( cursor, expectedKey, valueForKey( expectedKey ) );
    }

    private static void assertKeyAndValue( SeekCursor<MutableLong,MutableLong> cursor,
            long expectedKey, long expectedValue )
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
            insertKey.setValue( i );
            insertValue.setValue( valueForKey( i ) );
            append( i );
        }
    }

    private void append( long k )
    {
        int keyCount = TreeNode.keyCount( cursor );
        insertKey.setValue( k );
        insertValue.setValue( valueForKey( k ) );
        node.insertKeyAt( cursor, insertKey, keyCount, keyCount );
        node.insertValueAt( cursor, insertValue, keyCount, keyCount );
        TreeNode.setKeyCount( cursor, keyCount + 1 );
    }

    private void insertIn( int pos, long k )
    {
        int keyCount = TreeNode.keyCount( cursor );
        if ( keyCount + 1 > maxKeyCount )
        {
            throw new IllegalStateException( "Can not insert another key in current node" );
        }
        insertKey.setValue( k );
        insertValue.setValue( valueForKey( k ) );
        node.insertKeyAt( cursor, insertKey, pos, keyCount );
        node.insertValueAt( cursor, insertValue, pos, keyCount );
        TreeNode.setKeyCount( cursor, keyCount + 1 );
    }

    private void removeAtPos( int pos )
    {
        int keyCount = TreeNode.keyCount( cursor );
        node.removeKeyAt( cursor, pos, keyCount );
        node.removeValueAt( cursor, pos, keyCount );
        TreeNode.setKeyCount( cursor, keyCount - 1 );
    }

    private static class BreadcrumbPageCursor extends DelegatingPageCursor
    {
        private final List<Long> breadcrumbs = new ArrayList<>();

        BreadcrumbPageCursor( PageCursor delegate )
        {
            super( delegate );
        }

        @Override
        public boolean next() throws IOException
        {
            boolean next = super.next();
            breadcrumbs.add( getCurrentPageId() );
            return next;
        }

        @Override
        public boolean next( long pageId ) throws IOException
        {
            boolean next = super.next( pageId );
            breadcrumbs.add( getCurrentPageId() );
            return next;
        }

        List<Long> getBreadcrumbs()
        {
            return breadcrumbs;
        }
    }

    private long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        return pointer( node.childAt( cursor, pos, stableGeneration, unstableGeneration ) );
    }

    private long keyAt( PageCursor cursor, int pos )
    {
        node.keyAt( cursor, readKey, pos );
        return readKey.longValue();
    }
}
