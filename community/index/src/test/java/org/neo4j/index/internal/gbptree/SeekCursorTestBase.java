/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.SeekCursor.DEFAULT_MAX_READ_AHEAD;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;

@SuppressWarnings( "UnnecessaryLocalVariable" )
public abstract class SeekCursorTestBase<KEY, VALUE>
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
    private static final Consumer<Throwable> exceptionDecorator = t ->
    {
    };

    @Rule
    public final RandomRule random = new RandomRule();

    private TestLayout<KEY,VALUE> layout;
    private TreeNode<KEY,VALUE> node;
    private InternalTreeLogic<KEY,VALUE> treeLogic;
    private StructurePropagation<KEY> structurePropagation;

    private PageAwareByteArrayCursor cursor;
    private PageAwareByteArrayCursor utilCursor;
    private SimpleIdProvider id;

    private static long stableGeneration = GenerationSafePointer.MIN_GENERATION;
    private static long unstableGeneration = stableGeneration + 1;

    private long rootId;
    private int numberOfRootSplits;

    @Before
    public void setUp() throws IOException
    {
        cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
        utilCursor = cursor.duplicate();
        id = new SimpleIdProvider( cursor::duplicate );

        layout = getLayout();
        node = getTreeNode( PAGE_SIZE, layout );
        treeLogic = new InternalTreeLogic<>( id, node, layout );
        structurePropagation = new StructurePropagation<>( layout.newKey(), layout.newKey(), layout.newKey() );

        long firstPage = id.acquireNewId( stableGeneration, unstableGeneration );
        goTo( cursor, firstPage );
        goTo( utilCursor, firstPage );

        node.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        updateRoot();
    }

    abstract TestLayout<KEY,VALUE> getLayout();

    abstract TreeNode<KEY,VALUE> getTreeNode( int pageSize, TestLayout<KEY,VALUE> layout );

    private static void goTo( PageCursor cursor, long pageId ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "test", pointer( pageId ) );
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
        long lastSeed = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = lastSeed / 2;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInBeginningOfSingleLeafBackwards() throws Exception
    {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = maxKeyCount / 2;
        long toExclusive = -1;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInEndOfSingleLeaf() throws Exception
    {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = maxKeyCount / 2;
        long toExclusive = maxKeyCount;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInEndOfSingleLeafBackwards() throws Exception
    {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long fromInclusive = maxKeyCount - 1;
        long toExclusive = maxKeyCount / 2;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInMiddleOfSingleLeaf() throws Exception
    {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long middle = maxKeyCount / 2;
        long fromInclusive = middle / 2;
        long toExclusive = (middle + maxKeyCount) / 2;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesWithinRangeInMiddleOfSingleLeafBackwards() throws Exception
    {
        // GIVEN
        long maxKeyCount = fullLeaf();
        long middle = maxKeyCount / 2;
        long fromInclusive = (middle + maxKeyCount) / 2;
        long toExclusive = middle / 2;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesSpanningTwoLeaves() throws Exception
    {
        // GIVEN
        long i = fullLeaf();
        long left = createRightSibling( cursor );
        i = fullLeaf( i );
        cursor.next( left );

        long fromInclusive = 0;
        long toExclusive = i;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesSpanningTwoLeavesBackwards() throws Exception
    {
        // GIVEN
        long i = fullLeaf();
        createRightSibling( cursor );
        i = fullLeaf( i );

        long fromInclusive = i - 1;
        long toExclusive = -1;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            assertRangeInSingleLeaf( fromInclusive, toExclusive, cursor );
        }
    }

    @Test
    public void mustFindEntriesOnSecondLeafWhenStartingFromFirstLeaf() throws Exception
    {
        // GIVEN
        long i = fullLeaf();
        long left = createRightSibling( cursor );
        long j = fullLeaf( i );
        cursor.next( left );

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( i, j ) )
        {
            // THEN
            assertRangeInSingleLeaf( i, j, cursor );
        }
    }

    @Test
    public void mustFindEntriesOnSecondLeafWhenStartingFromFirstLeafBackwards() throws Exception
    {
        // GIVEN
        long leftKeyCount = fullLeaf();
        long left = createRightSibling( cursor );
        fullLeaf( leftKeyCount );
        cursor.next( left );

        long fromInclusive = leftKeyCount - 1;
        long toExclusive = -1;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        long i = fullLeaf();
        long left = createRightSibling( cursor );
        long j = fullLeaf( i );

        long fromInclusive = j - 1;
        long toExclusive = i;

        // Reset
        nextCalled.set( false );

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, pageCursorSpy ) )
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
        long maxKeyCount = fullLeaf();

        long expectedKey = 0;
        try ( SeekCursor<KEY,VALUE> seekCursor = seekCursor( -1, maxKeyCount - 1 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedKey );
                expectedKey++;
            }
        }
        assertEquals( expectedKey, maxKeyCount - 1 );
    }

    @Test
    public void mustFindKeysWhenGivenRangeStartingOutsideStartOfDataBackwards() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = maxKeyCount - 1;
        try ( SeekCursor<KEY,VALUE> seekCursor = seekCursor( maxKeyCount, 0 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedKey );
                expectedKey--;
            }
        }
        assertEquals( expectedKey, 0 );
    }

    @Test
    public void mustFindKeysWhenGivenRangeEndingOutsideEndOfData() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = 0;
        try ( SeekCursor<KEY,VALUE> seekCursor = seekCursor( 0, maxKeyCount + 1 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedKey );
                expectedKey++;
            }
        }
        assertEquals( expectedKey, maxKeyCount );
    }

    @Test
    public void mustFindKeysWhenGivenRangeEndingOutsideEndOfDataBackwards() throws Exception
    {
        // Given
        // [ 0 1... maxKeyCount-1]
        long maxKeyCount = fullLeaf();

        long expectedKey = maxKeyCount - 1;
        try ( SeekCursor<KEY,VALUE> seekCursor = seekCursor( maxKeyCount - 1, -2 ) )
        {
            while ( seekCursor.next() )
            {
                assertKeyAndValue( seekCursor, expectedKey );
                expectedKey--;
            }
        }
        assertEquals( expectedKey, -1 );
    }

    @Test
    public void mustStartReadingFromCorrectLeafWhenRangeStartWithKeyEqualToPrimKey() throws Exception
    {
        // given
        long lastSeed = rootWithTwoLeaves();
        KEY primKey = layout.newKey();
        node.keyAt( cursor, primKey, 0, INTERNAL );
        long expectedNext = getSeed( primKey );
        long rightChild = GenerationSafePointerPair.pointer( node.childAt( cursor, 1, stableGeneration,
                unstableGeneration ) );

        // when
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( expectedNext, lastSeed ) )
        {
            assertEquals( rightChild, cursor.getCurrentPageId() );
            while ( seek.next() )
            {
                assertKeyAndValue( seek, expectedNext );
                expectedNext++;
            }
        }

        // then
        assertEquals( lastSeed, expectedNext );
    }

    @Test
    public void mustStartReadingFromCorrectLeafWhenRangeStartWithKeyEqualToPrimKeyBackwards() throws Exception
    {
        // given
        rootWithTwoLeaves();
        KEY primKey = layout.newKey();
        node.keyAt( cursor, primKey, 0, INTERNAL );
        long expectedNext = getSeed( primKey );
        long rightChild = GenerationSafePointerPair.pointer( node.childAt( cursor, 1, stableGeneration,
                unstableGeneration ) );

        // when
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( expectedNext, -1 ) )
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

    @Test
    public void exactMatchInStableRoot() throws Exception
    {
        // given
        long maxKeyCount = fullLeaf();

        // when
        for ( long i = 0; i < maxKeyCount; i++ )
        {
            assertExactMatch( i );
        }
    }

    @Test
    public void exactMatchInLeaves() throws Exception
    {
        // given
        long lastSeed = rootWithTwoLeaves();

        // when
        for ( long i = 0; i < lastSeed; i++ )
        {
            assertExactMatch( i );
        }
    }

    private long rootWithTwoLeaves() throws IOException
    {
        long i = 0;
        for ( ; numberOfRootSplits < 1; i++ )
        {
            insert( i );
        }
        return i;
    }

    private void assertExactMatch( long i ) throws IOException
    {
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( i, i ) )
        {
            // then
            assertTrue( seeker.next() );
            assertEqualsKey( key( i ), seeker.get().key() );
            assertEqualsValue( value( i ), seeker.get().value() );
            assertFalse( seeker.next() );
        }
    }

    /* INSERT */

    @Test
    public void mustFindNewKeyInsertedAfterOfSeekPoint() throws Exception
    {
        // GIVEN
        int middle = 2;
        for ( int i = 0; i < middle; i++ )
        {
            append( i );
        }
        long fromInclusive = 0;
        long toExclusive = middle + 1; // Will insert middle later

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        int middle = 2;
        for ( int i = 1; i <= middle; i++ )
        {
            append( i );
        }
        long fromInclusive = middle;
        long toExclusive = 0; // Will insert 0 later

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        int middle = 2;
        for ( int i = 0; i < middle; i++ )
        {
            long key = i * 2;
            append( key );
            expected.add( key );
        }
        long fromInclusive = 0;
        long toExclusive = middle * 2;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        int middle = 2;
        for ( int i = middle; i > 0; i-- )
        {
            long key = i * 2;
            insert( key );
            expected.add( key );
        }
        long fromInclusive = middle * 2;
        long toExclusive = 0;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
            insert( midInsert );
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
        int middle = 2;
        for ( int i = 0; i < middle; i++ )
        {
            long key = i * 2;
            append( key );
            expected.add( key );
        }
        long fromInclusive = 0;
        long toExclusive = middle * 2;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
        int middle = 2;
        for ( int i = middle; i > 0; i-- )
        {
            long key = i * 2;
            insert( key );
            expected.add( key );
        }
        long fromInclusive = middle * 2;
        long toExclusive = 0;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
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
            insert( midInsert );
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
        long maxKeyCount = fullLeaf( expected );
        long fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            long middle = maxKeyCount / 2;
            long stopPoint = middle / 2;
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( maxKeyCount );
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
        long lastSeed = fullLeaf( 1, expected );
        Collections.reverse( expected ); // Because backwards
        long fromInclusive = lastSeed - 1;
        long toExclusive = -1; // We will add 0 later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            long middle = lastSeed / 2;
            long stopPoint = middle / 2;
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
        long maxKeyCount = fullLeaf( expected );
        long fromInclusive = 0;
        long toExclusive = maxKeyCount + 1; // We will add maxKeyCount later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            long middle = maxKeyCount / 2;
            long stopPoint = middle + (middle / 2);
            int readKeys = 0;
            while ( readKeys < stopPoint && cursor.next() )
            {
                long key = expected.get( readKeys );
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer insert new key which causes a split
            expected.add( maxKeyCount );
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
        long lastSeed = fullLeaf( 1, expected );
        Collections.reverse( expected ); // Because backwards
        long fromInclusive = lastSeed - 1;
        long toExclusive = -1; // We will add 0 later

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            long middle = lastSeed / 2;
            long stopPoint = middle + (middle / 2);
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
        long maxKeyCount = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = maxKeyCount;

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive ) )
        {
            // THEN
            long middle = maxKeyCount / 2;
            int readKeys = 0;
            while ( readKeys < middle && cursor.next() )
            {
                long key = readKeys;
                assertKeyAndValue( cursor, key );
                readKeys++;
            }

            // Seeker pauses and writer remove rightmost key
            // [0 1 ... maxKeyCount-2]
            removeAtPos( (int) maxKeyCount - 1 );
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

    private long fullLeaf( List<Long> expectedSeeds )
    {
        return fullLeaf( 0, expectedSeeds );
    }

    private long fullLeaf( long firstSeed )
    {
        return fullLeaf( firstSeed, new ArrayList<>() );
    }

    private long fullLeaf( long firstSeed, List<Long> expectedSeeds )
    {
        int keyCount = 0;
        KEY key = key( firstSeed + keyCount );
        VALUE value = value( firstSeed + keyCount );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == TreeNode.Overflow.NO )
        {
            node.insertKeyValueAt( cursor, key, value, keyCount, keyCount );
            expectedSeeds.add( firstSeed + keyCount );
            keyCount++;
            key = key( firstSeed + keyCount );
            value = value( firstSeed + keyCount );
        }
        TreeNode.setKeyCount( cursor, keyCount );
        return firstSeed + keyCount;
    }

    /**
     * @return next seed to be inserted
     */
    private long fullLeaf()
    {
        return fullLeaf( 0 );
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private long getSeed( KEY primKey )
    {
        return layout.keySeed( primKey );
    }

    @Test
    public void mustNotFindKeyRemovedInFrontOfSeekerBackwards() throws Exception
    {
        // GIVEN
        // [1 2 ... maxKeyCount]
        long lastSeed = fullLeaf( 1 );
        long maxKeyCount = lastSeed - 1;
        long fromInclusive = maxKeyCount;
        long toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            long middle = maxKeyCount / 2;
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
        long maxKeyCount = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = maxKeyCount;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            long middle = maxKeyCount / 2;
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
        long lastSeed = fullLeaf( 1 );
        long maxKeyCount = lastSeed - 1;
        long fromInclusive = maxKeyCount;
        long toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            long middle = maxKeyCount / 2;
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
        long maxKeyCount = fullLeaf();
        long fromInclusive = 0;
        long toExclusive = maxKeyCount;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            long middle = maxKeyCount / 2;
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
        long i = fullLeaf( 1 );
        long maxKeyCount = i - 1;
        long fromInclusive = i - 1;
        long toExclusive = 0;

        // WHEN
        PageAwareByteArrayCursor seekCursor = cursor.duplicate();
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> cursor = seekCursor( fromInclusive, toExclusive, seekCursor ) )
        {
            // THEN
            long middle = maxKeyCount / 2;
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
        int keyCount = 2;
        insertKeysAndValues( keyCount );
        KEY from = key( 0 );
        KEY to = key( keyCount + 1 ); // +1 because we're adding one more down below

        // WHEN
        try ( SeekCursor<KEY,VALUE> cursor = new SeekCursor<>( this.cursor,
                node, from, to, layout, stableGeneration, unstableGeneration, () -> 0L, failingRootCatchup,
                unstableGeneration, exceptionDecorator, 1 ) )
        {
            // reading a couple of keys
            assertTrue( cursor.next() );
            assertEqualsKey( key( 0 ), cursor.get().key() );

            // and WHEN a change happens
            append( keyCount );
            this.cursor.forceRetry();

            // THEN at least keyCount should be re-read on next()
            assertTrue( cursor.next() );

            // and the new key should be found in the end as well
            assertEqualsKey( key( 1 ), cursor.get().key() );
            long lastFoundKey = 1;
            while ( cursor.next() )
            {
                assertEqualsKey( key( lastFoundKey + 1 ), cursor.get().key() );
                lastFoundKey = getSeed( cursor.get().key() );
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
        KEY readKey = layout.newKey();
        node.keyAt( readCursor, readKey, keyCount - 1, LEAF );
        long fromInclusive = getSeed( readKey );
        long toExclusive = fromInclusive + 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        KEY from = layout.newKey();
        node.keyAt( readCursor, from, keyCount - 1, LEAF );
        long fromInclusive = getSeed( from );
        long toExclusive = fromInclusive - 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        KEY from = layout.newKey();
        KEY to = layout.newKey();
        node.keyAt( readCursor, from, keyCount - 2, LEAF );
        node.keyAt( readCursor, to, keyCount - 1, LEAF );
        long fromInclusive = getSeed( from );
        long toExclusive = getSeed( to ) + 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        KEY from = layout.newKey();
        KEY to = layout.newKey();
        node.keyAt( readCursor, from, keyCount - 1, LEAF );
        node.keyAt( readCursor, to, keyCount - 2, LEAF );
        long fromInclusive = getSeed( from );
        long toExclusive = getSeed( to ) - 1;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        KEY from = layout.newKey();
        node.keyAt( readCursor, from, 0, LEAF );
        long fromInclusive = getSeed( from );
        long toExclusive = getSeed( from ) + 2;

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        long fromInclusive = keyAt( readCursor, keyCount - 3, LEAF );
        long toExclusive = keyAt( readCursor, keyCount - 1, LEAF );

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        long fromInclusive = keyAt( readCursor, keyCount - 1, LEAF );
        long toExclusive = keyAt( readCursor, keyCount - 3, LEAF );

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
        KEY from = layout.newKey();
        node.keyAt( readCursor, from, 0, LEAF );
        long fromInclusive = getSeed( from ) + 2;
        long toExclusive = getSeed( from );

        // when
        TestPageCursor seekCursor = new TestPageCursor( cursor.duplicate( rootId ) );
        seekCursor.next();
        try ( SeekCursor<KEY,VALUE> seeker = seekCursor( fromInclusive, toExclusive, seekCursor ) )
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
            insert( i );
            i++;
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( 0L, i, cursor ) )
        {
            // when right sibling gets an successor
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            insert( i, i * 10, duplicate );

            // then
            // we should not fail to read right sibling
            //noinspection StatementWithEmptyBody
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
            insert( i );
            i++;
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( 0L, i, cursor ) )
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
                //noinspection StatementWithEmptyBody
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
        for ( ; i < 2; i++ )
        {
            insert( i );
            expected.add( i );
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( 0L, 5, cursor ) )
        {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            insert( i, i, duplicate ); // Create successor of leaf
            expected.add( i );

            while ( seek.next() )
            {
                Hit<KEY,VALUE> hit = seek.get();
                actual.add( getSeed( hit.key() ) );
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
        for ( ; i < 2; i++ )
        {
            insert( i );
        }

        long currentNode = cursor.getCurrentPageId();
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( 0L, 5, cursor ) )
        {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( currentNode );
            duplicate.next();
            insert( i, i, duplicate ); // Create successor of leaf

            // and corrupt successor pointer
            corruptGSPP( duplicate, TreeNode.BYTE_POS_SUCCESSOR );

            // then
            try
            {
                //noinspection StatementWithEmptyBody
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
            insert( i );
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
            insert( i );
            i++;
        }
        TreeNode.goTo( cursor, "root", rootId );
        long rightChild = childAt( cursor, 2, stableGeneration, unstableGeneration );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( oldRootId );
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor( pageCursorForSeeker );
        breadcrumbCursor.next();
        try ( SeekCursor<KEY,VALUE> seek = seekCursor(
                i, i + 1, breadcrumbCursor, oldStableGeneration, oldUnstableGeneration ) )
        {
            //noinspection StatementWithEmptyBody
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
            insert( i );
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
            insert( i );
            i++;
        }

        // and corrupt successor pointer
        cursor.next( oldRootId );
        corruptGSPP( cursor, TreeNode.BYTE_POS_SUCCESSOR );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( oldRootId );
        pageCursorForSeeker.next();
        try ( SeekCursor<KEY,VALUE> ignored = seekCursor(
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
            insert( i );
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();

        // and an update to root with a child pointer in new generation
        insert( i );
        i++;
        long newRightChild = childAt( cursor, 1, stableGeneration, unstableGeneration );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor( pageCursorForSeeker );
        breadcrumbCursor.next();
        try ( SeekCursor<KEY,VALUE> seek = seekCursor(
                i, i + 1, breadcrumbCursor, oldStableGeneration, oldUnstableGeneration ) )
        {
            //noinspection StatementWithEmptyBody
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
            insert( i );
            i++;
        }

        // a checkpoint
        long oldStableGeneration = stableGeneration;
        long oldUnstableGeneration = unstableGeneration;
        checkpoint();

        // and update root with an insert in new generation
        insert( i );
        i++;

        // and corrupt successor pointer
        corruptGSPP( cursor, node.childOffset( 1 ) );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        pageCursorForSeeker.next();
        try ( SeekCursor<KEY,VALUE> ignored = seekCursor(
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
        //noinspection EmptyTryBlock
        try ( SeekCursor<KEY,VALUE> ignored = new SeekCursor<>( cursor, node, key( 0 ), key( 1 ), layout,
                stableGeneration, unstableGeneration, generationSupplier, rootCatchup, generation - 1,
                exceptionDecorator, 1 ) )
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
        long rightChild = 999; // We don't care

        // a newer leaf
        long leftChild = cursor.getCurrentPageId();
        node.initializeLeaf( cursor, stableGeneration + 1, unstableGeneration + 1 ); // A newer leaf
        cursor.next();

        // a root
        long rootId = cursor.getCurrentPageId();
        node.initializeInternal( cursor, stableGeneration, unstableGeneration );
        long keyInRoot = 10L;
        node.insertKeyAndRightChildAt( cursor, key( keyInRoot ), rightChild, 0, 0, stableGeneration, unstableGeneration );
        TreeNode.setKeyCount( cursor, 1 );
        // with old pointer to child (simulating reuse of child node)
        node.setChildAt( cursor, leftChild, 0, stableGeneration, unstableGeneration );

        // a root catchup that records usage
        Supplier<Root> rootCatchup = () ->
        {
            triggered.setTrue();

            // and set child generation to match pointer
            cursor.next( leftChild );
            cursor.zapPage();
            node.initializeLeaf( cursor, stableGeneration, unstableGeneration );

            cursor.next( rootId );
            return new Root( rootId, generation );
        };

        // when
        KEY from = key( 1L );
        KEY to = key( 2L );
        //noinspection EmptyTryBlock
        try ( SeekCursor<KEY,VALUE> ignored = new SeekCursor<>( cursor, node, from, to, layout,
                stableGeneration, unstableGeneration, generationSupplier, rootCatchup, unstableGeneration,
                exceptionDecorator, 1 ) )
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
        long oldRightChild = 666; // We don't care

        // a newer right leaf
        long rightChild = cursor.getCurrentPageId();
        node.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        cursor.next();

        Supplier<Root> rootCatchup = () ->
        {
            // Use right child as new start over root to terminate test
            cursor.next( rightChild );
            triggered.setTrue();
            return new Root( cursor.getCurrentPageId(), TreeNode.generation( cursor ) );
        };

        // a left leaf
        long leftChild = cursor.getCurrentPageId();
        node.initializeLeaf( cursor, stableGeneration - 1, unstableGeneration - 1 );
        // with an old pointer to right sibling
        TreeNode.setRightSibling( cursor, rightChild, stableGeneration - 1, unstableGeneration - 1 );
        cursor.next();

        // a root
        node.initializeInternal( cursor, stableGeneration - 1, unstableGeneration - 1 );
        long keyInRoot = 10L;
        node.insertKeyAndRightChildAt( cursor, key( keyInRoot ), oldRightChild, 0, 0, stableGeneration, unstableGeneration );
        TreeNode.setKeyCount( cursor, 1 );
        // with old pointer to child (simulating reuse of internal node)
        node.setChildAt( cursor, leftChild, 0, stableGeneration, unstableGeneration );

        // when
        KEY from = key( 1L );
        KEY to = key( 20L );
        try ( SeekCursor<KEY,VALUE> seek = new SeekCursor<>( cursor, node, from, to, layout,
                stableGeneration - 1, unstableGeneration - 1, generationSupplier, rootCatchup, unstableGeneration,
                exceptionDecorator, 1 ) )
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
        //noinspection EmptyTryBlock
        try ( SeekCursor<KEY,VALUE> ignored = seekCursor( 0L, Long.MAX_VALUE ) )
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
            insert( i );
            i++;
        }
        long rootId = cursor.getCurrentPageId();
        long leftChild = node.childAt( cursor, 0, stableGeneration, unstableGeneration );

        // WHEN
        goTo( cursor, leftChild );
        cursor.setOffset( TreeNode.BYTE_POS_KEYCOUNT );
        cursor.putInt( keyCount ); // Bad key count
        goTo( cursor, rootId );

        // THEN
        try ( SeekCursor<KEY,VALUE> seek = seekCursor( 0L, Long.MAX_VALUE ) )
        {
            //noinspection StatementWithEmptyBody
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

    private void triggerUnderflowAndSeekRange( SeekCursor<KEY,VALUE> seeker,
            TestPageCursor seekCursor, long fromInclusive, long toExclusive, long rightChild ) throws IOException
    {
        // ... then seeker should still find range
        int stride = fromInclusive <= toExclusive ? 1 : -1;
        triggerUnderflowAndSeekRange( seeker, seekCursor, fromInclusive, toExclusive, rightChild, stride );
    }

    private void seekRangeWithUnderflowMidSeek( SeekCursor<KEY,VALUE> seeker, TestPageCursor seekCursor,
            long fromInclusive, long toExclusive, long underflowNode ) throws IOException
    {
        // ... seeker has started seeking in range
        assertTrue( seeker.next() );
        assertThat( getSeed( seeker.get().key() ), is( fromInclusive ) );

        int stride = fromInclusive <= toExclusive ? 1 : -1;
        triggerUnderflowAndSeekRange( seeker, seekCursor, fromInclusive + stride, toExclusive, underflowNode, stride );
    }

    private void triggerUnderflowAndSeekRange( SeekCursor<KEY,VALUE> seeker,
            TestPageCursor seekCursor, long fromInclusive, long toExclusive, long rightChild, int stride ) throws IOException
    {
        // ... rebalance happens before first call to next
        triggerUnderflow( rightChild );
        seekCursor.changed(); // ByteArrayPageCursor is not aware of should retry, so fake it here

        for ( long expected = fromInclusive; Long.compare( expected, toExclusive ) * stride < 0; expected += stride )
        {
            assertTrue( seeker.next() );
            assertThat( getSeed( seeker.get().key() ), is( expected ) );
        }
        assertFalse( seeker.next() );
    }

    private void triggerUnderflow( long nodeId ) throws IOException
    {
        // On underflow keys will move from left to right
        // and key count of the right will increase.
        // We don't know if keys will move from nodeId to
        // right sibling or to nodeId from left sibling.
        // So we monitor both nodeId and rightSibling.
        PageCursor readCursor = cursor.duplicate( nodeId );
        readCursor.next();
        int midKeyCount = TreeNode.keyCount( readCursor );
        int prevKeyCount = midKeyCount + 1;

        PageCursor rightSiblingCursor = null;
        long rightSibling = TreeNode.rightSibling( readCursor, stableGeneration, unstableGeneration );
        int rightKeyCount = 0;
        int prevRightKeyCount = 1;
        boolean monitorRight = TreeNode.isNode( rightSibling );
        if ( monitorRight )
        {
            rightSiblingCursor = cursor.duplicate( GenerationSafePointerPair.pointer( rightSibling ) );
            rightSiblingCursor.next();
            rightKeyCount = TreeNode.keyCount( rightSiblingCursor );
            prevRightKeyCount = rightKeyCount + 1;
        }

        while ( midKeyCount < prevKeyCount && rightKeyCount <= prevRightKeyCount )
        {
            long toRemove = keyAt( readCursor, 0, LEAF );
            remove( toRemove );
            prevKeyCount = midKeyCount;
            midKeyCount = TreeNode.keyCount( readCursor );
            if ( monitorRight )
            {
                prevRightKeyCount = rightKeyCount;
                rightKeyCount = TreeNode.keyCount( rightSiblingCursor );
            }
        }
    }

    private void checkpoint()
    {
        stableGeneration = unstableGeneration;
        unstableGeneration++;
    }

    private void newRootFromSplit( StructurePropagation<KEY> split )
    {
        assertTrue( split.hasRightKeyInsert );
        long rootId = id.acquireNewId( stableGeneration, unstableGeneration );
        cursor.next( rootId );
        node.initializeInternal( cursor, stableGeneration, unstableGeneration );
        node.setChildAt( cursor, split.midChild, 0, stableGeneration, unstableGeneration );
        node.insertKeyAndRightChildAt( cursor, split.rightKey, split.rightChild, 0, 0, stableGeneration, unstableGeneration );
        TreeNode.setKeyCount( cursor, 1 );
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
        insert( key, key );
    }

    private void insert( long key, long value ) throws IOException
    {
        insert( key, value, cursor );
    }

    private void insert( long key, long value, PageCursor cursor ) throws IOException
    {
        treeLogic.insert( cursor, structurePropagation, key( key ), value( value ), overwrite(), stableGeneration,
                unstableGeneration );
        handleAfterChange();
    }

    private void remove( long key ) throws IOException
    {
        treeLogic.remove( cursor, structurePropagation, key( key ), layout.newValue(), stableGeneration, unstableGeneration );
        handleAfterChange();
    }

    private void handleAfterChange()
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

    private SeekCursor<KEY,VALUE> seekCursor( long fromInclusive, long toExclusive ) throws IOException
    {
        return seekCursor( fromInclusive, toExclusive, cursor );
    }

    private SeekCursor<KEY,VALUE> seekCursor( long fromInclusive, long toExclusive,
            PageCursor pageCursor ) throws IOException
    {
        return seekCursor( fromInclusive, toExclusive, pageCursor, stableGeneration, unstableGeneration );
    }

    private SeekCursor<KEY,VALUE> seekCursor( long fromInclusive, long toExclusive,
            PageCursor pageCursor, long stableGeneration, long unstableGeneration ) throws IOException
    {
        return new SeekCursor<>( pageCursor, node, key( fromInclusive ), key( toExclusive ), layout, stableGeneration, unstableGeneration,
                generationSupplier, failingRootCatchup, unstableGeneration , exceptionDecorator, random.nextInt( 1, DEFAULT_MAX_READ_AHEAD ) );
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
        node.initializeLeaf( pageCursor, stableGeneration, unstableGeneration );
        TreeNode.setLeftSibling( pageCursor, left, stableGeneration, unstableGeneration );
        return left;
    }

    private void assertRangeInSingleLeaf( long fromInclusive, long toExclusive,
            SeekCursor<KEY,VALUE> cursor ) throws IOException
    {
        int stride = fromInclusive <= toExclusive ? 1 : -1;
        long expected = fromInclusive;
        while ( cursor.next() )
        {
            KEY key = key( expected );
            VALUE value = value( expected );
            assertKeyAndValue( cursor, key, value );
            expected += stride;
        }
        assertEquals( toExclusive, expected );
    }

    private void assertKeyAndValue( SeekCursor<KEY,VALUE> cursor, long expectedKeySeed )
    {
        KEY key = key( expectedKeySeed );
        VALUE value = value( expectedKeySeed );
        assertKeyAndValue( cursor, key, value );
    }

    private void assertKeyAndValue( SeekCursor<KEY,VALUE> cursor, KEY expectedKey, VALUE expectedValue )
    {
        KEY foundKey = cursor.get().key();
        VALUE foundValue = cursor.get().value();
        assertEqualsKey( expectedKey, foundKey );
        assertEqualsValue( expectedValue, foundValue );
    }

    private void assertEqualsKey( KEY expected, KEY actual )
    {
        assertTrue( String.format( "expected equal, expected=%s, actual=%s", expected.toString(), actual.toString() ),
                layout.compare( expected, actual ) == 0 );
    }

    private void assertEqualsValue( VALUE expected, VALUE actual )
    {
        assertTrue( String.format( "expected equal, expected=%s, actual=%s", expected.toString(), actual.toString() ),
                layout.compareValue( expected, actual ) == 0 );
    }

    private void insertKeysAndValues( int keyCount )
    {
        for ( int i = 0; i < keyCount; i++ )
        {
            append( i );
        }
    }

    private void append( long k )
    {
        int keyCount = TreeNode.keyCount( cursor );
        node.insertKeyValueAt( cursor, key( k ), value( k ), keyCount, keyCount );
        TreeNode.setKeyCount( cursor, keyCount + 1 );
    }

    private void insertIn( int pos, long k )
    {
        int keyCount = TreeNode.keyCount( cursor );
        KEY key = key( k );
        VALUE value = value( k );
        TreeNode.Overflow overflow = node.leafOverflow( cursor, keyCount, key, value );
        if ( overflow != TreeNode.Overflow.NO )
        {
            throw new IllegalStateException( "Can not insert another key in current node" );
        }
        node.insertKeyValueAt( cursor, key, value, pos, keyCount );
        TreeNode.setKeyCount( cursor, keyCount + 1 );
    }

    private void removeAtPos( int pos )
    {
        int keyCount = TreeNode.keyCount( cursor );
        node.removeKeyValueAt( cursor, pos, keyCount );
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

    private long keyAt( PageCursor cursor, int pos, TreeNode.Type type )
    {
        KEY readKey = layout.newKey();
        node.keyAt( cursor, readKey, pos, type );
        return getSeed( readKey );
    }

    // KEEP even if unused
    @SuppressWarnings( "unused" )
    private void printTree() throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        cursor.next( rootId );
        new TreePrinter<>( node, layout, stableGeneration, unstableGeneration )
                .printTree( cursor, cursor, System.out, false, false, false, false );
        cursor.next( currentPageId );
    }
}
