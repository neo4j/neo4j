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

import org.neo4j.index.Hit;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.ValueMergers.overwrite;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointer;

public class SeekCursorTest
{
    private static final int PAGE_SIZE = 256;
    private static final LongSupplier generationSupplier = new LongSupplier()
    {
        @Override
        public long getAsLong()
        {
            return Generation.generation( stableGen, unstableGen );
        }
    };
    private static final Supplier<Root> failingRootCatchup = () -> {
        throw new AssertionError( "Should not happen" );
    };

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( PAGE_SIZE, layout );
    private final InternalTreeLogic<MutableLong,MutableLong> treeLogic = new InternalTreeLogic<>( id, node, layout );
    private final StructurePropagation<MutableLong> structurePropagation =
            new StructurePropagation<>( layout.newKey() );
    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
    private final int maxKeyCount = node.leafMaxKeyCount();

    private final MutableLong insertKey = layout.newKey();
    private final MutableLong insertValue = layout.newValue();
    private final MutableLong readKey = layout.newKey();
    private final MutableLong readValue = layout.newValue();

    private final MutableLong from = layout.newKey();
    private final MutableLong to = layout.newKey();

    private static long stableGen = GenSafePointer.MIN_GENERATION;
    private static long unstableGen = stableGen + 1;

    private long rootId;
    private long rootGen;
    private int numberOfRootSplits;

    @Before
    public void setUp() throws IOException
    {
        cursor.next( id.acquireNewId( stableGen, unstableGen ) );
        node.initializeLeaf( cursor, stableGen, unstableGen );
        updateRoot();
    }

    private void updateRoot()
    {
        rootId = cursor.getCurrentPageId();
        rootGen = unstableGen;
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
        long left = cursor.getCurrentPageId();
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive, pageCursorSpy ) )
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
            long midInsert = (stopPoint * 2) - 1;
            insertIn( stopPoint, midInsert );
            expected.add( readKeys, midInsert );
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
            long midInsert = ((stopPoint - 1) * 2) - 1;
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
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

            // Add rightmost keys to right sibling
            long left = createRightSibling( this.cursor );
            for ( int i = middle; i <= maxKeyCount; i++ )
            {
                Long key = expected.get( i );
                append( key );
            }
            // Update keycount in left sibling
            this.cursor.next( left );
            node.setKeyCount( this.cursor, middle );
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
        try ( SeekCursor<MutableLong,MutableLong> cursor = seekCursor( fromInclusive, toExclusive ) )
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

            // Add rightmost keys to right sibling
            long left = createRightSibling( this.cursor );
            for ( int i = middle; i <= maxKeyCount; i++ )
            {
                Long key = expected.get( i );
                append( key );
            }
            // Update keycount in left sibling
            this.cursor.next( left );
            node.setKeyCount( this.cursor, middle );
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
            remove( maxKeyCount - 1 );
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
            // [1 ... maxKeyCount-1]
            remove( 0 );
            this.cursor.forceRetry();

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
            remove( middle - 1 );
            this.cursor.forceRetry();

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
        try ( SeekCursor<MutableLong,MutableLong> cursor = new SeekCursor<>( this.cursor, insertKey, insertValue,
                node, from, to, layout, stableGen, unstableGen, () -> 0L, failingRootCatchup, unstableGen ) )
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

        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, i, cursor ) )
        {
            // when new generation of right sibling
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( rootId );
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
        long newRoot;
        long i = 0L;
        while ( numberOfRootSplits == 0 )
        {
            insert( i, i * 10 );
            i++;
        }

        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, i, cursor ) )
        {
            // when right sibling pointer is corrupt
            PageAwareByteArrayCursor duplicate = cursor.duplicate( rootId );
            duplicate.next();
            long leftChild = childAt( duplicate, 0, stableGen, unstableGen );
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
    public void shouldRereadNewGenIfReadFailureCausedByCheckpointInLeaf() throws Exception
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

        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, maxKeyCount, cursor ) )
        {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( cursor.getCurrentPageId() );
            duplicate.next();
            insert( i, i * 10, duplicate ); // Create new gen of leaf
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
    public void shouldFailNewGenIfReadFailureNotCausedByCheckpointInLeaf() throws Exception
    {
        // given
        long i = 0L;
        for ( ; i < maxKeyCount / 2; i++ )
        {
            insert( i, i * 10 );
        }

        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor( 0L, maxKeyCount, cursor ) )
        {
            // when
            checkpoint();
            PageAwareByteArrayCursor duplicate = cursor.duplicate( cursor.getCurrentPageId() );
            duplicate.next();
            insert( i, i * 10, duplicate ); // Create new gen of leaf

            // and corrupt new gen pointer
            corruptGSPP( duplicate, TreeNode.BYTE_POS_NEWGEN );

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
    public void shouldRereadNewGenIfReadFailureCausedByCheckpointInInternal() throws Exception
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
        long oldStableGen = stableGen;
        long oldUnstableGen = unstableGen;
        checkpoint();
        int keyCount = node.keyCount( cursor );

        // and update root with an insert in new generation
        while ( keyCount( rootId ) == keyCount )
        {
            insert( i, i * 10 );
            i++;
        }
        node.goTo( cursor, "root", rootId );
        long rightChild = childAt( cursor, 2, stableGen, unstableGen );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( oldRootId );
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor( pageCursorForSeeker );
        breadcrumbCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, breadcrumbCursor, oldStableGen, oldUnstableGen ) )
        {
            while ( seek.next() )
            {
            }
        }

        // then
        // make sure seek cursor went to new gen of root node
        assertEquals( Arrays.asList( oldRootId, rootId, rightChild ), breadcrumbCursor.getBreadcrumbs() );
    }

    private int keyCount( long nodeId ) throws IOException
    {
        long prevId = cursor.getCurrentPageId();
        try
        {
            node.goTo( cursor, "supplied", nodeId );
            return node.keyCount( cursor );
        }
        finally
        {
            node.goTo( cursor, "prev", prevId );
        }
    }

    @Test
    public void shouldFailNewGenIfReadFailureNotCausedByCheckpointInInternal() throws Exception
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
        long oldStableGen = stableGen;
        long oldUnstableGen = unstableGen;
        checkpoint();
        int keyCount = node.keyCount( cursor );

        // and update root with an insert in new generation
        while ( node.keyCount( cursor ) == keyCount )
        {
            insert( i, i * 10 );
            i++;
        }

        // and corrupt new gen pointer
        cursor.next( rootId );
        corruptGSPP( cursor, TreeNode.BYTE_POS_NEWGEN );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        pageCursorForSeeker.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, pageCursorForSeeker, oldStableGen, oldUnstableGen ) )
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
        long oldStableGen = stableGen;
        long oldUnstableGen = unstableGen;
        checkpoint();

        // and an update to root with a new gen child pointer
        insert( i, i * 10 );
        i++;
        long newRightChild = childAt( cursor, 1, stableGen, unstableGen );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        BreadcrumbPageCursor breadcrumbCursor = new BreadcrumbPageCursor( pageCursorForSeeker );
        breadcrumbCursor.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, breadcrumbCursor, oldStableGen, oldUnstableGen ) )
        {
            while ( seek.next() )
            {
            }
        }

        // then
        // make sure seek cursor went to new gen of root node
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
        long oldStableGen = stableGen;
        long oldUnstableGen = unstableGen;
        checkpoint();

        // and update root with an insert in new generation
        insert( i, i * 10 );
        i++;

        // and corrupt new gen pointer
        corruptGSPP( cursor, node.childOffset( 1 ) );

        // when
        // starting a seek on the old root with generation that is not up to date, simulating a concurrent checkpoint
        PageAwareByteArrayCursor pageCursorForSeeker = cursor.duplicate( rootId );
        pageCursorForSeeker.next();
        try ( SeekCursor<MutableLong,MutableLong> seek = seekCursor(
                i, i + 1, pageCursorForSeeker, oldStableGen, oldUnstableGen ) )
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
        long gen = node.gen( cursor );
        MutableBoolean triggered = new MutableBoolean( false );
        Supplier<Root> rootCatchup = () ->
        {
            triggered.setTrue();
            return new Root( id, gen );
        };

        // when
        try ( SeekCursor<MutableLong,MutableLong> seek = new SeekCursor<>( cursor, readKey, readValue, node, from, to,
                layout, stableGen, unstableGen, generationSupplier, rootCatchup, gen - 1 ) )
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
        long gen = node.gen( cursor );
        MutableBoolean triggered = new MutableBoolean( false );

        // a newer leaf
        long leftChild = cursor.getCurrentPageId();
        node.initializeLeaf( cursor, stableGen + 1, unstableGen + 1 ); // A newer leaf
        cursor.next();

        // a root
        long rootId = cursor.getCurrentPageId();
        node.initializeInternal( cursor, stableGen, unstableGen );
        long keyInRoot = 10L;
        insertKey.setValue( keyInRoot );
        node.insertKeyAt( cursor, insertKey, 0, 0 );
        node.setKeyCount( cursor, 1 );
        // with old pointer to child (simulating reuse of child node)
        node.setChildAt( cursor, leftChild, 0, stableGen, unstableGen );

        // a root catchup that records usage
        Supplier<Root> rootCatchup = () ->
        {
            try
            {
                triggered.setTrue();

                // and set child generation to match pointer
                cursor.next( leftChild );
                cursor.zapPage();
                node.initializeLeaf( cursor, stableGen, unstableGen );

                cursor.next( rootId );
                return new Root( rootId, gen );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };

        // when
        from.setValue( 1L );
        to.setValue( 2L );
        try ( SeekCursor<MutableLong,MutableLong> seek = new SeekCursor<>( cursor, readKey, readValue, node, from, to,
                layout, stableGen, unstableGen, generationSupplier, rootCatchup, unstableGen ) )
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        cursor.next();

        Supplier<Root> rootCatchup = () ->
        {
            try
            {
                // Use right child as new start over root to terminate test
                cursor.next( rightChild );
                triggered.setTrue();
                return new Root( cursor.getCurrentPageId(), node.gen( cursor ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        };

        // a left leaf
        long leftChild = cursor.getCurrentPageId();
        node.initializeLeaf( cursor, stableGen - 1, unstableGen - 1 );
        // with an old pointer to right sibling
        node.setRightSibling( cursor, rightChild, stableGen - 1, unstableGen - 1 );
        cursor.next();

        // a root
        node.initializeInternal( cursor, stableGen - 1, unstableGen - 1 );
        long keyInRoot = 10L;
        insertKey.setValue( keyInRoot );
        node.insertKeyAt( cursor, insertKey, 0, 0 );
        node.setKeyCount( cursor, 1 );
        // with old pointer to child (simulating reuse of internal node)
        node.setChildAt( cursor, leftChild, 0, stableGen, unstableGen );

        // when
        from.setValue( 1L );
        to.setValue( 20L );
        try ( SeekCursor<MutableLong,MutableLong> seek = new SeekCursor<>( cursor, readKey, readValue, node, from, to,
                layout, stableGen - 1, unstableGen - 1, generationSupplier, rootCatchup, unstableGen ) )
        {
            while ( seek.next() )
            {
                seek.get();
            }
        }

        // then
        assertTrue( triggered.getValue() );
    }

    private void checkpoint()
    {
        stableGen = unstableGen;
        unstableGen++;
    }

    private void newRootFromSplit( StructurePropagation<MutableLong> split ) throws IOException
    {
        assertTrue( split.hasSplit );
        long rootId = id.acquireNewId( stableGen, unstableGen );
        cursor.next( rootId );
        node.initializeInternal( cursor, stableGen, unstableGen );
        node.insertKeyAt( cursor, split.primKey, 0, 0 );
        node.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.left, 0, stableGen, unstableGen );
        node.setChildAt( cursor, split.right, 1, stableGen, unstableGen );
        split.hasSplit = false;
        numberOfRootSplits++;
        updateRoot();
    }

    private void corruptGSPP( PageAwareByteArrayCursor duplicate, int offset )
    {
        int someBytes = duplicate.getInt( offset );
        duplicate.putInt( offset, ~someBytes );
        someBytes = duplicate.getInt( offset + GenSafePointer.SIZE );
        duplicate.putInt( offset + GenSafePointer.SIZE, ~someBytes );
    }

    private void insert( long key, long value ) throws IOException
    {
        insert( key, value, cursor );
    }

    private void insert( long key, long value, PageCursor cursor ) throws IOException
    {
        insertKey.setValue( key );
        insertValue.setValue( value );
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, overwrite(), stableGen, unstableGen );
        handleAfterChange();
    }

    private void handleAfterChange() throws IOException
    {
        if ( structurePropagation.hasSplit )
        {
            newRootFromSplit( structurePropagation );
        }
        if ( structurePropagation.hasNewGen )
        {
            structurePropagation.hasNewGen = false;
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
        return seekCursor( fromInclusive, toExclusive, pageCursor, stableGen, unstableGen );
    }
    private SeekCursor<MutableLong,MutableLong> seekCursor( long fromInclusive, long toExclusive,
            PageCursor pageCursor, long stableGen, long unstableGen ) throws IOException
    {
        from.setValue( fromInclusive );
        to.setValue( toExclusive );
        return new SeekCursor<>( pageCursor, readKey, readValue, node, from,
                to, layout, stableGen, unstableGen, generationSupplier, failingRootCatchup, unstableGen );
    }

    /**
     * Create a right sibling to node pointed to by cursor. Leave cursor on new right sibling when done,
     * and return id of left sibling.
     */
    private long createRightSibling( PageCursor pageCursor ) throws IOException
    {
        long left = pageCursor.getCurrentPageId();
        long right = left + 1;

        node.setRightSibling( pageCursor, right, stableGen, unstableGen );

        pageCursor.next( right );
        node.initializeLeaf( pageCursor, stableGen, unstableGen );
        node.setLeftSibling( pageCursor, left, stableGen, unstableGen );
        return left;
    }

    private static void assertRangeInSingleLeaf( int fromInclusive, int toExclusive,
            SeekCursor<MutableLong,MutableLong> cursor ) throws IOException
    {
        long expectedKey = fromInclusive;
        while ( cursor.next() )
        {
            assertKeyAndValue( cursor, expectedKey );
            expectedKey++;
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
        int keyCount = node.keyCount( cursor );
        insertKey.setValue( k );
        insertValue.setValue( valueForKey( k ) );
        node.insertKeyAt( cursor, insertKey, keyCount, keyCount );
        node.insertValueAt( cursor, insertValue, keyCount, keyCount );
        node.setKeyCount( cursor, keyCount + 1 );
    }

    private void insertIn( int pos, long k )
    {
        int keyCount = node.keyCount( cursor );
        if ( keyCount + 1 > maxKeyCount )
        {
            throw new IllegalStateException( "Can not insert another key in current node" );
        }
        insertKey.setValue( k );
        insertValue.setValue( valueForKey( k ) );
        node.insertKeyAt( cursor, insertKey, pos, keyCount );
        node.insertValueAt( cursor, insertValue, pos, keyCount );
        node.setKeyCount( cursor, keyCount + 1 );
    }

    private void remove( int pos )
    {
        int keyCount = node.keyCount( cursor );
        node.removeKeyAt( cursor, pos, keyCount );
        node.removeValueAt( cursor, pos, keyCount );
        node.setKeyCount( cursor, keyCount - 1 );
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

    private long childAt( PageCursor cursor, int pos, long stableGen, long unstableGen )
    {
        return pointer( node.childAt( cursor, pos, stableGen, unstableGen ) );
    }
}
