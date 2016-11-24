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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.index.ValueMerger;
import org.neo4j.index.ValueMergers;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.IndexWriter.Options.DEFAULTS;
import static org.neo4j.index.ValueMergers.overwrite;

public class InternalTreeLogicTest
{
    private static final ValueMerger<MutableLong> ADDER = (base,add) -> {
        base.add( add.longValue() );
        return base;
    };

    private static final int STABLE_GENERATION = 1;
    private static final int UNSTABLE_GENERATION = 2;
    private final int pageSize = 256;

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( pageSize, layout );
    private final InternalTreeLogic<MutableLong,MutableLong> treeLogic = new InternalTreeLogic<>( id, node, layout );

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );
    private final int maxKeyCount = node.leafMaxKeyCount();

    private final MutableLong insertKey = new MutableLong();
    private final MutableLong insertValue = new MutableLong();
    private final MutableLong readKey = new MutableLong();
    private final MutableLong readValue = new MutableLong();
    private final byte[] tmp = new byte[pageSize];
    private final StructurePropagation<MutableLong> structurePropagation = new StructurePropagation<>( layout.newKey() );

    @Rule
    public RandomRule random = new RandomRule();

    @Before
    public void setUp() throws IOException
    {
        id.reset();
        cursor.initialize();
        long newId = id.acquireNewId();
        cursor.next( newId );
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    @Test
    public void modifierMustInsertAtFirstPositionInEmptyLeaf() throws Exception
    {
        // given
        long key = 1L;
        long value = 1L;
        assertThat( node.keyCount( cursor ), is( 0 ) );

        // when
        insert( key, value );

        // then
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( key ) );
        assertThat( valueAt( 0 ), is( key ) );
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertFirstInLeaf() throws Exception
    {
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // given
            long key = maxKeyCount - i;
            insert( key, key );

            // then
            assertThat( keyAt( 0 ), is( key ) );
            assertThat( valueAt( 0 ), is( key ) );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertLastInLeaf() throws Exception
    {
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // given
            insert( i, i );

            // then
            assertThat( keyAt( i ), is( (long) i ) );
            assertThat( valueAt( i ), is( (long) i ) );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertInMiddleOfLeaf() throws Exception
    {
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // given
            long key = i % 2 == 0 ? i / 2 : maxKeyCount - i / 2;
            insert( key, key );

            // then
            assertThat( keyAt( (i + 1) / 2 ), is( key ) );
        }
    }

    @Test
    public void modifierMustSplitWhenInsertingMiddleOfFullLeaf() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i % 2 == 0 ? i : maxKeyCount * 2 - i;
            insert( key, key );
        }

        // then
        long middle = maxKeyCount;
        insert( middle, middle );
        assertTrue( structurePropagation.hasSplit );
    }

    @Test
    public void modifierMustSplitWhenInsertingLastInFullLeaf() throws Exception
    {
        // given
        long key = 0;
        while ( key < maxKeyCount )
        {
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
            key++;
        }

        // then
        insert( key, key );
        assertTrue( structurePropagation.hasSplit ); // Should cause a split
    }

    @Test
    public void modifierMustSplitWhenInsertingFirstInFullLeaf() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i + 1;
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
        }

        // then
        insert( 0L, 0L );
        assertTrue( structurePropagation.hasSplit );
    }

    @Test
    public void modifierMustLeaveCursorOnSamePageAfterSplitInLeaf() throws Exception
    {
        // given
        long pageId = cursor.getCurrentPageId();
        long key = 0;
        while ( key < maxKeyCount )
        {
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
            key++;
        }

        // when
        insert( key, key );
        assertTrue( structurePropagation.hasSplit ); // Should cause a split

        // then
        assertThat( cursor.getCurrentPageId(), is( pageId ) );
    }

    @Test
    public void modifierMustUpdatePointersInSiblingsToSplit() throws Exception
    {
        // given
        long someLargeNumber = maxKeyCount * 1000;
        long i = 0;
        while ( i < maxKeyCount )
        {
            insert( someLargeNumber - i, i );
            i++;
        }

        // First split
        insert( someLargeNumber - i, i );
        i++;
        newRootFromSplit( structurePropagation );

        // Assert child pointers and sibling pointers are intact after split in root
        long child0 = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        long child1 = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertSiblingOrderAndPointers( child0, child1 );

        // Insert until we have another split in leftmost leaf
        while ( node.keyCount( cursor ) == 1 )
        {
            insert( someLargeNumber - i, i );
            i++;
        }

        // Just to be sure
        assertTrue( node.isInternal( cursor ) );
        assertThat( node.keyCount( cursor ), is( 2 ) );

        // Assert child pointers and sibling pointers are intact
        // AND that node not involved in split also has its left sibling pointer updated
        child0 = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        child1 = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        long child2 = node.childAt( cursor, 2, STABLE_GENERATION, UNSTABLE_GENERATION ); // <- right sibling to split-node before split

        assertSiblingOrderAndPointers( child0, child1, child2 );
    }

    /* REMOVE */
    @Test
    public void modifierMustRemoveFirstInEmptyLeaf() throws Exception
    {
        // given
        long key = 1L;
        long value = 1L;
        insert( key, value );

        // when
        remove( key, readValue );

        // then
        assertThat( node.keyCount( cursor ), is( 0 ) );
    }

    @Test
    public void modifierMustRemoveFirstInFullLeaf() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        remove( 0, readValue );

        // then
        assertThat( node.keyCount( cursor ), is( maxKeyCount - 1) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            assertThat( keyAt( i ), is( i + 1L ) );
        }
    }

    @Test
    public void modifierMustRemoveInMiddleInFullLeaf() throws Exception
    {
        // given
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        remove( middle, readValue );

        // then
        assertThat( node.keyCount( cursor ), is( maxKeyCount - 1) );
        assertThat( keyAt( middle ), is( middle + 1L ) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            long expected = i < middle ? i : i + 1L;
            assertThat( keyAt( i ), is( expected ) );
        }
    }

    @Test
    public void modifierMustRemoveLastInFullLeaf() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        remove( maxKeyCount - 1, readValue );

        // then
        assertThat( node.keyCount( cursor ), is( maxKeyCount - 1) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            Long actual = keyAt( i );
            assertThat( actual, is( (long) i ) );
        }
    }

    @Test
    public void modifierMustRemoveFromLeftChild() throws Exception
    {
        // given
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when
        cursor.next( structurePropagation.left );
        assertThat( keyAt( 0 ), is( 0L ) );
        cursor.next( rootId );
        remove( 0, readValue );

        // then
        cursor.next( structurePropagation.left );
        assertThat( keyAt( 0 ), is( 1L ) );
    }

    @Test
    public void modifierMustRemoveFromRightChildButNotFromInternalWithHitOnInternalSearch() throws Exception
    {
        // given
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when key to remove exists in internal
        Long keyToRemove = structurePropagation.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        cursor.next( structurePropagation.right );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        cursor.next( rootId );
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        cursor.next( structurePropagation.right );
        assertThat( node.keyCount( cursor ), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );
    }

    @Test
    public void modifierMustLeaveCursorOnInitialPageAfterRemove() throws Exception
    {
        // given
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when
        assertThat( cursor.getCurrentPageId(), is( rootId) );
        remove( structurePropagation.primKey.getValue(), readValue );

        // then
        assertThat( cursor.getCurrentPageId(), is( rootId ) );
    }

    @Test
    public void modifierMustNotRemoveWhenKeyDoesNotExist() throws Exception
    {
        // given
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        assertNull( remove( maxKeyCount, readValue ) );

        // then
        assertThat( node.keyCount( cursor ), is( maxKeyCount ) );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            Long actual = keyAt( i );
            assertThat( actual, is( (long) i ) );
        }
    }

    @Test
    public void modifierMustNotRemoveWhenKeyOnlyExistInInternal() throws Exception
    {
        // given
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when key to remove exists in internal
        Long keyToRemove = structurePropagation.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        cursor.next( structurePropagation.right );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        cursor.next( rootId );
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        cursor.next( structurePropagation.right );
        assertThat( node.keyCount( cursor ), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );

        // and when we remove same key again, nothing should change
        cursor.next( rootId );
        assertNull( remove( keyToRemove, readValue ) );
    }

    /* REBALANCE (when rebalance is implemented) */
    /* MERGE (when merge is implemented) */

    @Test
    public void modifierMustProduceConsistentTreeWithRandomInserts() throws Exception
    {
        int numberOfEntries = 100_000;
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            insert( random.nextLong(), random.nextLong() );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation );
            }
        }

        ConsistencyChecker<MutableLong> consistencyChecker =
                new ConsistencyChecker<>( node, layout, STABLE_GENERATION, UNSTABLE_GENERATION );
        consistencyChecker.check( cursor );
    }

    /* TEST VALUE MERGER */

    @Test
    public void modifierMustOverwriteWithOverwriteMerger() throws Exception
    {
        // given
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue );

        // when
        long secondValue = random.nextLong();
        insert( key, secondValue, ValueMergers.overwrite() );

        // then
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( valueAt( 0 ), is( secondValue ) );
    }

    @Test
    public void modifierMustKeepExistingWithKeepExistingMerger() throws Exception
    {
        // given
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue, ValueMergers.keepExisting() );
        assertThat( node.keyCount( cursor ), is( 1 ) );
        Long actual = valueAt( 0 );
        assertThat( actual, is( firstValue ) );

        // when
        long secondValue = random.nextLong();
        insert( key, secondValue, ValueMergers.keepExisting() );

        // then
        assertThat( node.keyCount( cursor ), is( 1 ) );
        actual = valueAt( 0 );
        assertThat( actual, is( firstValue ) );
    }

    @Test
    public void shouldMergeValueInRootLeaf() throws Exception
    {
        // GIVEN
        long key = 10;
        long baseValue = 100;
        insert( key, baseValue );

        // WHEN
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        int searchResult = KeySearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 0, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    @Test
    public void shouldMergeValueInLeafLeftOfParentKey() throws Exception
    {
        // GIVEN
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        newRootFromSplit( structurePropagation );

        // WHEN
        long key = 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        cursor.next( structurePropagation.left );
        int searchResult = KeySearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 1, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    @Test
    public void shouldMergeValueInLeafAtParentKey() throws Exception
    {
        // GIVEN
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        newRootFromSplit( structurePropagation );

        // WHEN
        long key = structurePropagation.primKey.longValue();
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        cursor.next( structurePropagation.right );
        int searchResult = KeySearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 0, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    @Test
    public void shouldMergeValueInLeafBetweenTwoParentKeys() throws Exception
    {
        // GIVEN
        long rootId = -1;
        long middle = -1;
        long firstSplitPrimKey = -1;
        for ( int i = 0; rootId == -1 || node.keyCount( cursor ) == 1; i++ )
        {
            insert( i, i );
            if ( structurePropagation.hasSplit )
            {
                rootId = newRootFromSplit( structurePropagation );
                middle = structurePropagation.right;
                firstSplitPrimKey = structurePropagation.primKey.longValue();
            }
        }

        // WHEN
        long key = firstSplitPrimKey + 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        cursor.next( middle );
        int searchResult = KeySearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 1, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    // KEEP even if unused
    private void printTree() throws IOException
    {
        TreePrinter.printTree( cursor, node, layout, STABLE_GENERATION, UNSTABLE_GENERATION, System.out );
    }

    private MutableLong key( long key )
    {
        return new MutableLong( key );
    }

    private long newRootFromSplit( StructurePropagation<MutableLong> split ) throws IOException
    {
        assertTrue( split.hasSplit );
        long rootId = id.acquireNewId();
        cursor.next( rootId );
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.insertKeyAt( cursor, split.primKey, 0, 0, tmp );
        node.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.left, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.setChildAt( cursor, split.right, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        return rootId;
    }

    private void assertSiblingOrderAndPointers( long... children ) throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        RightmostInChain<MutableLong> rightmost =
                new RightmostInChain<>( node, STABLE_GENERATION, UNSTABLE_GENERATION );
        for ( long child : children )
        {
            cursor.next( child );
            rightmost.assertNext( cursor );
        }
        rightmost.assertLast();
        cursor.next( currentPageId );
    }

    private Long keyAt( int pos )
    {
        return node.keyAt( cursor, readKey, pos ).getValue();
    }

    private Long valueAt( int pos )
    {
        return node.valueAt( cursor, readValue, pos ).getValue();
    }

    private void insert( long key, long value ) throws IOException
    {
        insert( key, value, overwrite() );
    }

    private void insert( long key, long value, ValueMerger<MutableLong> valueMerger ) throws IOException
    {
        structurePropagation.hasSplit = false;
        structurePropagation.hasNewGen = false;
        insertKey.setValue( key );
        insertValue.setValue( value );
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, valueMerger, DEFAULTS,
                STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private MutableLong remove( long key, MutableLong into ) throws IOException
    {
        insertKey.setValue( key );
        return treeLogic.remove( cursor, structurePropagation, insertKey, into, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private static class SimpleIdProvider implements IdProvider
    {
        private long lastId;

        private SimpleIdProvider()
        {
            reset();
        }

        @Override
        public long acquireNewId()
        {
            lastId++;
            return lastId;
        }

        private void reset()
        {
            lastId = IdSpace.MIN_TREE_NODE_ID - 1;
        }
    }
}
