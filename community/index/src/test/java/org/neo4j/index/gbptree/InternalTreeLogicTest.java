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
import static org.junit.Assert.assertNotEquals;
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

    private static final long OLD_STABLE_GENERATION = 1;
    private static final long STABLE_GENERATION = 2;
    private static final long UNSTABLE_GENERATION = 3;
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
        long newId = id.acquireNewId();
        cursor.next( newId );
    }

    @Test
    public void modifierMustInsertAtFirstPositionInEmptyLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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

    @Test
    public void shouldCreateNewVersionWhenInsertInStableRootAsLeaf() throws Exception
    {
        // GIVEN root
        node.initializeLeaf( cursor, OLD_STABLE_GENERATION, STABLE_GENERATION );
        long oldGenId = cursor.getCurrentPageId();

        // WHEN root -[newGen]-> newGen root
        insert( 1L, 1L, STABLE_GENERATION, UNSTABLE_GENERATION );
        long newGenId = cursor.getCurrentPageId();

        // THEN
        assertTrue( structurePropagation.hasNewGen );
        assertEquals( newGenId, structurePropagation.left );
        assertNotEquals( oldGenId, newGenId );
        assertEquals( 1, node.keyCount( cursor ) );

        node.goTo( cursor, oldGenId, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( newGenId, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 0, node.keyCount( cursor ) );
    }

    @Test
    public void shouldCreateNewVersionWhenRemoveInStableRootAsLeaf() throws Exception
    {
        // GIVEN root
        node.initializeLeaf( cursor, OLD_STABLE_GENERATION, STABLE_GENERATION );
        long key = 1L;
        long value = 10L;
        insert( key, value, OLD_STABLE_GENERATION, STABLE_GENERATION );
        long oldGenId = cursor.getCurrentPageId();

        // WHEN root -[newGen]-> newGen root
        remove( key, readValue, STABLE_GENERATION, UNSTABLE_GENERATION );
        long newGenId = cursor.getCurrentPageId();

        // THEN
        assertTrue( structurePropagation.hasNewGen );
        assertEquals( newGenId, structurePropagation.left );
        assertNotEquals( oldGenId, newGenId );
        assertEquals( 0, node.keyCount( cursor ) );

        node.goTo( cursor, oldGenId, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( newGenId, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 1, node.keyCount( cursor ) );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableLeaf() throws Exception
    {
        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        node.initializeLeaf( cursor, OLD_STABLE_GENERATION, STABLE_GENERATION );
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i, OLD_STABLE_GENERATION, STABLE_GENERATION );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation, OLD_STABLE_GENERATION, STABLE_GENERATION );
            }
        }
        assertEquals( 2, node.keyCount( cursor ) );
        long leftChild = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        long middleChild = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        long rightChild = node.childAt( cursor, 2, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        long root = cursor.getCurrentPageId();
        long middleKey = i / 2; // Should be located in middle leaf
        long newValue = middleKey * 100;
        insert( middleKey, newValue, STABLE_GENERATION, UNSTABLE_GENERATION );
        cursor.next( 5 );
        cursor.next( root );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has new gen
        cursor.next( middleChild );
        assertEquals( newMiddleChild, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );

        // old middle child has seen no change
        assertKeyAssociatedWithValue( middleKey, middleKey );

        // new middle child has seen change
        cursor.next( newMiddleChild );
        assertKeyAssociatedWithValue( middleKey, newValue );

        // sibling pointers updated
        assertSiblings( leftChild, newMiddleChild, rightChild );
    }

    @Test
    public void shouldCreateNewVersionWhenRemoveInStableLeaf() throws Exception
    {
        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        node.initializeLeaf( cursor, OLD_STABLE_GENERATION, STABLE_GENERATION );
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i, OLD_STABLE_GENERATION, STABLE_GENERATION );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation, OLD_STABLE_GENERATION, STABLE_GENERATION );
            }
        }
        assertEquals( 2, node.keyCount( cursor ) );
        long leftChild = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        long middleChild = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        long rightChild = node.childAt( cursor, 2, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        long root = cursor.getCurrentPageId();
        long middleKey = i / 2; // Should be located in middle leaf
        remove( middleKey, insertValue, STABLE_GENERATION, UNSTABLE_GENERATION );
        cursor.next( 5 );
        cursor.next( root );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has new gen
        cursor.next( middleChild );
        assertEquals( newMiddleChild, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );

        // old middle child has seen no change
        assertKeyAssociatedWithValue( middleKey, middleKey );

        // new middle child has seen change
        cursor.next( newMiddleChild );
        assertKeyNotFound( middleKey );

        // sibling pointers updated
        assertSiblings( leftChild, newMiddleChild, rightChild );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableRootAsInternal() throws Exception
    {
        // GIVEN:
        //                       root
        //                   ----   ----
        //                  /           \
        //                 v             v
        //               left <-------> right
        node.initializeLeaf( cursor, OLD_STABLE_GENERATION, STABLE_GENERATION );
        long i = 0;
        int countToProduceAboveImageAndFullRight =
                maxKeyCount /*will split root leaf into two half left/right*/ + maxKeyCount / 2;
        for ( ; i < countToProduceAboveImageAndFullRight; i++ )
        {
            insert( i, i, OLD_STABLE_GENERATION, STABLE_GENERATION );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation, OLD_STABLE_GENERATION, STABLE_GENERATION );
            }
        }
        long root = cursor.getCurrentPageId();
        assertEquals( 1, node.keyCount( cursor ) );
        long leftChild = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        long rightChild = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertSiblings( leftChild, rightChild, TreeNode.NO_NODE_FLAG );

        // WHEN
        //                       root(newGen)
        //                   ----  | ---------------
        //                  /      |                \
        //                 v       v                 v
        //               left <-> right(newGen) <--> farRight
        insert( i, i, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertTrue( structurePropagation.hasNewGen );
        long newRoot = cursor.getCurrentPageId();
        leftChild = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        rightChild = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        // siblings are correct
        long farRightChild = node.childAt( cursor, 2, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertSiblings( leftChild, rightChild, farRightChild );

        // old root points to new gen root
        cursor.next( root );
        assertEquals( newRoot, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableInternal() throws Exception
    {
        // GIVEN:
        node.initializeLeaf( cursor, OLD_STABLE_GENERATION, STABLE_GENERATION );
        int rootAllocations = 0;
        for ( int i = 0; rootAllocations < 2; i++ )
        {
            long keyAndValue = i * maxKeyCount;
            insert( keyAndValue, keyAndValue, OLD_STABLE_GENERATION, STABLE_GENERATION );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation, OLD_STABLE_GENERATION, STABLE_GENERATION );
                rootAllocations++;
            }
        }
        long root = cursor.getCurrentPageId();
        assertEquals( 1, node.keyCount( cursor ) );
        long leftInternal = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        long rightInternal = node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertSiblings( leftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
        cursor.next( leftInternal );
        int leftInternalKeyCount = node.keyCount( cursor );
        assertTrue( node.isInternal( cursor ) );
        long leftLeaf = node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        cursor.next( leftLeaf );
        long firstKeyInLeaf = node.keyAt( cursor, readKey, 0 ).longValue();
        cursor.next( root );

        // WHEN
        long targetLastId = id.lastId() + 3; /*one for newGen in leaf, one for split leaf, one for newGen in internal*/
        for ( int i = 0; id.lastId() < targetLastId; i++ )
        {
            insert( firstKeyInLeaf + i, firstKeyInLeaf + i, STABLE_GENERATION, UNSTABLE_GENERATION );
            assertFalse( structurePropagation.hasSplit ); // there should be no root split
        }

        // THEN
        // root hasn't been split further
        assertEquals( root, cursor.getCurrentPageId() );

        // there's a new generation of left internal w/ one more key in
        long newGenLeftInternal = id.lastId();
        assertEquals( newGenLeftInternal, node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        cursor.next( newGenLeftInternal );
        int newGenLeftInternalKeyCount = node.keyCount( cursor );
        assertEquals( leftInternalKeyCount + 1, newGenLeftInternalKeyCount );

        // and left internal points to the new gen
        cursor.next( leftInternal );
        assertEquals( newGenLeftInternal, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertSiblings( newGenLeftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
    }

    private void assertKeyAssociatedWithValue( long key, long expectedValue )
    {
        insertKey.setValue( key );
        int search = KeySearch.search( cursor, node, insertKey, readKey, node.keyCount( cursor ) );
        assertTrue( KeySearch.isHit( search ) );
        int keyPos = KeySearch.positionOf( search );
        node.valueAt( cursor, readValue, keyPos );
        assertEquals( expectedValue, readValue.longValue() );
    }

    private void assertKeyNotFound( long key )
    {
        insertKey.setValue( key );
        int search = KeySearch.search( cursor, node, insertKey, readKey, node.keyCount( cursor ) );
        assertFalse( KeySearch.isHit( search ) );
    }

    private void assertSiblings( long left, long middle, long right ) throws IOException
    {
        long origin = cursor.getCurrentPageId();
        cursor.next( middle );
        assertEquals( right, node.rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( left, node.leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        if ( left != TreeNode.NO_NODE_FLAG )
        {
            cursor.next( left );
            assertEquals( middle, node.rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        }
        if ( right != TreeNode.NO_NODE_FLAG )
        {
            cursor.next( right );
            assertEquals( middle, node.leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        }
        cursor.next( origin );
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
        return newRootFromSplit( split, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private long newRootFromSplit( StructurePropagation<MutableLong> split,
            long stableGeneration, long unstableGeneration ) throws IOException
    {
        assertTrue( split.hasSplit );
        long rootId = id.acquireNewId();
        cursor.next( rootId );
        node.initializeInternal( cursor, stableGeneration, unstableGeneration );
        node.insertKeyAt( cursor, split.primKey, 0, 0, tmp );
        node.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.left, 0, stableGeneration, unstableGeneration );
        node.setChildAt( cursor, split.right, 1, stableGeneration, unstableGeneration );
        split.hasSplit = false;
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

    private void insert( long key, long value, long stableGeneration, long unstableGeneration ) throws IOException
    {
        insert( key, value, stableGeneration, unstableGeneration, overwrite() );
    }

    private void insert( long key, long value, ValueMerger<MutableLong> valueMerger ) throws IOException
    {
        insert( key, value, STABLE_GENERATION, UNSTABLE_GENERATION, valueMerger );
    }

    private void insert( long key, long value, long stableGeneration, long unstableGeneration,
            ValueMerger<MutableLong> valueMerger ) throws IOException
    {
        structurePropagation.hasSplit = false;
        structurePropagation.hasNewGen = false;
        insertKey.setValue( key );
        insertValue.setValue( value );
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, valueMerger, DEFAULTS,
                stableGeneration, unstableGeneration );
    }

    private MutableLong remove( long key, MutableLong into ) throws IOException
    {
        return remove( key, into, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private MutableLong remove( long key, MutableLong into, long stableGeneration, long unstableGeneration )
            throws IOException
    {
        insertKey.setValue( key );
        return treeLogic.remove( cursor, structurePropagation, insertKey, into, stableGeneration, unstableGeneration );
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

        long lastId()
        {
            return lastId;
        }

        private void reset()
        {
            lastId = IdSpace.MIN_TREE_NODE_ID - 1;
        }
    }
}
