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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.index.IndexWriter.Options.DEFAULTS;
import static org.neo4j.index.ValueMergers.overwrite;

@RunWith( Parameterized.class )
public class InternalTreeLogicTest
{
    private static final ValueMerger<MutableLong> ADDER = (base,add) -> {
        base.add( add.longValue() );
        return base;
    };

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

    private static long stableGen = GenSafePointer.MIN_GENERATION;
    private static long unstableGen = stableGen + 1;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> generators()
    {
        List<Object[]> parameters = new ArrayList<>();
        // Initial state has same generation as update state
        parameters.add( new Object[]{
                "NoCheckpoint", (Checkpointer) () -> {
        }, false} );
        // Update state in next generation
        parameters.add( new Object[]{
                "Checkpoint", (Checkpointer) () -> {
            stableGen = unstableGen;
            unstableGen++;
        }, true} );
        return parameters;
    }

    @Parameterized.Parameter( 0 )
    public String name;
    @Parameterized.Parameter( 1 )
    public Checkpointer checkpointer;
    @Parameterized.Parameter( 2 )
    public boolean isCheckpointing;

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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = 1L;
        long value = 1L;
        assertThat( node.keyCount( cursor ), is( 0 ) );

        // when
        checkpointer.checkpoint();
        insert( key, value );

        // then
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( key ) );
        assertThat( valueAt( 0 ), is( key ) );
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertFirstInLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        checkpointer.checkpoint();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
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
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        checkpointer.checkpoint();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
            insert( i, i );

            // then
            assertThat( keyAt( i ), is( (long) i ) );
            assertThat( valueAt( i ), is( (long) i ) );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertInMiddleOfLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        checkpointer.checkpoint();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i % 2 == 0 ? i : maxKeyCount * 2 - i;
            insert( key, key );
        }

        // when
        checkpointer.checkpoint();
        long middle = maxKeyCount;
        insert( middle, middle );

        // then
        assertTrue( structurePropagation.hasSplit );
    }

    @Test
    public void modifierMustSplitWhenInsertingLastInFullLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = 0;
        while ( key < maxKeyCount )
        {
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
            key++;
        }

        // when
        checkpointer.checkpoint();
        insert( key, key );

        // then
        assertTrue( structurePropagation.hasSplit ); // Should cause a split
    }

    @Test
    public void modifierMustSplitWhenInsertingFirstInFullLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i + 1;
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
        }

        // when
        checkpointer.checkpoint();
        insert( 0L, 0L );

        // then
        assertTrue( structurePropagation.hasSplit );
    }

    @Test
    public void modifierMustLeaveCursorOnSamePageAfterSplitInLeaf() throws Exception
    {
        assumeFalse( "Checkpoiting will cause curser to move to new gen", isCheckpointing );

        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long pageId = cursor.getCurrentPageId();
        long key = 0;
        while ( key < maxKeyCount )
        {
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
            key++;
        }

        // when
        checkpointer.checkpoint();
        insert( key, key );
        assertTrue( structurePropagation.hasSplit ); // Should cause a split

        // then
        assertThat( cursor.getCurrentPageId(), is( pageId ) );
    }

    @Test
    public void modifierMustUpdatePointersInSiblingsToSplit() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long someLargeNumber = maxKeyCount * 1000;
        long i = 0;
        while ( i < maxKeyCount )
        {
            insert( someLargeNumber - i, i );
            i++;
        }

        // First split
        checkpointer.checkpoint();
        insert( someLargeNumber - i, i );
        i++;
        newRootFromSplit( structurePropagation );

        // Assert child pointers and sibling pointers are intact after split in root
        long child0 = node.childAt( cursor, 0, stableGen, unstableGen );
        long child1 = node.childAt( cursor, 1, stableGen, unstableGen );
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
        child0 = node.childAt( cursor, 0, stableGen, unstableGen );
        child1 = node.childAt( cursor, 1, stableGen, unstableGen );
        long child2 = node.childAt( cursor, 2, stableGen, unstableGen ); // <- right sibling to split-node before split

        assertSiblingOrderAndPointers( child0, child1, child2 );
    }

    /* REMOVE */
    @Test
    public void modifierMustRemoveFirstInEmptyLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = 1L;
        long value = 1L;
        insert( key, value );

        // when
        checkpointer.checkpoint();
        remove( key, readValue );

        // then
        assertThat( node.keyCount( cursor ), is( 0 ) );
    }

    @Test
    public void modifierMustRemoveFirstInFullLeaf() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when and key to remove exists in internal
        Long keyToRemove = structurePropagation.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        long rightChild = structurePropagation.right;
        cursor.next( rightChild );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        checkpointer.checkpoint();
        cursor.next( rootId );
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        rightChild = node.childAt( cursor, 1, stableGen, unstableGen );
        cursor.next( rightChild );
        assertThat( node.keyCount( cursor ), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );
    }

    @Test
    public void modifierMustLeaveCursorOnInitialPageAfterRemove() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when
        checkpointer.checkpoint();
        assertThat( cursor.getCurrentPageId(), is( rootId) );
        remove( structurePropagation.primKey.getValue(), readValue );

        // then
        assertThat( cursor.getCurrentPageId(), is( rootId ) );
    }

    @Test
    public void modifierMustNotRemoveWhenKeyDoesNotExist() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        long rootId = newRootFromSplit( structurePropagation );

        // when key to remove exists in internal
        Long keyToRemove = structurePropagation.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        long currentRightChild = structurePropagation.right;
        cursor.next( currentRightChild );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        checkpointer.checkpoint();
        cursor.next( rootId );
        remove( keyToRemove, readValue ); // Possibly create new gen of right child
        currentRightChild = node.childAt( cursor, 1, stableGen, unstableGen );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        cursor.next( currentRightChild );
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
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        int numberOfEntries = 100_000;
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            // when
            insert( random.nextLong(), random.nextLong() );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation );
            }

            if ( i == numberOfEntries / 2 )
            {
                checkpointer.checkpoint();
            }
        }

        // then
        ConsistencyChecker<MutableLong> consistencyChecker =
                new ConsistencyChecker<>( node, layout, stableGen, unstableGen );
        consistencyChecker.check( cursor );
    }

    /* TEST VALUE MERGER */

    @Test
    public void modifierMustOverwriteWithOverwriteMerger() throws Exception
    {
        // given
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue );

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue, ValueMergers.keepExisting() );
        assertThat( node.keyCount( cursor ), is( 1 ) );
        Long actual = valueAt( 0 );
        assertThat( actual, is( firstValue ) );

        // when
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = 10;
        long baseValue = 100;
        insert( key, baseValue );

        // WHEN
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        newRootFromSplit( structurePropagation );

        // WHEN
        checkpointer.checkpoint();
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        for ( int i = 0; !structurePropagation.hasSplit; i++ )
        {
            insert( i, i );
        }
        newRootFromSplit( structurePropagation );

        // WHEN
        checkpointer.checkpoint();
        long key = structurePropagation.primKey.longValue();
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        long rightChild = node.childAt( cursor, 1, stableGen, unstableGen );
        cursor.next( rightChild );
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
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long rootId = -1;
        long firstSplitPrimKey = -1;
        for ( int i = 0; rootId == -1 || node.keyCount( cursor ) == 1; i++ )
        {
            insert( i, i );
            if ( structurePropagation.hasSplit )
            {
                rootId = newRootFromSplit( structurePropagation );
                firstSplitPrimKey = structurePropagation.primKey.longValue();
            }
        }

        // WHEN
        checkpointer.checkpoint();
        long key = firstSplitPrimKey + 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        long middle = node.childAt( cursor, 1, stableGen, unstableGen );
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
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN root
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long oldGenId = cursor.getCurrentPageId();

        // WHEN root -[newGen]-> newGen root
        checkpointer.checkpoint();
        insert( 1L, 1L );
        long newGenId = cursor.getCurrentPageId();

        // THEN
        assertTrue( structurePropagation.hasNewGen );
        assertEquals( newGenId, structurePropagation.left );
        assertNotEquals( oldGenId, newGenId );
        assertEquals( 1, node.keyCount( cursor ) );

        node.goTo( cursor, oldGenId, stableGen, unstableGen );
        assertEquals( newGenId, node.newGen( cursor, stableGen, unstableGen ) );
        assertEquals( 0, node.keyCount( cursor ) );
    }

    @Test
    public void shouldCreateNewVersionWhenRemoveInStableRootAsLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN root
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long key = 1L;
        long value = 10L;
        insert( key, value );
        long oldGenId = cursor.getCurrentPageId();

        // WHEN root -[newGen]-> newGen root
        checkpointer.checkpoint();
        remove( key, readValue );
        long newGenId = cursor.getCurrentPageId();

        // THEN
        assertTrue( structurePropagation.hasNewGen );
        assertEquals( newGenId, structurePropagation.left );
        assertNotEquals( oldGenId, newGenId );
        assertEquals( 0, node.keyCount( cursor ) );

        node.goTo( cursor, oldGenId, stableGen, unstableGen );
        assertEquals( newGenId, node.newGen( cursor, stableGen, unstableGen ) );
        assertEquals( 1, node.keyCount( cursor ) );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation );
            }
        }
        assertEquals( 2, node.keyCount( cursor ) );
        long leftChild = node.childAt( cursor, 0, stableGen, unstableGen );
        long middleChild = node.childAt( cursor, 1, stableGen, unstableGen );
        long rightChild = node.childAt( cursor, 2, stableGen, unstableGen );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        checkpointer.checkpoint();
        long root = cursor.getCurrentPageId();
        long middleKey = i / 2; // Should be located in middle leaf
        long newValue = middleKey * 100;
        insert( middleKey, newValue );
        cursor.next( 5 );
        cursor.next( root );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = node.childAt( cursor, 1, stableGen, unstableGen );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has new gen
        cursor.next( middleChild );
        assertEquals( newMiddleChild, node.newGen( cursor, stableGen, unstableGen ) );

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
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation );
            }
        }
        assertEquals( 2, node.keyCount( cursor ) );
        long leftChild = node.childAt( cursor, 0, stableGen, unstableGen );
        long middleChild = node.childAt( cursor, 1, stableGen, unstableGen );
        long rightChild = node.childAt( cursor, 2, stableGen, unstableGen );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        checkpointer.checkpoint();
        long root = cursor.getCurrentPageId();
        long middleKey = i / 2; // Should be located in middle leaf
        remove( middleKey, insertValue );
        cursor.next( 5 );
        cursor.next( root );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = node.childAt( cursor, 1, stableGen, unstableGen );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has new gen
        cursor.next( middleChild );
        assertEquals( newMiddleChild, node.newGen( cursor, stableGen, unstableGen ) );

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
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN:
        //                       root
        //                   ----   ----
        //                  /           \
        //                 v             v
        //               left <-------> right
        node.initializeLeaf( cursor, stableGen, unstableGen );
        long i = 0;
        int countToProduceAboveImageAndFullRight =
                maxKeyCount /*will split root leaf into two half left/right*/ + maxKeyCount / 2;
        for ( ; i < countToProduceAboveImageAndFullRight; i++ )
        {
            insert( i, i );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation );
            }
        }
        long root = cursor.getCurrentPageId();
        assertEquals( 1, node.keyCount( cursor ) );
        long leftChild = node.childAt( cursor, 0, stableGen, unstableGen );
        long rightChild = node.childAt( cursor, 1, stableGen, unstableGen );
        assertSiblings( leftChild, rightChild, TreeNode.NO_NODE_FLAG );

        // WHEN
        //                       root(newGen)
        //                   ----  | ---------------
        //                  /      |                \
        //                 v       v                 v
        //               left <-> right(newGen) <--> farRight
        checkpointer.checkpoint();
        insert( i, i );
        assertTrue( structurePropagation.hasNewGen );
        long newRoot = cursor.getCurrentPageId();
        leftChild = node.childAt( cursor, 0, stableGen, unstableGen );
        rightChild = node.childAt( cursor, 1, stableGen, unstableGen );

        // THEN
        // siblings are correct
        long farRightChild = node.childAt( cursor, 2, stableGen, unstableGen );
        assertSiblings( leftChild, rightChild, farRightChild );

        // old root points to new gen root
        cursor.next( root );
        assertEquals( newRoot, node.newGen( cursor, stableGen, unstableGen ) );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableInternal() throws Exception
    {
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN:
        node.initializeLeaf( cursor, stableGen, unstableGen );
        int rootAllocations = 0;
        for ( int i = 0; rootAllocations < 2; i++ )
        {
            long keyAndValue = i * maxKeyCount;
            insert( keyAndValue, keyAndValue );
            if ( structurePropagation.hasSplit )
            {
                newRootFromSplit( structurePropagation );
                rootAllocations++;
            }
        }
        long root = cursor.getCurrentPageId();
        assertEquals( 1, node.keyCount( cursor ) );
        long leftInternal = node.childAt( cursor, 0, stableGen, unstableGen );
        long rightInternal = node.childAt( cursor, 1, stableGen, unstableGen );
        assertSiblings( leftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
        cursor.next( leftInternal );
        int leftInternalKeyCount = node.keyCount( cursor );
        assertTrue( node.isInternal( cursor ) );
        long leftLeaf = node.childAt( cursor, 0, stableGen, unstableGen );
        cursor.next( leftLeaf );
        long firstKeyInLeaf = node.keyAt( cursor, readKey, 0 ).longValue();
        cursor.next( root );

        // WHEN
        checkpointer.checkpoint();
        long targetLastId = id.lastId() + 3; /*one for newGen in leaf, one for split leaf, one for newGen in internal*/
        for ( int i = 0; id.lastId() < targetLastId; i++ )
        {
            insert( firstKeyInLeaf + i, firstKeyInLeaf + i );
            assertFalse( structurePropagation.hasSplit ); // there should be no root split
        }

        // THEN
        // root hasn't been split further
        assertEquals( root, cursor.getCurrentPageId() );

        // there's a new generation of left internal w/ one more key in
        long newGenLeftInternal = id.lastId();
        assertEquals( newGenLeftInternal, node.childAt( cursor, 0, stableGen, unstableGen ) );
        cursor.next( newGenLeftInternal );
        int newGenLeftInternalKeyCount = node.keyCount( cursor );
        assertEquals( leftInternalKeyCount + 1, newGenLeftInternalKeyCount );

        // and left internal points to the new gen
        cursor.next( leftInternal );
        assertEquals( newGenLeftInternal, node.newGen( cursor, stableGen, unstableGen ) );
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
        assertEquals( right, node.rightSibling( cursor, stableGen, unstableGen ) );
        assertEquals( left, node.leftSibling( cursor, stableGen, unstableGen ) );
        if ( left != TreeNode.NO_NODE_FLAG )
        {
            cursor.next( left );
            assertEquals( middle, node.rightSibling( cursor, stableGen, unstableGen ) );
        }
        if ( right != TreeNode.NO_NODE_FLAG )
        {
            cursor.next( right );
            assertEquals( middle, node.leftSibling( cursor, stableGen, unstableGen ) );
        }
        cursor.next( origin );
    }

    // KEEP even if unused
    private void printTree() throws IOException
    {
        TreePrinter.printTree( cursor, node, layout, stableGen, unstableGen, System.out );
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
        node.initializeInternal( cursor, stableGen, unstableGen );
        node.insertKeyAt( cursor, split.primKey, 0, 0, tmp );
        node.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.left, 0, stableGen, unstableGen );
        node.setChildAt( cursor, split.right, 1, stableGen, unstableGen );
        split.hasSplit = false;
        return rootId;
    }

    private void assertSiblingOrderAndPointers( long... children ) throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        RightmostInChain<MutableLong> rightmost =
                new RightmostInChain<>( node, stableGen, unstableGen );
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
                stableGen, unstableGen );
    }

    private MutableLong remove( long key, MutableLong into ) throws IOException
    {
        insertKey.setValue( key );
        return treeLogic.remove( cursor, structurePropagation, insertKey, into, stableGen, unstableGen );
    }

    @FunctionalInterface
    private interface Checkpointer
    {
        void checkpoint();
    }
}
