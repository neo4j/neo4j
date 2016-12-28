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
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.index.ValueMergers.overwrite;
import static org.neo4j.index.gbptree.ConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointer;

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
    private final StructurePropagation<MutableLong> structurePropagation = new StructurePropagation<>( layout.newKey() );

    private static long stableGen = GenSafePointer.MIN_GENERATION;
    private static long unstableGen = stableGen + 1;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> generators()
    {
        List<Object[]> parameters = new ArrayList<>();
        // Initial state has same generation as update state
        parameters.add( new Object[]{
                "NoCheckpoint", GenerationManager.NO_OP_GEN, false} );
        // Update state in next generation
        parameters.add( new Object[]{
                "Checkpoint", GenerationManager.DEFAULT, true} );
        return parameters;
    }

    @Parameterized.Parameter( 0 )
    public String name;
    @Parameterized.Parameter( 1 )
    public GenerationManager generationManager;
    @Parameterized.Parameter( 2 )
    public boolean isCheckpointing;

    @Rule
    public RandomRule random = new RandomRule();

    private long rootId;
    private long rootGen;
    private int numberOfRootSplits;
    private int numberOfRootNewGens;

    @Before
    public void setUp() throws IOException
    {
        id.reset();
        long newId = id.acquireNewId( stableGen, unstableGen );
        goTo( cursor, newId );
    }

    @Test
    public void modifierMustInsertAtFirstPositionInEmptyLeaf() throws Exception
    {
        // given
        initialize();
        long key = 1L;
        long value = 1L;
        assertThat( node.keyCount( cursor ), is( 0 ) );

        // when
        generationManager.checkpoint();
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
        initialize();
        generationManager.checkpoint();
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
        initialize();
        generationManager.checkpoint();
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
        initialize();
        generationManager.checkpoint();
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
        initialize();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i % 2 == 0 ? i : maxKeyCount * 2 - i;
            insert( key, key );
        }

        // when
        generationManager.checkpoint();
        long middle = maxKeyCount;
        insert( middle, middle );

        // then
        assertEquals( 1, numberOfRootSplits );
    }

    @Test
    public void modifierMustSplitWhenInsertingLastInFullLeaf() throws Exception
    {
        // given
        initialize();
        long key = 0;
        while ( key < maxKeyCount )
        {
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
            key++;
        }

        // when
        generationManager.checkpoint();
        insert( key, key );

        // then
        assertEquals( 1, numberOfRootSplits ); // Should cause a split
    }

    @Test
    public void modifierMustSplitWhenInsertingFirstInFullLeaf() throws Exception
    {
        // given
        initialize();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i + 1;
            insert( key, key );
            assertFalse( structurePropagation.hasSplit );
        }

        // when
        generationManager.checkpoint();
        insert( 0L, 0L );

        // then
        assertEquals( 1, numberOfRootSplits );
    }

    @Test
    public void modifierMustUpdatePointersInSiblingsToSplit() throws Exception
    {
        // given
        initialize();
        long someLargeNumber = maxKeyCount * 1000;
        long i = 0;
        while ( i < maxKeyCount )
        {
            insert( someLargeNumber - i, i );
            i++;
        }

        // First split
        generationManager.checkpoint();
        insert( someLargeNumber - i, i );
        i++;

        // Assert child pointers and sibling pointers are intact after split in root
        long child0 = childAt( cursor, 0, stableGen, unstableGen );
        long child1 = childAt( cursor, 1, stableGen, unstableGen );
        assertSiblingOrderAndPointers( child0, child1 );

        // Insert until we have another split in leftmost leaf
        while ( keyCount( rootId ) == 1 )
        {
            insert( someLargeNumber - i, i );
            i++;
        }
        goTo( cursor, rootId );

        // Just to be sure
        assertTrue( node.isInternal( cursor ) );
        assertThat( node.keyCount( cursor ), is( 2 ) );

        // Assert child pointers and sibling pointers are intact
        // AND that node not involved in split also has its left sibling pointer updated
        child0 = childAt( cursor, 0, stableGen, unstableGen );
        child1 = childAt( cursor, 1, stableGen, unstableGen );
        long child2 = childAt( cursor, 2, stableGen, unstableGen ); // <- right sibling to split-node before split

        assertSiblingOrderAndPointers( child0, child1, child2 );
    }

    /* REMOVE */
    @Test
    public void modifierMustRemoveFirstInEmptyLeaf() throws Exception
    {
        // given
        initialize();
        long key = 1L;
        long value = 1L;
        insert( key, value );

        // when
        generationManager.checkpoint();
        remove( key, readValue );

        // then
        assertThat( node.keyCount( cursor ), is( 0 ) );
    }

    @Test
    public void modifierMustRemoveFirstInFullLeaf() throws Exception
    {
        // given
        initialize();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        generationManager.checkpoint();
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
        initialize();
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        generationManager.checkpoint();
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
        initialize();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        generationManager.checkpoint();
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
        initialize();
        for ( int i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( i, i );
        }

        // when
        generationManager.checkpoint();
        goTo( cursor, structurePropagation.left );
        assertThat( keyAt( 0 ), is( 0L ) );
        goTo( cursor, rootId );
        remove( 0, readValue );

        // then
        goTo( cursor, structurePropagation.left );
        assertThat( keyAt( 0 ), is( 1L ) );
    }

    @Test
    public void modifierMustRemoveFromRightChildButNotFromInternalWithHitOnInternalSearch() throws Exception
    {
        initialize();
        for ( int i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( i, i );
        }

        // when key to remove exists in internal
        Long keyToRemove = structurePropagation.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        long rightChild = structurePropagation.right;
        goTo( cursor, rightChild );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        generationManager.checkpoint();
        goTo( cursor, rootId );
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        goTo( cursor, rootId );
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        rightChild = childAt( cursor, 1, stableGen, unstableGen );
        goTo( cursor, rightChild );
        assertThat( node.keyCount( cursor ), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );
    }

    @Test
    public void modifierMustNotRemoveWhenKeyDoesNotExist() throws Exception
    {
        // given
        initialize();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        generationManager.checkpoint();
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
        initialize();
        for ( int i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( i, i );
        }

        // when key to remove exists in internal
        Long keyToRemove = structurePropagation.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        long currentRightChild = structurePropagation.right;
        goTo( cursor, currentRightChild );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        generationManager.checkpoint();
        goTo( cursor, rootId );
        remove( keyToRemove, readValue ); // Possibly create new gen of right child
        long pageIdBeforeVerification = cursor.getCurrentPageId();
        goTo( cursor, rootId );
        currentRightChild = childAt( cursor, 1, stableGen, unstableGen );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        goTo( cursor, currentRightChild );
        assertThat( node.keyCount( cursor ), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );

        // and when we remove same key again, nothing should change
        goTo( cursor, pageIdBeforeVerification );
        assertNull( remove( keyToRemove, readValue ) );
    }

    /* REBALANCE (when rebalance is implemented) */
    /* MERGE (when merge is implemented) */

    @Test
    public void modifierMustProduceConsistentTreeWithRandomInserts() throws Exception
    {
        // given
        initialize();
        int numberOfEntries = 100_000;
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            // when
            insert( random.nextLong(), random.nextLong() );
            if ( i == numberOfEntries / 2 )
            {
                generationManager.checkpoint();
            }
        }

        // then
        goTo( cursor, rootId );
        ConsistencyChecker<MutableLong> consistencyChecker =
                new ConsistencyChecker<>( node, layout, stableGen, unstableGen );
        consistencyChecker.check( cursor, rootGen );
    }

    /* TEST VALUE MERGER */

    @Test
    public void modifierMustOverwriteWithOverwriteMerger() throws Exception
    {
        // given
        initialize();
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue );

        // when
        generationManager.checkpoint();
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
        initialize();
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue, ValueMergers.keepExisting() );
        assertThat( node.keyCount( cursor ), is( 1 ) );
        Long actual = valueAt( 0 );
        assertThat( actual, is( firstValue ) );

        // when
        generationManager.checkpoint();
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
        initialize();
        long key = 10;
        long baseValue = 100;
        insert( key, baseValue );

        // WHEN
        generationManager.checkpoint();
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
        initialize();
        for ( int i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( i, i );
        }

        // WHEN
        generationManager.checkpoint();
        long key = 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        goTo( cursor, structurePropagation.left );
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
        initialize();
        for ( int i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( i, i );
        }

        // WHEN
        generationManager.checkpoint();
        long key = structurePropagation.primKey.longValue();
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        goTo( cursor, rootId );
        long rightChild = childAt( cursor, 1, stableGen, unstableGen );
        goTo( cursor, rightChild );
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
        initialize();
        long firstSplitPrimKey = -1;
        for ( int i = 0; numberOfRootSplits == 0 || keyCount( rootId ) < 1; i++ )
        {
            insert( i, i );
            if ( firstSplitPrimKey == -1 && numberOfRootSplits == 1 )
            {
                firstSplitPrimKey = structurePropagation.primKey.longValue();
            }
        }

        // WHEN
        generationManager.checkpoint();
        long key = firstSplitPrimKey + 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        goTo( cursor, rootId );
        long middle = childAt( cursor, 1, stableGen, unstableGen );
        goTo( cursor, middle );
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
        initialize();
        long oldGenId = cursor.getCurrentPageId();

        // WHEN root -[newGen]-> newGen root
        generationManager.checkpoint();
        insert( 1L, 1L );
        long newGenId = cursor.getCurrentPageId();

        // THEN
        assertEquals( 1, numberOfRootNewGens );
        assertEquals( newGenId, structurePropagation.left );
        assertNotEquals( oldGenId, newGenId );
        assertEquals( 1, node.keyCount( cursor ) );

        node.goTo( cursor, "old gen", oldGenId );
        assertEquals( newGenId, newGen( cursor, stableGen, unstableGen ) );
        assertEquals( 0, node.keyCount( cursor ) );
    }

    @Test
    public void shouldCreateNewVersionWhenRemoveInStableRootAsLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN root
        initialize();
        long key = 1L;
        long value = 10L;
        insert( key, value );
        long oldGenId = cursor.getCurrentPageId();

        // WHEN root -[newGen]-> newGen root
        generationManager.checkpoint();
        remove( key, readValue );
        long newGenId = cursor.getCurrentPageId();

        // THEN
        assertEquals( 1, numberOfRootNewGens );
        assertEquals( newGenId, structurePropagation.left );
        assertNotEquals( oldGenId, newGenId );
        assertEquals( 0, node.keyCount( cursor ) );

        node.goTo( cursor, "old gen", oldGenId );
        assertEquals( newGenId, newGen( cursor, stableGen, unstableGen ) );
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
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i );
        }
        assertEquals( 2, node.keyCount( cursor ) );
        long leftChild = childAt( cursor, 0, stableGen, unstableGen );
        long middleChild = childAt( cursor, 1, stableGen, unstableGen );
        long rightChild = childAt( cursor, 2, stableGen, unstableGen );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        generationManager.checkpoint();
        long root = cursor.getCurrentPageId();
        long middleKey = i / 2; // Should be located in middle leaf
        long newValue = middleKey * 100;
        insert( middleKey, newValue );
        goTo( cursor, 5 );
        goTo( cursor, root );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = childAt( cursor, 1, stableGen, unstableGen );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has new gen
        goTo( cursor, middleChild );
        assertEquals( newMiddleChild, newGen( cursor, stableGen, unstableGen ) );

        // old middle child has seen no change
        assertKeyAssociatedWithValue( middleKey, middleKey );

        // new middle child has seen change
        goTo( cursor, newMiddleChild );
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
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i );
        }
        assertEquals( 2, node.keyCount( cursor ) );
        long leftChild = childAt( cursor, 0, stableGen, unstableGen );
        long middleChild = childAt( cursor, 1, stableGen, unstableGen );
        long rightChild = childAt( cursor, 2, stableGen, unstableGen );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        generationManager.checkpoint();
        long root = cursor.getCurrentPageId();
        long middleKey = i / 2; // Should be located in middle leaf
        remove( middleKey, insertValue );
        goTo( cursor, 5 );
        goTo( cursor, root );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = childAt( cursor, 1, stableGen, unstableGen );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has new gen
        goTo( cursor, middleChild );
        assertEquals( newMiddleChild, newGen( cursor, stableGen, unstableGen ) );

        // old middle child has seen no change
        assertKeyAssociatedWithValue( middleKey, middleKey );

        // new middle child has seen change
        goTo( cursor, newMiddleChild );
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
        initialize();
        long i = 0;
        int countToProduceAboveImageAndFullRight =
                maxKeyCount /*will split root leaf into two half left/right*/ + maxKeyCount / 2;
        for ( ; i < countToProduceAboveImageAndFullRight; i++ )
        {
            insert( i, i );
        }
        long oldRootId = rootId;
        long prevId = cursor.getCurrentPageId();
        goTo( cursor, rootId );
        assertEquals( 1, node.keyCount( cursor ) );
        long leftChild = childAt( cursor, 0, stableGen, unstableGen );
        long rightChild = childAt( cursor, 1, stableGen, unstableGen );
        assertSiblings( leftChild, rightChild, TreeNode.NO_NODE_FLAG );

        // WHEN
        //                       root(newGen)
        //                   ----  | ---------------
        //                  /      |                \
        //                 v       v                 v
        //               left <-> right(newGen) <--> farRight
        generationManager.checkpoint();
        goTo( cursor, prevId );
        insert( i, i );
        assertEquals( 1, numberOfRootNewGens );
        long newRoot = cursor.getCurrentPageId();
        leftChild = childAt( cursor, 0, stableGen, unstableGen );
        rightChild = childAt( cursor, 1, stableGen, unstableGen );

        // THEN
        // siblings are correct
        long farRightChild = childAt( cursor, 2, stableGen, unstableGen );
        assertSiblings( leftChild, rightChild, farRightChild );

        // old root points to new gen root
        goTo( cursor, oldRootId );
        assertEquals( newRoot, newGen( cursor, stableGen, unstableGen ) );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableInternal() throws Exception
    {
        assumeTrue( "No checkpointing, no new gen", isCheckpointing );

        // GIVEN
        initialize();
        for ( int i = 0; numberOfRootSplits < 2; i++ )
        {
            long keyAndValue = i * maxKeyCount;
            insert( keyAndValue, keyAndValue );
        }
        long root = cursor.getCurrentPageId();
        assertEquals( 1, node.keyCount( cursor ) );
        long leftInternal = childAt( cursor, 0, stableGen, unstableGen );
        long rightInternal = childAt( cursor, 1, stableGen, unstableGen );
        assertSiblings( leftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
        goTo( cursor, leftInternal );
        int leftInternalKeyCount = node.keyCount( cursor );
        assertTrue( node.isInternal( cursor ) );
        long leftLeaf = childAt( cursor, 0, stableGen, unstableGen );
        goTo( cursor, leftLeaf );
        long firstKeyInLeaf = node.keyAt( cursor, readKey, 0 ).longValue();
        goTo( cursor, root );

        // WHEN
        generationManager.checkpoint();
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
        assertEquals( newGenLeftInternal, childAt( cursor, 0, stableGen, unstableGen ) );
        goTo( cursor, newGenLeftInternal );
        int newGenLeftInternalKeyCount = node.keyCount( cursor );
        assertEquals( leftInternalKeyCount + 1, newGenLeftInternalKeyCount );

        // and left internal points to the new gen
        goTo( cursor, leftInternal );
        assertEquals( newGenLeftInternal, newGen( cursor, stableGen, unstableGen ) );
        assertSiblings( newGenLeftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
    }

    @Test
    public void shouldOverwriteInheritedNewGenOnNewGen() throws Exception
    {
        // GIVEN
        assumeTrue( isCheckpointing );
        initialize();
        long originalNodeId = cursor.getCurrentPageId();
        generationManager.checkpoint();
        insert( 1L, 10L ); // TX1 will create heir
        assertEquals( 1, numberOfRootNewGens );

        // WHEN
        // recovery happens
        generationManager.recovery();
        // start up on stable root
        goTo( cursor, originalNodeId );
        treeLogic.initialize( cursor );
        // replay transaction TX1 will create a new heir
        insert( 1L, 10L );
        assertEquals( 2, numberOfRootNewGens );

        // THEN
        // new gen pointer for heir should not have broken or crashed GSPP slot
        assertNewGenPointerNotCrashOrBroken();
        // and previously crashed new gen GSPP slot should have been overwritten
        goTo( cursor, originalNodeId );
        assertNewGenPointerNotCrashOrBroken();
    }

    private int keyCount( long nodeId ) throws IOException
    {
        long prevId = cursor.getCurrentPageId();
        try
        {
            goTo( cursor, nodeId );
            return node.keyCount( cursor );
        }
        finally
        {
            goTo( cursor, prevId );
        }
    }

    private void initialize()
    {
        node.initializeLeaf( cursor, stableGen, unstableGen );
        updateRoot();
    }

    private void updateRoot()
    {
        rootId = cursor.getCurrentPageId();
        rootGen = unstableGen;
        treeLogic.initialize( cursor );
    }

    private void assertNewGenPointerNotCrashOrBroken()
    {
        assertNoCrashOrBrokenPointerInGSPP( cursor, stableGen, unstableGen, "NewGen", TreeNode.BYTE_POS_NEWGEN, node );
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
        goTo( cursor, middle );
        assertEquals( right, rightSibling( cursor, stableGen, unstableGen ) );
        assertEquals( left, leftSibling( cursor, stableGen, unstableGen ) );
        if ( left != TreeNode.NO_NODE_FLAG )
        {
            goTo( cursor, left );
            assertEquals( middle, rightSibling( cursor, stableGen, unstableGen ) );
        }
        if ( right != TreeNode.NO_NODE_FLAG )
        {
            goTo( cursor, right );
            assertEquals( middle, leftSibling( cursor, stableGen, unstableGen ) );
        }
        goTo( cursor, origin );
    }

    // KEEP even if unused
    private void printTree() throws IOException
    {
        TreePrinter.printTree( cursor, node, layout, stableGen, unstableGen, System.out, true );
    }

    private static MutableLong key( long key )
    {
        return new MutableLong( key );
    }

    private void newRootFromSplit( StructurePropagation<MutableLong> split ) throws IOException
    {
        assertTrue( split.hasSplit );
        long rootId = id.acquireNewId( stableGen, unstableGen );
        goTo( cursor, rootId );
        node.initializeInternal( cursor, stableGen, unstableGen );
        node.insertKeyAt( cursor, split.primKey, 0, 0 );
        node.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.left, 0, stableGen, unstableGen );
        node.setChildAt( cursor, split.right, 1, stableGen, unstableGen );
        split.hasSplit = false;
        updateRoot();
    }

    private void assertSiblingOrderAndPointers( long... children ) throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        RightmostInChain<MutableLong> rightmost =
                new RightmostInChain<>( node, stableGen, unstableGen );
        for ( long child : children )
        {
            goTo( cursor, child );
            rightmost.assertNext( cursor );
        }
        rightmost.assertLast();
        goTo( cursor, currentPageId );
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
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, valueMerger, stableGen, unstableGen );
        handleAfterChange();
    }

    private void handleAfterChange() throws IOException
    {
        if ( structurePropagation.hasSplit )
        {
            newRootFromSplit( structurePropagation );
            numberOfRootSplits++;
        }
        if ( structurePropagation.hasNewGen )
        {
            structurePropagation.hasNewGen = false;
            updateRoot();
            numberOfRootNewGens++;
        }
    }

    private MutableLong remove( long key, MutableLong into ) throws IOException
    {
        insertKey.setValue( key );
        MutableLong result = treeLogic.remove( cursor, structurePropagation, insertKey, into, stableGen, unstableGen );
        handleAfterChange();
        return result;
    }

    private interface GenerationManager
    {
        void checkpoint();

        void recovery();

        static GenerationManager NO_OP_GEN = new GenerationManager()
        {
            @Override
            public void checkpoint()
            {
                // Do nothing
            }

            @Override
            public void recovery()
            {
                // Do nothing
            }
        };

        static GenerationManager DEFAULT = new GenerationManager()
        {
            @Override
            public void checkpoint()
            {
                stableGen = unstableGen;
                unstableGen++;
            }

            @Override
            public void recovery()
            {
                unstableGen++;
            }
        };
    }

    private static void goTo( PageCursor cursor, long pageId ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "test", pointer( pageId ) );
    }

    private long childAt( PageCursor cursor, int pos, long stableGen, long unstableGen )
    {
        return pointer( node.childAt( cursor, pos, stableGen, unstableGen ) );
    }

    private long rightSibling( PageCursor cursor, long stableGen, long unstableGen )
    {
        return pointer( node.rightSibling( cursor, stableGen, unstableGen ) );
    }

    private long leftSibling( PageCursor cursor, long stableGen, long unstableGen )
    {
        return pointer( node.leftSibling( cursor, stableGen, unstableGen ) );
    }

    private long newGen( PageCursor cursor, long stableGen, long unstableGen )
    {
        return pointer( node.newGen( cursor, stableGen, unstableGen ) );
    }
}
