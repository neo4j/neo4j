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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.index.internal.gbptree.ConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;

@RunWith( Parameterized.class )
public class InternalTreeLogicTest
{
    private static final ValueMerger<MutableLong,MutableLong> ADDER = ( existingKey, newKey, base, add ) ->
    {
        base.add( add.longValue() );
        return base;
    };

    private final int pageSize = 256;

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( pageSize, layout );
    private final InternalTreeLogic<MutableLong,MutableLong> treeLogic = new InternalTreeLogic<>( id, node, layout );

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );
    private final PageAwareByteArrayCursor readCursor = cursor.duplicate();
    private final int maxKeyCount = node.leafMaxKeyCount();

    private final MutableLong insertKey = new MutableLong();
    private final MutableLong insertValue = new MutableLong();
    private final MutableLong readKey = new MutableLong();
    private final MutableLong readValue = new MutableLong();
    private final StructurePropagation<MutableLong> structurePropagation = new StructurePropagation<>(
            layout.newKey(), layout.newKey(), layout.newKey() );

    private static long stableGeneration = GenerationSafePointer.MIN_GENERATION;
    private static long unstableGeneration = stableGeneration + 1;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> generators()
    {
        List<Object[]> parameters = new ArrayList<>();
        // Initial state has same generation as update state
        parameters.add( new Object[]{
                "NoCheckpoint", GenerationManager.NO_OP_GENERATION, false} );
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
    private long rootGeneration;
    private int numberOfRootSplits;
    private int numberOfRootSuccessors;

    @Before
    public void setUp() throws IOException
    {
        id.reset();
        long newId = id.acquireNewId( stableGeneration, unstableGeneration );
        goTo( cursor, newId );
        readCursor.next( newId );
    }

    @Test
    public void modifierMustInsertAtFirstPositionInEmptyLeaf() throws Exception
    {
        // given
        initialize();
        long key = 1L;
        long value = 1L;
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 0 ) );

        // when
        generationManager.checkpoint();
        insert( key, value );

        // then
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
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
            readCursor.next( rootId );
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
            readCursor.next( rootId );
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
            readCursor.next( rootId );
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
            assertFalse( structurePropagation.hasRightKeyInsert );
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
            assertFalse( structurePropagation.hasRightKeyInsert );
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
        goTo( readCursor, rootId );
        long child0 = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long child1 = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertSiblingOrderAndPointers( child0, child1 );

        // Insert until we have another split in leftmost leaf
        while ( keyCount( rootId ) == 1 )
        {
            insert( someLargeNumber - i, i );
            i++;
        }

        // Just to be sure
        assertTrue( TreeNode.isInternal( readCursor ) );
        assertThat( TreeNode.keyCount( readCursor ), is( 2 ) );

        // Assert child pointers and sibling pointers are intact
        // AND that node not involved in split also has its left sibling pointer updated
        child0 = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        child1 = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long child2 = childAt( readCursor, 2, stableGeneration, unstableGeneration ); // <- right sibling to split-node before split

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
        goTo( readCursor, rootId );
        assertThat( TreeNode.keyCount( cursor ), is( 0 ) );
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
        goTo( readCursor, rootId );
        assertThat( TreeNode.keyCount( readCursor ), is( maxKeyCount - 1 ) );
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
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( maxKeyCount - 1 ) );
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
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( maxKeyCount - 1 ) );
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
        goTo( readCursor, structurePropagation.midChild );
        assertThat( keyAt( 0 ), is( 0L ) );
        remove( 0, readValue );

        // then
        goTo( readCursor, structurePropagation.midChild );
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
        Long keyToRemove = structurePropagation.rightKey.getValue();
        goTo( readCursor, rootId );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        long rightChild = structurePropagation.rightChild;
        goTo( readCursor, rightChild );
        int keyCountInRightChild = keyCount();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        generationManager.checkpoint();
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, rightChild );
        assertThat( keyCount(), is( keyCountInRightChild - 1 ) );
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
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( maxKeyCount ) );
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
        Long keyToRemove = structurePropagation.rightKey.getValue();
        assertThat( keyAt( rootId, 0 ), is( keyToRemove ) );

        // and as first key in right child
        long currentRightChild = structurePropagation.rightChild;
        goTo( readCursor, currentRightChild );
        int keyCountInRightChild = keyCount();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        generationManager.checkpoint();
        remove( keyToRemove, readValue ); // Possibly create successor of right child
        goTo( readCursor, rootId );
        currentRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // then we should still find it in internal
        assertThat( keyCount(), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        goTo( readCursor, currentRightChild );
        assertThat( keyCount(), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );

        // and when we remove same key again, nothing should change
        assertNull( remove( keyToRemove, readValue ) );
    }

    /* REBALANCE */

    @Test
    public void mustNotRebalanceFromRightToLeft() throws Exception
    {
        // given
        initialize();
        long key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( key, key );
            key++;
        }

        // ... enough keys in right child to share with left child if rebalance is needed
        insert( key, key );
        key++;

        // ... and the prim key diving key range for left child and right child
        goTo( readCursor, rootId );
        long primKey = keyAt( 0 );

        // when
        // ... removing all keys from left child
        for ( long i = 0; i < primKey; i++ )
        {
            remove( i, readValue );
        }

        // then
        // ... looking a right child
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, rightChild );

        // ... no keys should have moved from right sibling
        int pos = 0;
        long expected = primKey;
        while ( expected < key )
        {
            assertThat( keyAt( pos ), is( expected ) );
            pos++;
            expected++;
        }
    }

    @Test
    public void mustPropagateAllStructureChanges() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        //given
        initialize();
        long key = 10;
        while ( numberOfRootSplits == 0 )
        {
            insert( key, key );
            key++;
        }
        // ... enough keys in left child to share with right child if rebalance is needed
        for ( long smallKey = 0; smallKey < 2; smallKey++ )
        {
            insert( smallKey, smallKey );
        }

        // ... and the prim key dividing key range for left and right child
        goTo( readCursor, rootId );
        long oldPrimKey = keyAt( 0 );

        // ... and left and right child
        long originalLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long originalRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, originalRightChild );
        List<Long> keysInRightChild = allKeys( readCursor );

        // when
        // ... after checkpoint
        generationManager.checkpoint();

        // ... removing keys from right child until rebalance is triggered
        int index = 0;
        long rightChild;
        long leftmostInRightChild;
        do
        {
            remove( keysInRightChild.get( index ), readValue );
            index++;
            goTo( readCursor, rootId );
            rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
            goTo( readCursor, rightChild );
            leftmostInRightChild = keyAt( 0 );
        } while ( leftmostInRightChild >= keysInRightChild.get( 0 ) );

        // then
        // ... primKey in root is updated
        goTo( readCursor, rootId );
        Long primKey = keyAt( 0 );
        assertThat( primKey, is( leftmostInRightChild ) );
        assertThat( primKey, is( not( oldPrimKey ) ) );

        // ... new versions of left and right child
        long newLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long newRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertThat( newLeftChild, is( not( originalLeftChild ) ) );
        assertThat( newRightChild, is( not( originalRightChild ) ) );
    }

    /* MERGE */

    @Test
    public void mustPropagateStructureOnMergeFromLeft() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        List<Long> allKeys = new ArrayList<>();
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i );
            allKeys.add( i );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );
        long oldLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long oldMiddleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long oldRightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( oldLeftChild, oldMiddleChild, oldRightChild );

        // WHEN
        generationManager.checkpoint();
        long middleKey = keyAt( 0 ) + 1; // Should be located in middle leaf
        remove( middleKey, insertValue );
        allKeys.remove( middleKey );

        // THEN
        // old root should still have 2 keys
        assertEquals( 2, keyCount() );

        // new root should have only 1 key
        goTo( readCursor, rootId );
        assertEquals( 1, keyCount() );

        // left child should be a new node
        long newLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        assertNotEquals( newLeftChild, oldLeftChild );
        assertNotEquals( newLeftChild, oldMiddleChild );

        // right child should be same old node
        long newRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertEquals( newRightChild, oldRightChild );

        // old left and old middle has new left as successor
        goTo( readCursor, oldLeftChild );
        assertEquals( newLeftChild, successor( readCursor, stableGeneration, unstableGeneration ) );
        goTo( readCursor, oldMiddleChild );
        assertEquals( newLeftChild, successor( readCursor, stableGeneration, unstableGeneration ) );

        // new left child contain keys from old left and old middle
        goTo( readCursor, oldRightChild );
        Long firstKeyOfOldRightChild = keyAt( 0 );
        List<Long> expectedKeysInNewLeftChild = allKeys.subList( 0, allKeys.indexOf( firstKeyOfOldRightChild ) );
        goTo( readCursor, newLeftChild );
        assertNodeContainsExpectedKeys( expectedKeysInNewLeftChild );

        // new children are siblings
        assertSiblings( newLeftChild, oldRightChild, TreeNode.NO_NODE_FLAG );
    }

    @Test
    public void mustPropagateStructureOnMergeToRight() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        // GIVEN:
        //        ---------root---------
        //       /           |          \
        //      v            v           v
        //   oldleft <-> oldmiddle <-> oldright
        List<Long> allKeys = new ArrayList<>();
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            insert( i, i );
            allKeys.add( i );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );
        long oldLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long oldMiddleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long oldRightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( oldLeftChild, oldMiddleChild, oldRightChild );

        // WHEN
        generationManager.checkpoint();
        long keyInLeftChild = keyAt( 0 ) - 1; // Should be located in left leaf
        remove( keyInLeftChild, insertValue );
        allKeys.remove( keyInLeftChild );
        // New structure
        // NOTE: oldleft gets a successor (intermediate) before removing key and then another one once it is merged,
        //       effectively creating a chain of successor pointers to our newleft that in the end contain keys from
        //       oldleft and oldmiddle
        //                                                  ----root----
        //                                                 /            |
        //                                                v             v
        // oldleft -[successor]-> intermediate -[successor]-> newleft <-> oldright
        //                                                ^
        //                                                  \-[successor]- oldmiddle

        // THEN
        // old root should still have 2 keys
        assertEquals( 2, keyCount() );

        // new root should have only 1 key
        goTo( readCursor, rootId );
        assertEquals( 1, keyCount() );

        // left child should be a new node
        long newLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        assertNotEquals( newLeftChild, oldLeftChild );
        assertNotEquals( newLeftChild, oldMiddleChild );

        // right child should be same old node
        long newRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertEquals( newRightChild, oldRightChild );

        // old left and old middle has new left as successor
        goTo( readCursor, oldLeftChild );
        assertEquals( newLeftChild, newestGeneration( readCursor, stableGeneration, unstableGeneration ) );
        goTo( readCursor, oldMiddleChild );
        assertEquals( newLeftChild, successor( readCursor, stableGeneration, unstableGeneration ) );

        // new left child contain keys from old left and old middle
        goTo( readCursor, oldRightChild );
        Long firstKeyInOldRightChild = keyAt( 0 );
        List<Long> expectedKeysInNewLeftChild = allKeys.subList( 0, allKeys.indexOf( firstKeyInOldRightChild ) );
        goTo( readCursor, newLeftChild );
        assertNodeContainsExpectedKeys( expectedKeysInNewLeftChild );

        // new children are siblings
        assertSiblings( newLeftChild, oldRightChild, TreeNode.NO_NODE_FLAG );
    }

    @Test
    public void mustPropagateStructureWhenMergingBetweenDifferentSubtrees() throws Exception
    {
        // GIVEN
        // We will merge oldLeft into oldRight
        //                               -----root----
        //                             /               \
        //                            v                 v
        //                 _____leftParent    <->      rightParent_____
        //                / / /         \              /           \ \ \
        //               v v v           v            v             v v v
        // [some more children]       oldLeft <-> oldRight         [some more children]
        initialize();
        long i = 0;
        while ( numberOfRootSplits < 2 )
        {
            insert( i, i );
            i++;
        }

        long oldLeft = rightmostLeafInSubtree( rootId, 0 );
        long oldRight = leftmostLeafInSubtree( rootId, 1 );
        long oldSplitter = keyAt( 0 );
        long rightmostKeyInLeftSubtree = rightmostInternalKeyInSubtree( rootId, 0 );

        ArrayList<Long> allKeysInOldLeftAndOldRight = new ArrayList<>();
        goTo( readCursor, oldLeft );
        allKeys( readCursor, allKeysInOldLeftAndOldRight );
        goTo( readCursor, oldRight );
        allKeys( readCursor, allKeysInOldLeftAndOldRight );

        long keyInOldRight = keyAt( 0 );

        // WHEN
        generationManager.checkpoint();
        remove( keyInOldRight, readValue );
        allKeysInOldLeftAndOldRight.remove( keyInOldRight );

        // THEN
        // oldSplitter in root should have been replaced by rightmostKeyInLeftSubtree
        goTo( readCursor, rootId );
        Long newSplitter = keyAt( 0 );
        assertThat( newSplitter, is( not( oldSplitter ) ) );
        assertThat( newSplitter, is( rightmostKeyInLeftSubtree ) );

        // rightmostKeyInLeftSubtree should have been removed from successor version of leftParent
        long newRightmostInternalKeyInLeftSubtree = rightmostInternalKeyInSubtree( rootId, 0 );
        assertThat( newRightmostInternalKeyInLeftSubtree, is( not( rightmostKeyInLeftSubtree ) ) );

        // newRight contain all
        goToSuccessor( readCursor, oldRight );
        List<Long> allKeysInNewRight = allKeys( readCursor );
        assertEquals( allKeysInOldLeftAndOldRight, allKeysInNewRight );
    }

    @Test
    public void mustLeaveSingleLeafAsRootWhenEverythingIsRemoved() throws Exception
    {
        // GIVEN
        // a tree with some keys
        List<Long> allKeys = new ArrayList<>();
        initialize();
        long i = 0;
        while ( numberOfRootSplits < 3 )
        {
            insert( i, i );
            allKeys.add( i );
            i++;
        }

        // WHEN
        // removing all keys but one
        generationManager.checkpoint();
        for ( int j = 0; j < allKeys.size() - 1; j++ )
        {
            remove( allKeys.get( j ), readValue );
        }

        // THEN
        goTo( readCursor, rootId );
        assertTrue( TreeNode.isLeaf( readCursor ) );
    }

    /* OVERALL CONSISTENCY */

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
        goTo( readCursor, rootId );
        ConsistencyChecker<MutableLong> consistencyChecker =
                new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration );
        consistencyChecker.check( readCursor, rootGeneration );
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
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
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
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        Long actual = valueAt( 0 );
        assertThat( actual, is( firstValue ) );

        // when
        generationManager.checkpoint();
        long secondValue = random.nextLong();
        insert( key, secondValue, ValueMergers.keepExisting() );

        // then
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
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
        goTo( readCursor, rootId );
        int searchResult = KeySearch.search( readCursor, node, key( key ), new MutableLong(), keyCount() );
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
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        goTo( readCursor, structurePropagation.midChild );
        int searchResult = KeySearch.search( readCursor, node, key( key ), new MutableLong(), keyCount() );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 1, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( key + toAdd, valueAt( pos ).longValue() );
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
        long key = structurePropagation.rightKey.longValue();
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        goTo( readCursor, rootId );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, rightChild );
        int searchResult = KeySearch.search( readCursor, node, key( key ), new MutableLong(), keyCount() );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 0, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( key + toAdd, valueAt( pos ).longValue() );
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
                firstSplitPrimKey = structurePropagation.rightKey.longValue();
            }
        }

        // WHEN
        generationManager.checkpoint();
        long key = firstSplitPrimKey + 1;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        goTo( readCursor, rootId );
        long middle = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, middle );
        int searchResult = KeySearch.search( readCursor, node, key( key ), new MutableLong(), keyCount() );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 1, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( key + toAdd, valueAt( pos ).longValue() );
    }

    /* CREATE NEW VERSION ON UPDATE */

    @Test
    public void shouldCreateNewVersionWhenInsertInStableRootAsLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        // GIVEN root
        initialize();
        long oldGenerationId = cursor.getCurrentPageId();

        // WHEN root -[successor]-> successor of root
        generationManager.checkpoint();
        insert( 1L, 1L );
        long successor = cursor.getCurrentPageId();

        // THEN
        goTo( readCursor, rootId );
        assertEquals( 1, numberOfRootSuccessors );
        assertEquals( successor, structurePropagation.midChild );
        assertNotEquals( oldGenerationId, successor );
        assertEquals( 1, keyCount() );

        goTo( readCursor, oldGenerationId );
        assertEquals( successor, successor( readCursor, stableGeneration, unstableGeneration ) );
        assertEquals( 0, keyCount() );
    }

    @Test
    public void shouldCreateNewVersionWhenRemoveInStableRootAsLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        // GIVEN root
        initialize();
        long key = 1L;
        long value = 10L;
        insert( key, value );
        long oldGenerationId = cursor.getCurrentPageId();

        // WHEN root -[successor]-> successor of root
        generationManager.checkpoint();
        remove( key, readValue );
        long successor = cursor.getCurrentPageId();

        // THEN
        goTo( readCursor, rootId );
        assertEquals( 1, numberOfRootSuccessors );
        assertEquals( successor, structurePropagation.midChild );
        assertNotEquals( oldGenerationId, successor );
        assertEquals( 0, keyCount() );

        goTo( readCursor, oldGenerationId );
        assertEquals( successor, successor( readCursor, stableGeneration, unstableGeneration ) );
        assertEquals( 1, keyCount() );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

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
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long middleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        generationManager.checkpoint();
        long middleKey = i / 2; // Should be located in middle leaf
        long newValue = middleKey * 100;
        insert( middleKey, newValue );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has successor
        goTo( readCursor, middleChild );
        assertEquals( newMiddleChild, successor( readCursor, stableGeneration, unstableGeneration ) );

        // old middle child has seen no change
        assertKeyAssociatedWithValue( middleKey, middleKey );

        // new middle child has seen change
        goTo( readCursor, newMiddleChild );
        assertKeyAssociatedWithValue( middleKey, newValue );

        // sibling pointers updated
        assertSiblings( leftChild, newMiddleChild, rightChild );
    }

    @Test
    public void shouldCreateNewVersionWhenRemoveInStableLeaf() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        // GIVEN:
        //       ------root-------
        //      /        |         \
        //     v         v          v
        //   left <--> middle <--> right
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i += 2 )
        {
            insert( i, i );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );

        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long middleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );

        // add some more keys to middleChild to not have remove trigger a merge
        goTo( readCursor, middleChild );
        Long firstKeyInMiddleChild = keyAt( 0 );
        insert( firstKeyInMiddleChild + 1, firstKeyInMiddleChild + 1 );
        goTo( readCursor, rootId );

        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        generationManager.checkpoint();
        long middleKey = i / 2; // Should be located in middle leaf
        remove( middleKey, insertValue );

        // THEN
        // root have new middle child
        long expectedNewMiddleChild = targetLastId + 1;
        assertEquals( expectedNewMiddleChild, id.lastId() );
        long newMiddleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertEquals( expectedNewMiddleChild, newMiddleChild );

        // old middle child has successor
        goTo( readCursor, middleChild );
        assertEquals( newMiddleChild, successor( readCursor, stableGeneration, unstableGeneration ) );

        // old middle child has seen no change
        assertKeyAssociatedWithValue( middleKey, middleKey );

        // new middle child has seen change
        goTo( readCursor, newMiddleChild );
        assertKeyNotFound( middleKey );

        // sibling pointers updated
        assertSiblings( leftChild, newMiddleChild, rightChild );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableRootAsInternal() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

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
        goTo( readCursor, rootId );
        assertEquals( 1, keyCount() );
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertSiblings( leftChild, rightChild, TreeNode.NO_NODE_FLAG );

        // WHEN
        //                       root(successor)
        //                   ----  | ---------------
        //                  /      |                \
        //                 v       v                 v
        //               left <-> right(successor) <--> farRight
        generationManager.checkpoint();
        insert( i, i );
        assertEquals( 1, numberOfRootSuccessors );
        goTo( readCursor, rootId );
        leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // THEN
        // siblings are correct
        long farRightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( leftChild, rightChild, farRightChild );

        // old root points to successor of root
        goTo( readCursor, oldRootId );
        assertEquals( rootId, successor( readCursor, stableGeneration, unstableGeneration ) );
    }

    @Test
    public void shouldCreateNewVersionWhenInsertInStableInternal() throws Exception
    {
        assumeTrue( "No checkpointing, no successor", isCheckpointing );

        // GIVEN
        initialize();
        for ( int i = 0; numberOfRootSplits < 2; i++ )
        {
            long keyAndValue = i * maxKeyCount;
            insert( keyAndValue, keyAndValue );
        }
        long rootAfterInitialData = rootId;
        goTo( readCursor, rootId );
        assertEquals( 1, keyCount() );
        long leftInternal = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long rightInternal = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertSiblings( leftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
        goTo( readCursor, leftInternal );
        int leftInternalKeyCount = keyCount();
        assertTrue( TreeNode.isInternal( readCursor ) );
        long leftLeaf = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        goTo( readCursor, leftLeaf );
        long firstKeyInLeaf = keyAt( 0 );

        // WHEN
        generationManager.checkpoint();
        long targetLastId = id.lastId() + 3; /*one for successor in leaf, one for split leaf, one for successor in internal*/
        for ( int i = 0; id.lastId() < targetLastId; i++ )
        {
            insert( firstKeyInLeaf + i, firstKeyInLeaf + i );
            assertFalse( structurePropagation.hasRightKeyInsert ); // there should be no root split
        }

        // THEN
        // root hasn't been split further
        assertEquals( rootAfterInitialData, rootId );

        // there's an successor to left internal w/ one more key in
        goTo( readCursor, rootId );
        long successorLeftInternal = id.lastId();
        assertEquals( successorLeftInternal, childAt( readCursor, 0, stableGeneration, unstableGeneration ) );
        goTo( readCursor, successorLeftInternal );
        int successorLeftInternalKeyCount = keyCount();
        assertEquals( leftInternalKeyCount + 1, successorLeftInternalKeyCount );

        // and left internal points to the successor
        goTo( readCursor, leftInternal );
        assertEquals( successorLeftInternal, successor( readCursor, stableGeneration, unstableGeneration ) );
        assertSiblings( successorLeftInternal, rightInternal, TreeNode.NO_NODE_FLAG );
    }

    @Test
    public void shouldOverwriteInheritedSuccessorOnSuccessor() throws Exception
    {
        // GIVEN
        assumeTrue( isCheckpointing );
        initialize();
        long originalNodeId = rootId;
        generationManager.checkpoint();
        insert( 1L, 10L ); // TX1 will create successor
        assertEquals( 1, numberOfRootSuccessors );

        // WHEN
        // recovery happens
        generationManager.recovery();
        // start up on stable root
        goTo( cursor, originalNodeId );
        treeLogic.initialize( cursor );
        // replay transaction TX1 will create a new successor
        insert( 1L, 10L );
        assertEquals( 2, numberOfRootSuccessors );

        // THEN
        goTo( readCursor, rootId );
        // successor pointer for successor should not have broken or crashed GSPP slot
        assertSuccessorPointerNotCrashOrBroken();
        // and previously crashed successor GSPP slot should have been overwritten
        goTo( readCursor, originalNodeId );
        assertSuccessorPointerNotCrashOrBroken();
    }

    @Test
    public void mustThrowIfReachingNodeWithValidSuccessor() throws Exception
    {
        // GIVEN
        // root with two children
        assumeTrue( isCheckpointing );
        initialize();
        for ( int i = 1; numberOfRootSplits < 1; i++ )
        {
            long keyAndValue = i * maxKeyCount;
            insert( keyAndValue, keyAndValue );
        }
        generationManager.checkpoint();

        // and leftmost child has successor that is not pointed to by parent (root)
        goTo( readCursor, rootId );
        long leftmostChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        giveSuccessor( readCursor, leftmostChild );

        // WHEN
        // insert in leftmostChild
        try
        {
            insert( 0, 0 );
            fail( "Expected insert to throw because child targeted for insertion has a valid new successor." );
        }
        catch ( TreeInconsistencyException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE ) );
        }
    }

    private void giveSuccessor( PageCursor cursor, long nodeId ) throws IOException
    {
        goTo( cursor, nodeId );
        TreeNode.setSuccessor( cursor, 42, stableGeneration, unstableGeneration );
    }

    private long rightmostInternalKeyInSubtree( long parentNodeId, int subtreePosition ) throws IOException
    {
        long current = readCursor.getCurrentPageId();
        goToSubtree( parentNodeId, subtreePosition );
        boolean found = false;
        long rightmostKeyInSubtree = -1;
        while ( TreeNode.isInternal( readCursor ) )
        {
            int keyCount = TreeNode.keyCount( readCursor );
            if ( keyCount <= 0 )
            {
                break;
            }
            rightmostKeyInSubtree = keyAt( keyCount - 1 );
            found = true;
            long rightmostChild = childAt( readCursor, keyCount, stableGeneration, unstableGeneration );
            goTo( readCursor, rightmostChild );
        }
        if ( !found )
        {
            throw new IllegalArgumentException( "Subtree on position " + subtreePosition + " in node " + parentNodeId +
                    " did not contain a rightmost internal key." );
        }

        goTo( readCursor, current );
        return rightmostKeyInSubtree;
    }

    private void goToSubtree( long parentNodeId, int subtreePosition ) throws IOException
    {
        goTo( readCursor, parentNodeId );
        long subtree = childAt( readCursor, subtreePosition, stableGeneration, unstableGeneration );
        goTo( readCursor, subtree );
    }

    private long leftmostLeafInSubtree( long parentNodeId, int subtreePosition ) throws IOException
    {
        long current = readCursor.getCurrentPageId();
        goToSubtree( parentNodeId, subtreePosition );
        long leftmostChild = current;
        while ( TreeNode.isInternal( readCursor ) )
        {
            leftmostChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
            goTo( readCursor, leftmostChild );
        }

        goTo( readCursor, current );
        return leftmostChild;
    }

    private long rightmostLeafInSubtree( long parentNodeId, int subtreePosition ) throws IOException
    {
        long current = readCursor.getCurrentPageId();
        goToSubtree( parentNodeId, subtreePosition );
        long rightmostChild = current;
        while ( TreeNode.isInternal( readCursor ) )
        {
            int keyCount = TreeNode.keyCount( readCursor );
            rightmostChild = childAt( readCursor, keyCount, stableGeneration, unstableGeneration );
            goTo( readCursor, rightmostChild );
        }

        goTo( readCursor, current );
        return rightmostChild;
    }

    private void assertNodeContainsExpectedKeys( List<Long> expectedKeys )
    {
        List<Long> actualKeys = allKeys( readCursor );
        assertThat( actualKeys, is( expectedKeys ) );
    }

    private List<Long> allKeys( PageCursor cursor )
    {
        List<Long> keys = new ArrayList<>();
        return allKeys( cursor, keys );
    }

    private List<Long> allKeys( PageCursor cursor, List<Long> keys )
    {
        int keyCount = TreeNode.keyCount( cursor );
        for ( int i = 0; i < keyCount; i++ )
        {
            node.keyAt( cursor, readKey, i );
            keys.add( readKey.longValue() );
        }
        return keys;
    }

    private int keyCount( long nodeId ) throws IOException
    {
        long prevId = readCursor.getCurrentPageId();
        try
        {
            goTo( readCursor, nodeId );
            return TreeNode.keyCount( readCursor );
        }
        finally
        {
            goTo( readCursor, prevId );
        }
    }

    private int keyCount()
    {
        return TreeNode.keyCount( readCursor );
    }

    private void initialize()
    {
        TreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        updateRoot();
    }

    private void updateRoot()
    {
        rootId = cursor.getCurrentPageId();
        rootGeneration = unstableGeneration;
        treeLogic.initialize( cursor );
    }

    private void assertSuccessorPointerNotCrashOrBroken()
    {
        assertNoCrashOrBrokenPointerInGSPP( readCursor, stableGeneration, unstableGeneration, "Successor",
                TreeNode.BYTE_POS_SUCCESSOR );
    }

    private void assertKeyAssociatedWithValue( long key, long expectedValue )
    {
        insertKey.setValue( key );
        int search = KeySearch.search( readCursor, node, insertKey, readKey, TreeNode.keyCount( readCursor ) );
        assertTrue( KeySearch.isHit( search ) );
        int keyPos = KeySearch.positionOf( search );
        node.valueAt( readCursor, readValue, keyPos );
        assertEquals( expectedValue, readValue.longValue() );
    }

    private void assertKeyNotFound( long key )
    {
        insertKey.setValue( key );
        int search = KeySearch.search( readCursor, node, insertKey, readKey, TreeNode.keyCount( readCursor ) );
        assertFalse( KeySearch.isHit( search ) );
    }

    private void assertSiblings( long left, long middle, long right ) throws IOException
    {
        long origin = readCursor.getCurrentPageId();
        goTo( readCursor, middle );
        assertEquals( right, rightSibling( readCursor, stableGeneration, unstableGeneration ) );
        assertEquals( left, leftSibling( readCursor, stableGeneration, unstableGeneration ) );
        if ( left != TreeNode.NO_NODE_FLAG )
        {
            goTo( readCursor, left );
            assertEquals( middle, rightSibling( readCursor, stableGeneration, unstableGeneration ) );
        }
        if ( right != TreeNode.NO_NODE_FLAG )
        {
            goTo( readCursor, right );
            assertEquals( middle, leftSibling( readCursor, stableGeneration, unstableGeneration ) );
        }
        goTo( readCursor, origin );
    }

    // KEEP even if unused
    private void printTree() throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        cursor.next( rootId );
        new TreePrinter<>( node, layout, stableGeneration, unstableGeneration ).printTree( cursor, System.out, true, true, true );
        cursor.next( currentPageId );
    }

    private static MutableLong key( long key )
    {
        return new MutableLong( key );
    }

    private void newRootFromSplit( StructurePropagation<MutableLong> split ) throws IOException
    {
        assertTrue( split.hasRightKeyInsert );
        long rootId = id.acquireNewId( stableGeneration, unstableGeneration );
        goTo( cursor, rootId );
        TreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
        node.insertKeyAt( cursor, split.rightKey, 0, 0 );
        TreeNode.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.midChild, 0, stableGeneration, unstableGeneration );
        node.setChildAt( cursor, split.rightChild, 1, stableGeneration, unstableGeneration );
        split.hasRightKeyInsert = false;
        updateRoot();
    }

    private void assertSiblingOrderAndPointers( long... children ) throws IOException
    {
        long currentPageId = readCursor.getCurrentPageId();
        RightmostInChain rightmost = new RightmostInChain();
        for ( long child : children )
        {
            goTo( readCursor, child );
            long leftSibling = TreeNode.leftSibling( readCursor, stableGeneration, unstableGeneration );
            long rightSibling = TreeNode.rightSibling( readCursor, stableGeneration, unstableGeneration );
            rightmost.assertNext( readCursor,
                    TreeNode.generation( readCursor ),
                    pointer( leftSibling ),
                    node.pointerGeneration( readCursor, leftSibling ),
                    pointer( rightSibling ),
                    node.pointerGeneration( readCursor, rightSibling ) );
        }
        rightmost.assertLast();
        goTo( readCursor, currentPageId );
    }

    private Long keyAt( long nodeId, int pos ) throws IOException
    {
        long prevId = readCursor.getCurrentPageId();
        try
        {
            readCursor.next( nodeId );
            return node.keyAt( readCursor, readKey, pos ).getValue();
        }
        finally
        {
            readCursor.next( prevId );
        }
    }

    private Long keyAt( int pos )
    {
        return node.keyAt( readCursor, readKey, pos ).getValue();
    }

    private Long valueAt( int pos )
    {
        return node.valueAt( readCursor, readValue, pos ).getValue();
    }

    private void insert( long key, long value ) throws IOException
    {
        insert( key, value, overwrite() );
    }

    private void insert( long key, long value, ValueMerger<MutableLong,MutableLong> valueMerger ) throws IOException
    {
        structurePropagation.hasRightKeyInsert = false;
        structurePropagation.hasMidChildUpdate = false;
        insertKey.setValue( key );
        insertValue.setValue( value );
        treeLogic.insert( cursor, structurePropagation, insertKey, insertValue, valueMerger, stableGeneration,
                unstableGeneration );
        handleAfterChange();
    }

    private void handleAfterChange() throws IOException
    {
        if ( structurePropagation.hasRightKeyInsert )
        {
            newRootFromSplit( structurePropagation );
            numberOfRootSplits++;
        }
        if ( structurePropagation.hasMidChildUpdate )
        {
            structurePropagation.hasMidChildUpdate = false;
            updateRoot();
            numberOfRootSuccessors++;
        }
    }

    private MutableLong remove( long key, MutableLong into ) throws IOException
    {
        insertKey.setValue( key );
        MutableLong result = treeLogic.remove( cursor, structurePropagation, insertKey, into, stableGeneration,
                unstableGeneration );
        handleAfterChange();
        return result;
    }

    private interface GenerationManager
    {
        void checkpoint();

        void recovery();

        GenerationManager NO_OP_GENERATION = new GenerationManager()
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

        GenerationManager DEFAULT = new GenerationManager()
        {
            @Override
            public void checkpoint()
            {
                stableGeneration = unstableGeneration;
                unstableGeneration++;
            }

            @Override
            public void recovery()
            {
                unstableGeneration++;
            }
        };
    }

    private static void goTo( PageCursor cursor, long pageId ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "test", pointer( pageId ) );
    }

    private void goToSuccessor( PageCursor cursor ) throws IOException
    {
        long newestGeneration = newestGeneration( cursor, stableGeneration, unstableGeneration );
        goTo( cursor, newestGeneration );
    }

    private void goToSuccessor( PageCursor cursor, long targetNode ) throws IOException
    {
        goTo( cursor, targetNode );
        goToSuccessor( cursor );
    }

    private long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        return pointer( node.childAt( cursor, pos, stableGeneration, unstableGeneration ) );
    }

    private long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        return pointer( TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration ) );
    }

    private long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        return pointer( TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration ) );
    }

    private long successor( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        return pointer( TreeNode.successor( cursor, stableGeneration, unstableGeneration ) );
    }

    private long newestGeneration( PageCursor cursor, long stableGeneration, long unstableGeneration ) throws IOException
    {
        long current = cursor.getCurrentPageId();
        long successor = current;
        do
        {
            goTo( cursor, successor );
            successor = pointer( TreeNode.successor( cursor, stableGeneration, unstableGeneration ) );
        }
        while ( successor != TreeNode.NO_NODE_FLAG );
        successor = cursor.getCurrentPageId();
        goTo( cursor, current );
        return successor;
    }
}
