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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.index.internal.gbptree.ConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNode.Overflow.NO;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;

@RunWith( Parameterized.class )
public abstract class InternalTreeLogicTestBase<KEY,VALUE>
{
    protected TestLayout<KEY,VALUE> layout;
    protected TreeNode<KEY,VALUE> node;

    private final int pageSize = 256;
    private PageAwareByteArrayCursor cursor;
    private PageAwareByteArrayCursor readCursor;
    private SimpleIdProvider id;

    private ValueMerger<KEY,VALUE> adder;
    private InternalTreeLogic<KEY,VALUE> treeLogic;
    private VALUE dontCare;
    private StructurePropagation<KEY> structurePropagation;

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
        cursor = new PageAwareByteArrayCursor( pageSize );
        readCursor = cursor.duplicate();
        id = new SimpleIdProvider( cursor::duplicate );

        id.reset();
        long newId = id.acquireNewId( stableGeneration, unstableGeneration );
        goTo( cursor, newId );
        readCursor.next( newId );

        layout = getLayout();
        node = getTreeNode( pageSize, layout );
        adder = getAdder();
        treeLogic = new InternalTreeLogic<>( id, node, layout );
        dontCare = layout.newValue();
        structurePropagation = new StructurePropagation<>( layout.newKey(), layout.newKey(), layout.newKey() );
    }

    protected abstract ValueMerger<KEY,VALUE> getAdder();

    protected abstract TreeNode<KEY,VALUE> getTreeNode( int pageSize, Layout<KEY,VALUE> layout );

    protected abstract TestLayout<KEY,VALUE> getLayout();

    @Test
    public void modifierMustInsertAtFirstPositionInEmptyLeaf() throws Exception
    {
        // given
        initialize();
        KEY key = key( 1L );
        VALUE value = value( 1L );
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 0 ) );

        // when
        generationManager.checkpoint();
        insert( key, value );

        // then
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        assertEqualsKey( keyAt( 0, LEAF ), key );
        assertEqualsValue( valueAt( 0 ), value );
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertFirstInLeaf() throws Exception
    {
        // given
        initialize();
        generationManager.checkpoint();

        long someHighSeed = 1000L;
        int keyCount = 0;
        KEY newKey = key( someHighSeed );
        VALUE newValue = value( someHighSeed );
        while ( node.leafOverflow( cursor, keyCount, newKey, newValue ) == NO )
        {
            insert( newKey, newValue );

            // then
            readCursor.next( rootId );
            assertEqualsKey( keyAt( 0, LEAF ), newKey );
            assertEqualsValue( valueAt( 0 ), newValue );

            keyCount++;
            newKey = key( someHighSeed - keyCount );
            newValue = value( someHighSeed - keyCount );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertLastInLeaf() throws Exception
    {
        // given
        initialize();
        generationManager.checkpoint();
        int keyCount = 0;
        KEY key = key( keyCount );
        VALUE value = value( keyCount );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            // when
            insert( key, value );

            // then
            readCursor.next( rootId );
            assertEqualsKey( keyAt( keyCount, LEAF ), key );
            assertEqualsValue( valueAt( keyCount ), value );

            keyCount++;
            key = key( keyCount );
            value = value( keyCount );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertInMiddleOfLeaf() throws Exception
    {
        // given
        initialize();
        generationManager.checkpoint();
        int keyCount = 0;
        int someHighSeed = 1000;
        long middleValue = keyCount % 2 == 0 ? keyCount / 2 : someHighSeed - keyCount / 2;
        KEY key = key( middleValue );
        VALUE value = value( middleValue );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            insert( key, value );

            // then
            readCursor.next( rootId );
            assertEqualsKey( keyAt( (keyCount + 1) / 2, LEAF ), key );

            keyCount++;
            middleValue = keyCount % 2 == 0 ? keyCount / 2 : someHighSeed - keyCount / 2;
            key = key( middleValue );
            value = value( middleValue );
        }
    }

    @Test
    public void modifierMustSplitWhenInsertingMiddleOfFullLeaf() throws Exception
    {
        // given
        initialize();
        int someMiddleSeed = 1000;
        int keyCount = 0;
        int middle = keyCount % 2 == 0 ? keyCount : someMiddleSeed - keyCount;
        KEY key = key( middle );
        VALUE value = value( middle );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            insert( key, value );

            keyCount++;
            middle = keyCount % 2 == 0 ? keyCount : someMiddleSeed - keyCount;
            key = key( middle );
            value = value( middle );
        }

        // when
        generationManager.checkpoint();
        insert( key, value );

        // then
        assertEquals( 1, numberOfRootSplits );
    }

    @Test
    public void modifierMustSplitWhenInsertingLastInFullLeaf() throws Exception
    {
        // given
        initialize();
        int keyCount = 0;
        KEY key = key( keyCount );
        VALUE value = value( keyCount );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            insert( key, value );
            assertFalse( structurePropagation.hasRightKeyInsert );

            keyCount++;
            key = key( keyCount );
            value = value( keyCount );
        }

        // when
        generationManager.checkpoint();
        insert( key, value );

        // then
        assertEquals( 1, numberOfRootSplits ); // Should cause a split
    }

    @Test
    public void modifierMustSplitWhenInsertingFirstInFullLeaf() throws Exception
    {
        // given
        initialize();
        int keyCount = 0;
        int someHighSeed = 1000;
        KEY key = key( someHighSeed - keyCount );
        VALUE value = value( someHighSeed - keyCount );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            insert( key, value );
            assertFalse( structurePropagation.hasRightKeyInsert );

            keyCount++;
            key = key( someHighSeed - keyCount );
            value = value( someHighSeed - keyCount );
        }

        // when
        generationManager.checkpoint();
        insert( key, value );

        // then
        assertEquals( 1, numberOfRootSplits );
    }

    @Test
    public void modifierMustUpdatePointersInSiblingsToSplit() throws Exception
    {
        // given
        initialize();
        long someLargeSeed = 10000;
        int keyCount = 0;
        KEY key = key( someLargeSeed - keyCount );
        VALUE value = value( someLargeSeed - keyCount );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            insert( key, value );

            keyCount++;
            key = key( someLargeSeed - keyCount );
            value = value( someLargeSeed - keyCount );
        }

        // First split
        generationManager.checkpoint();
        insert( key, value );
        keyCount++;
        key = key( someLargeSeed - keyCount );
        value = value( keyCount );

        // Assert child pointers and sibling pointers are intact after split in root
        goTo( readCursor, rootId );
        long child0 = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long child1 = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        assertSiblingOrderAndPointers( child0, child1 );

        // Insert until we have another split in leftmost leaf
        while ( keyCount( rootId ) == 1 )
        {
            insert( key, value );
            keyCount++;
            key = key( someLargeSeed - keyCount );
            value = value( keyCount );
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
        long keyValue = 1L;
        long valueValue = 1L;
        KEY key = key( keyValue );
        VALUE value = value( valueValue );
        insert( key, value );

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove( key, readValue );

        // then
        goTo( readCursor, rootId );
        assertThat( TreeNode.keyCount( cursor ), is( 0 ) );
        assertEqualsValue( value, readValue );
    }

    @Test
    public void modifierMustRemoveFirstInFullLeaf() throws Exception
    {
        // given
        initialize();
        int maxKeyCount = 0;
        KEY key = key( maxKeyCount );
        VALUE value = value( maxKeyCount );
        while ( node.leafOverflow( cursor, maxKeyCount, key, value ) == NO )
        {
            insert( key, value );

            maxKeyCount++;
            key = key( maxKeyCount );
            value = value( maxKeyCount );
        }

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove( key( 0 ), readValue );

        // then
        assertEqualsValue( value( 0 ), readValue );
        goTo( readCursor, rootId );
        assertThat( TreeNode.keyCount( readCursor ), is( maxKeyCount - 1 ) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            assertEqualsKey( keyAt( i, LEAF ), key( i + 1L ) );
        }
    }

    @Test
    public void modifierMustRemoveInMiddleInFullLeaf() throws Exception
    {
        // given
        initialize();
        int maxKeyCount = 0;
        KEY key = key( maxKeyCount );
        VALUE value = value( maxKeyCount );
        while ( node.leafOverflow( cursor, maxKeyCount, key, value ) == NO )
        {
            insert( key, value );

            maxKeyCount++;
            key = key( maxKeyCount );
            value = value( maxKeyCount );
        }
        int middle = maxKeyCount / 2;

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove( key( middle ), readValue );

        // then
        assertEqualsValue( value( middle ), readValue );
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( maxKeyCount - 1 ) );
        assertEqualsKey( keyAt( middle, LEAF ), key( middle + 1L ) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            long expected = i < middle ? i : i + 1L;
            assertEqualsKey( keyAt( i, LEAF ), key( expected ) );
        }
    }

    @Test
    public void modifierMustRemoveLastInFullLeaf() throws Exception
    {
        initialize();
        int maxKeyCount = 0;
        KEY key = key( maxKeyCount );
        VALUE value = value( maxKeyCount );
        while ( node.leafOverflow( cursor, maxKeyCount, key, value ) == NO )
        {
            insert( key, value );

            maxKeyCount++;
            key = key( maxKeyCount );
            value = value( maxKeyCount );
        }

        // when
        generationManager.checkpoint();
        VALUE readValue = layout.newValue();
        remove( key( maxKeyCount - 1 ), readValue );

        // then
        assertEqualsValue( value( maxKeyCount - 1 ), readValue );
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( maxKeyCount - 1 ) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            assertEqualsKey( keyAt( i, LEAF ), key( i ) );
        }
    }

    @Test
    public void modifierMustRemoveFromLeftChild() throws Exception
    {
        initialize();
        for ( int i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( key( i ), value( i ) );
        }

        // when
        generationManager.checkpoint();
        goTo( readCursor, structurePropagation.midChild );
        assertEqualsKey( keyAt( 0, LEAF ), key( 0L ) );
        VALUE readValue = layout.newValue();
        remove( key( 0 ), readValue );

        // then
        assertEqualsValue( value( 0 ), readValue );
        goTo( readCursor, structurePropagation.midChild );
        assertEqualsKey( keyAt( 0, LEAF ), key( 1L ) );
    }

    @Test
    public void modifierMustRemoveFromRightChildButNotFromInternalWithHitOnInternalSearch() throws Exception
    {
        initialize();
        int i;
        for ( i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( key( i ), value( i ) );
        }
        insert( key( i ), value( i ) ); // And one more to avoid rebalance

        // when key to remove exists in internal
        KEY keyToRemove = structurePropagation.rightKey;
        goTo( readCursor, rootId );
        assertEqualsKey( keyAt( 0, INTERNAL ), keyToRemove );

        // and as first key in right child
        long rightChild = structurePropagation.rightChild;
        goTo( readCursor, rightChild );
        int keyCountInRightChild = keyCount();
        assertEqualsKey( keyAt( 0, LEAF ), keyToRemove );

        // and we remove it
        generationManager.checkpoint();
        remove( keyToRemove, dontCare );

        // then we should still find it in internal
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        assertEqualsKey( keyAt( 0, INTERNAL ), keyToRemove );

        // but not in right leaf
        rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, rightChild );
        assertThat( keyCount(), is( keyCountInRightChild - 1 ) );
        assertEqualsKey( keyAt( 0, LEAF ), key( getSeed( keyToRemove ) + 1 ) );
    }

    @Test
    public void modifierMustNotRemoveWhenKeyDoesNotExist() throws Exception
    {
        // given
        initialize();
        int maxKeyCount = 0;
        KEY key = key( maxKeyCount );
        VALUE value = value( maxKeyCount );
        while ( node.leafOverflow( cursor, maxKeyCount, key, value ) == NO )
        {
            insert( key, value );

            maxKeyCount++;
            key = key( maxKeyCount );
            value = value( maxKeyCount );
        }

        // when
        generationManager.checkpoint();
        assertNull( remove( key( maxKeyCount ), dontCare ) );

        // then
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( maxKeyCount ) );
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            assertEqualsKey( keyAt( i, LEAF ), key( i ) );
        }
    }

    @Test
    public void modifierMustNotRemoveWhenKeyOnlyExistInInternal() throws Exception
    {
        // given
        initialize();
        int i;
        for ( i = 0; numberOfRootSplits == 0; i++ )
        {
            insert( key( i ), value( i ) );
        }
        insert( key( i ), value( i ) ); // And an extra to not cause rebalance

        // when key to remove exists in internal
        KEY keyToRemove = structurePropagation.rightKey;
        assertEqualsKey( keyAt( rootId, 0, INTERNAL ), keyToRemove );

        // and as first key in right child
        long currentRightChild = structurePropagation.rightChild;
        goTo( readCursor, currentRightChild );
        int keyCountInRightChild = keyCount();
        assertEqualsKey( keyAt( 0, LEAF ), keyToRemove );

        // and we remove it
        generationManager.checkpoint();
        remove( keyToRemove, dontCare ); // Possibly create successor of right child
        goTo( readCursor, rootId );
        currentRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );

        // then we should still find it in internal
        assertThat( keyCount(), is( 1 ) );
        assertEqualsKey( keyAt( 0, INTERNAL ), keyToRemove );

        // but not in right leaf
        goTo( readCursor, currentRightChild );
        assertThat( keyCount(), is( keyCountInRightChild - 1 ) );
        assertEqualsKey( keyAt( 0, LEAF ), key( getSeed( keyToRemove ) + 1 ) );

        // and when we remove same key again, nothing should change
        assertNull( remove( keyToRemove, dontCare ) );
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
            insert( key( key ), value( key ) );
            key++;
        }

        // ... enough keys in right child to share with left child if rebalance is needed
        insert( key( key ), value( key ) );
        key++;

        // ... and the prim key diving key range for left child and right child
        goTo( readCursor, rootId );
        KEY primKey = keyAt( 0, INTERNAL );

        // ... and knowing key count of right child
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, rightChild );
        int expectedKeyCount = TreeNode.keyCount( readCursor );

        // when
        // ... removing all keys from left child
        for ( long i = 0; ; i++ )
        {
            KEY removeKey = key( i );
            if ( layout.compare( removeKey, primKey ) >= 0 )
            {
                break;
            }
            remove( removeKey, dontCare );
        }

        // then
        // ... looking a right child
        goTo( readCursor, rightChild );

        // ... no keys should have moved from right sibling
        int actualKeyCount = TreeNode.keyCount( readCursor );
        assertEquals( "actualKeyCount=" + actualKeyCount + ", expectedKeyCount=" + expectedKeyCount, expectedKeyCount, actualKeyCount );
        assertEqualsKey( keyAt( 0, LEAF ), primKey );
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
            insert( key( key ), value( key ) );
            key++;
        }
        // ... enough keys in left child to share with right child if rebalance is needed
        for ( long smallKey = 0; smallKey < 2; smallKey++ )
        {
            insert( key( smallKey ), value( smallKey ) );
        }

        // ... and the prim key dividing key range for left and right child
        goTo( readCursor, rootId );
        KEY oldPrimKey = keyAt( 0, INTERNAL );

        // ... and left and right child
        long originalLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long originalRightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, originalRightChild );
        List<KEY> keysInRightChild = allKeys( readCursor, LEAF );

        // when
        // ... after checkpoint
        generationManager.checkpoint();

        // ... removing keys from right child until rebalance is triggered
        int index = 0;
        long rightChild;
        KEY originalLeftmost = keysInRightChild.get( 0 );
        KEY leftmostInRightChild;
        do
        {
            remove( keysInRightChild.get( index ), dontCare );
            index++;
            goTo( readCursor, rootId );
            rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
            goTo( readCursor, rightChild );
            leftmostInRightChild = keyAt( 0, LEAF );
        }
        while ( layout.compare( leftmostInRightChild, originalLeftmost ) >= 0 );

        // then
        // ... primKey in root is updated
        goTo( readCursor, rootId );
        KEY primKey = keyAt( 0, INTERNAL );
        assertEqualsKey( primKey, leftmostInRightChild );
        assertNotEqualsKey( primKey, oldPrimKey );

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
        List<KEY> allKeys = new ArrayList<>();
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            KEY key = key( i );
            insert( key, value( i ) );
            allKeys.add( key );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );
        long oldLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long oldMiddleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long oldRightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( oldLeftChild, oldMiddleChild, oldRightChild );

        // WHEN
        generationManager.checkpoint();
        KEY middleKey = keyAt( 0, INTERNAL ); // Should be located in middle leaf
        remove( middleKey, dontCare );
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
        KEY firstKeyOfOldRightChild = keyAt( 0, LEAF );
        int index = indexOf( firstKeyOfOldRightChild, allKeys, layout );
        List<KEY> expectedKeysInNewLeftChild = allKeys.subList( 0, index );
        goTo( readCursor, newLeftChild );
        assertNodeContainsExpectedKeys( expectedKeysInNewLeftChild, LEAF );

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
        List<KEY> allKeys = new ArrayList<>();
        initialize();
        long targetLastId = id.lastId() + 3; // 2 splits and 1 new allocated root
        long i = 0;
        for ( ; id.lastId() < targetLastId; i++ )
        {
            KEY key = key( i );
            insert( key, value( i ) );
            allKeys.add( key );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );
        long oldLeftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long oldMiddleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long oldRightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( oldLeftChild, oldMiddleChild, oldRightChild );
        goTo( readCursor, oldLeftChild );
        KEY keyInLeftChild = keyAt( 0, LEAF );

        // WHEN
        generationManager.checkpoint();
        // removing key in left child
        goTo( readCursor, rootId );
        remove( keyInLeftChild, dontCare );
        allKeys.remove( keyInLeftChild );
        // New structure
        // NOTE: oldleft gets a successor (intermediate) before removing key and then another one once it is merged,
        //       effectively creating a chain of successor pointers to our newleft that in the end contain keys from
        //       oldleft and oldmiddle
        //                                                         ----root----
        //                                                        /            |
        //                                                       v             v
        // oldleft -[successor]-> intermediate -[successor]-> newleft <-> oldright
        //                                                      ^
        //                                                       \-[successor]- oldmiddle

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
        KEY firstKeyInOldRightChild = keyAt( 0, LEAF );
        int index = indexOf( firstKeyInOldRightChild, allKeys, layout );
        List<KEY> expectedKeysInNewLeftChild = allKeys.subList( 0, index );
        goTo( readCursor, newLeftChild );
        assertNodeContainsExpectedKeys( expectedKeysInNewLeftChild, LEAF );

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
            insert( key( i ), value( i ) );
            i++;
        }

        goTo( readCursor, rootId );
        long oldLeft = rightmostLeafInSubtree( rootId, 0 );
        long oldRight = leftmostLeafInSubtree( rootId, 1 );
        KEY oldSplitter = keyAt( 0, INTERNAL );
        KEY rightmostKeyInLeftSubtree = rightmostInternalKeyInSubtree( rootId, 0 );

        ArrayList<KEY> allKeysInOldLeftAndOldRight = new ArrayList<>();
        goTo( readCursor, oldLeft );
        allKeys( readCursor, allKeysInOldLeftAndOldRight, LEAF );
        goTo( readCursor, oldRight );
        allKeys( readCursor, allKeysInOldLeftAndOldRight, LEAF );

        KEY keyInOldRight = keyAt( 0, LEAF );

        // WHEN
        generationManager.checkpoint();
        remove( keyInOldRight, dontCare );
        remove( keyInOldRight, allKeysInOldLeftAndOldRight, layout );

        // THEN
        // oldSplitter in root should have been replaced by rightmostKeyInLeftSubtree
        goTo( readCursor, rootId );
        KEY newSplitter = keyAt( 0, INTERNAL );
        assertNotEqualsKey( newSplitter, oldSplitter );
        assertEqualsKey( newSplitter, rightmostKeyInLeftSubtree );

        // rightmostKeyInLeftSubtree should have been removed from successor version of leftParent
        KEY newRightmostInternalKeyInLeftSubtree = rightmostInternalKeyInSubtree( rootId, 0 );
        assertNotEqualsKey( newRightmostInternalKeyInLeftSubtree, rightmostKeyInLeftSubtree );

        // newRight contain all
        goToSuccessor( readCursor, oldRight );
        List<KEY> allKeysInNewRight = allKeys( readCursor, LEAF );
        assertThat( allKeysInNewRight.size(), is( allKeysInOldLeftAndOldRight.size() ) );
        for ( int index = 0; index < allKeysInOldLeftAndOldRight.size(); index++ )
        {
            assertEqualsKey( allKeysInOldLeftAndOldRight.get( index ), allKeysInNewRight.get( index ) );
        }
    }

    @Test
    public void mustLeaveSingleLeafAsRootWhenEverythingIsRemoved() throws Exception
    {
        // GIVEN
        // a tree with some keys
        List<KEY> allKeys = new ArrayList<>();
        initialize();
        long i = 0;
        while ( numberOfRootSplits < 3 )
        {
            KEY key = key( i );
            insert( key, value( i ) );
            allKeys.add( key );
            i++;
        }

        // WHEN
        // removing all keys but one
        generationManager.checkpoint();
        for ( int j = 0; j < allKeys.size() - 1; j++ )
        {
            remove( allKeys.get( j ), dontCare );
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
            long keySeed = random.nextLong();
            insert( key( keySeed ), value( random.nextLong() ) );
            if ( i == numberOfEntries / 2 )
            {
                generationManager.checkpoint();
            }
        }

        // then
        goTo( readCursor, rootId );
        ConsistencyChecker<KEY> consistencyChecker =
                new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration );
        consistencyChecker.check( readCursor, rootGeneration );
    }

    @Test
    public void modifierMustProduceConsistentTreeWithRandomInsertsWithConflictingKeys() throws Exception
    {
        // given
        initialize();
        int numberOfEntries = 100_000;
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            // when
            long keySeed = random.nextLong( 1000 );
            insert( key( keySeed ), value( random.nextLong() ) );
            if ( i == numberOfEntries / 2 )
            {
                generationManager.checkpoint();
            }
        }

        // then
        goTo( readCursor, rootId );
        ConsistencyChecker<KEY> consistencyChecker =
                new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration );
        consistencyChecker.check( readCursor, rootGeneration );
    }

    /* TEST VALUE MERGER */

    @Test
    public void modifierMustOverwriteWithOverwriteMerger() throws Exception
    {
        // given
        initialize();
        KEY key = key( random.nextLong() );
        VALUE firstValue = value( random.nextLong() );
        insert( key, firstValue );

        // when
        generationManager.checkpoint();
        VALUE secondValue = value( random.nextLong() );
        insert( key, secondValue, ValueMergers.overwrite() );

        // then
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        assertEqualsValue( valueAt( 0 ), secondValue );
    }

    @Test
    public void modifierMustKeepExistingWithKeepExistingMerger() throws Exception
    {
        // given
        initialize();
        KEY key = key( random.nextLong() );
        VALUE firstValue = value( random.nextLong() );
        insert( key, firstValue, ValueMergers.keepExisting() );
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        VALUE actual = valueAt( 0 );
        assertEqualsValue( actual, firstValue );

        // when
        generationManager.checkpoint();
        VALUE secondValue = value( random.nextLong() );
        insert( key, secondValue, ValueMergers.keepExisting() );

        // then
        goTo( readCursor, rootId );
        assertThat( keyCount(), is( 1 ) );
        actual = valueAt( 0 );
        assertEqualsValue( actual, firstValue );
    }

    @Test
    public void shouldMergeValue() throws Exception
    {
        // GIVEN
        initialize();
        KEY key = key( 10 );
        long baseValue = 100;
        insert( key, value( baseValue ) );

        // WHEN
        generationManager.checkpoint();
        long toAdd = 5;
        insert( key, value( toAdd ), adder );

        // THEN
        goTo( readCursor, rootId );
        int searchResult = KeySearch.search( readCursor, node, LEAF, key, layout.newKey(), keyCount() );
        assertTrue( KeySearch.isHit( searchResult ) );
        int pos = KeySearch.positionOf( searchResult );
        assertEquals( 0, pos );
        assertEqualsKey( key, keyAt( pos, LEAF ) );
        assertEqualsValue( value( baseValue + toAdd ), valueAt( pos ) );
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
        insert( key( 1L ), value( 1L ) );
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
        KEY key = key( 1L );
        VALUE value = value( 10L );
        insert( key, value );
        long oldGenerationId = cursor.getCurrentPageId();

        // WHEN root -[successor]-> successor of root
        generationManager.checkpoint();
        remove( key, dontCare );
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
            insert( key( i ), value( i ) );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long middleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );
        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        generationManager.checkpoint();
        long middle = i / 2;
        KEY middleKey = key( middle ); // Should be located in middle leaf
        VALUE oldValue = value( middle );
        VALUE newValue = value( middle * 11 );
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
        assertKeyAssociatedWithValue( middleKey, oldValue );

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
            insert( key( i ), value( i ) );
        }
        goTo( readCursor, rootId );
        assertEquals( 2, keyCount() );

        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        long middleChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        long rightChild = childAt( readCursor, 2, stableGeneration, unstableGeneration );

        // add some more keys to middleChild to not have remove trigger a merge
        goTo( readCursor, middleChild );
        KEY firstKeyInMiddleChild = keyAt( 0, LEAF );
        VALUE firstValueInMiddleChild = valueAt( 0 );
        long seed = getSeed( firstKeyInMiddleChild );
        insert( key( seed + 1 ), value( seed + 1 ) );
        insert( key( seed + 3 ), value( seed + 3 ) );
        goTo( readCursor, rootId );

        assertSiblings( leftChild, middleChild, rightChild );

        // WHEN
        generationManager.checkpoint();
        assertNotNull( remove( firstKeyInMiddleChild, dontCare ) );

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
        assertKeyAssociatedWithValue( firstKeyInMiddleChild, firstValueInMiddleChild );

        // new middle child has seen change
        goTo( readCursor, newMiddleChild );
        assertKeyNotFound( firstKeyInMiddleChild, LEAF );

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

        // Fill root
        int keyCount = 0;
        KEY key = key( keyCount );
        VALUE value = value( keyCount );
        while ( node.leafOverflow( cursor, keyCount, key, value ) == NO )
        {
            insert( key, value );
            keyCount++;
            key = key( keyCount );
            value = value( keyCount );
        }

        // Split
        insert( key, value );
        keyCount++;
        key = key( keyCount );
        value = value( keyCount );

        // Fill right child
        goTo( readCursor, rootId );
        long rightChild = childAt( readCursor, 1, stableGeneration, unstableGeneration );
        goTo( readCursor, rightChild );
        int rightChildKeyCount = TreeNode.keyCount( readCursor );
        while ( node.leafOverflow( readCursor, rightChildKeyCount, key, value ) == NO )
        {
            insert( key, value );
            keyCount++;
            rightChildKeyCount++;
            key = key( keyCount );
            value = value( keyCount );
        }

        long oldRootId = rootId;
        goTo( readCursor, rootId );
        assertEquals( 1, keyCount() );
        long leftChild = childAt( readCursor, 0, stableGeneration, unstableGeneration );
        assertSiblings( leftChild, rightChild, TreeNode.NO_NODE_FLAG );

        // WHEN
        //                       root(successor)
        //                   ----  | ---------------
        //                  /      |                \
        //                 v       v                 v
        //               left <-> right(successor) <--> farRight
        generationManager.checkpoint();
        insert( key, value );
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
        long someHighMultiplier = 1000;
        for ( int i = 0; numberOfRootSplits < 2; i++ )
        {
            long seed = i * someHighMultiplier;
            insert( key( seed ), value( seed ) );
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
        KEY firstKeyInLeaf = keyAt( 0, LEAF );
        long seedOfFirstKeyInLeaf = getSeed( firstKeyInLeaf );

        // WHEN
        generationManager.checkpoint();
        long targetLastId = id.lastId() + 3; /*one for successor in leaf, one for split leaf, one for successor in internal*/
        for ( int i = 0; id.lastId() < targetLastId; i++ )
        {
            insert( key( seedOfFirstKeyInLeaf + i ), value( seedOfFirstKeyInLeaf + i ) );
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
        insert( key( 1L ), value( 10L ) ); // TX1 will create successor
        assertEquals( 1, numberOfRootSuccessors );

        // WHEN
        // recovery happens
        generationManager.recovery();
        // start up on stable root
        goTo( cursor, originalNodeId );
        treeLogic.initialize( cursor );
        // replay transaction TX1 will create a new successor
        insert( key( 1L ), value( 10L ) );
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
        long someHighMultiplier = 1000;
        for ( int i = 1; numberOfRootSplits < 1; i++ )
        {
            long seed = i * someHighMultiplier;
            insert( key( seed ), value( seed ) );
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
            insert( key( 0 ), value( 0 ) );
            fail( "Expected insert to throw because child targeted for insertion has a valid new successor." );
        }
        catch ( TreeInconsistencyException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( PointerChecking.WRITER_TRAVERSE_OLD_STATE_MESSAGE ) );
        }
    }

    private void remove( KEY toRemove, List<KEY> list, Comparator<KEY> comparator )
    {
        int i = indexOf( toRemove, list, comparator );
        list.remove( i );
    }

    private int indexOf( KEY theKey, List<KEY> keys, Comparator<KEY> comparator )
    {
        int i = 0;
        for ( KEY key : keys )
        {
            if ( comparator.compare( theKey, key ) == 0 )
            {
                return i;
            }
            i++;
        }
        return -1;
    }

    private void giveSuccessor( PageCursor cursor, long nodeId ) throws IOException
    {
        goTo( cursor, nodeId );
        TreeNode.setSuccessor( cursor, 42, stableGeneration, unstableGeneration );
    }

    private KEY rightmostInternalKeyInSubtree( long parentNodeId, int subtreePosition ) throws IOException
    {
        long current = readCursor.getCurrentPageId();
        goToSubtree( parentNodeId, subtreePosition );
        boolean found = false;
        KEY rightmostKeyInSubtree = layout.newKey();
        while ( TreeNode.isInternal( readCursor ) )
        {
            int keyCount = TreeNode.keyCount( readCursor );
            if ( keyCount <= 0 )
            {
                break;
            }
            rightmostKeyInSubtree = keyAt( keyCount - 1, INTERNAL );
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

    private void assertNodeContainsExpectedKeys( List<KEY> expectedKeys, TreeNode.Type type )
    {
        List<KEY> actualKeys = allKeys( readCursor, type );
        for ( KEY actualKey : actualKeys )
        {
            GBPTreeTestUtil.contains( expectedKeys, actualKey, layout );
        }
        for ( KEY expectedKey : expectedKeys )
        {
            GBPTreeTestUtil.contains( actualKeys, expectedKey, layout );
        }
    }

    private List<KEY> allKeys( PageCursor cursor, TreeNode.Type type )
    {
        List<KEY> keys = new ArrayList<>();
        return allKeys( cursor, keys, type );
    }

    private List<KEY> allKeys( PageCursor cursor, List<KEY> keys, TreeNode.Type type )
    {
        int keyCount = TreeNode.keyCount( cursor );
        for ( int i = 0; i < keyCount; i++ )
        {
            KEY into = layout.newKey();
            node.keyAt( cursor, into, i, type );
            keys.add( into );
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

    protected void initialize()
    {
        node.initializeLeaf( cursor, stableGeneration, unstableGeneration );
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

    private void assertKeyAssociatedWithValue( KEY key, VALUE expectedValue )
    {
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();
        int search = KeySearch.search( readCursor, node, LEAF, key, readKey, TreeNode.keyCount( readCursor ) );
        assertTrue( KeySearch.isHit( search ) );
        int keyPos = KeySearch.positionOf( search );
        node.valueAt( readCursor, readValue, keyPos );
        assertEqualsValue( expectedValue, readValue );
    }

    private void assertKeyNotFound( KEY key, TreeNode.Type type )
    {
        KEY readKey = layout.newKey();
        int search = KeySearch.search( readCursor, node, type, key, readKey, TreeNode.keyCount( readCursor ) );
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
        new TreePrinter<>( node, layout, stableGeneration, unstableGeneration )
                .printTree( cursor, cursor, System.out, false, false, false, false );
        cursor.next( currentPageId );
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private long getSeed( KEY key )
    {
        return layout.keySeed( key );
    }

    private void newRootFromSplit( StructurePropagation<KEY> split ) throws IOException
    {
        assertTrue( split.hasRightKeyInsert );
        long rootId = id.acquireNewId( stableGeneration, unstableGeneration );
        goTo( cursor, rootId );
        node.initializeInternal( cursor, stableGeneration, unstableGeneration );
        node.setChildAt( cursor, split.midChild, 0, stableGeneration, unstableGeneration );
        node.insertKeyAndRightChildAt( cursor, split.rightKey, split.rightChild, 0, 0, stableGeneration, unstableGeneration );
        TreeNode.setKeyCount( cursor, 1 );
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

    private KEY keyAt( long nodeId, int pos, TreeNode.Type type )
    {
        KEY readKey = layout.newKey();
        long prevId = readCursor.getCurrentPageId();
        try
        {
            readCursor.next( nodeId );
            return node.keyAt( readCursor, readKey, pos, type );
        }
        finally
        {
            readCursor.next( prevId );
        }
    }

    private KEY keyAt( int pos, TreeNode.Type type )
    {
        return node.keyAt( readCursor, layout.newKey(), pos, type );
    }

    private VALUE valueAt( int pos )
    {
        return node.valueAt( readCursor, layout.newValue(), pos );
    }

    protected void insert( KEY key, VALUE value ) throws IOException
    {
        insert( key, value, overwrite() );
    }

    private void insert( KEY key, VALUE value, ValueMerger<KEY,VALUE> valueMerger ) throws IOException
    {
        structurePropagation.hasRightKeyInsert = false;
        structurePropagation.hasMidChildUpdate = false;
        treeLogic.insert( cursor, structurePropagation, key, value, valueMerger, stableGeneration, unstableGeneration );
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

    private VALUE remove( KEY key, VALUE into ) throws IOException
    {
        VALUE result = treeLogic.remove( cursor, structurePropagation, key, into, stableGeneration, unstableGeneration );
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

    private void assertNotEqualsKey( KEY key1, KEY key2 )
    {
        assertFalse( String.format( "expected no not equal, key1=%s, key2=%s", key1.toString(), key2.toString() ),
                layout.compare( key1, key2 ) == 0 );
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
}
