/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.resultIsFromSlotA;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

public class TreeNodeTest
{
    private static final int STABLE_GENERATION = 1;
    private static final int CRASH_GENERATION = 2;
    private static final int UNSTABLE_GENERATION = 3;
    private static final int HIGH_GENERATION = 4;

    private static final int PAGE_SIZE = 512;
    private final PageCursor cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNodeFixedSize<>( PAGE_SIZE, layout );

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void prepareCursor() throws IOException
    {
        cursor.next();
    }

    @Test
    public void shouldInitializeLeaf() throws Exception
    {
        // WHEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( TreeNode.NODE_TYPE_TREE_NODE, TreeNode.nodeType( cursor ) );
        assertTrue( TreeNode.isLeaf( cursor ) );
        assertFalse( TreeNode.isInternal( cursor ) );
        assertEquals( UNSTABLE_GENERATION, TreeNode.generation( cursor ) );
        assertEquals( 0, TreeNode.keyCount( cursor ) );
        assertEquals( NO_NODE_FLAG, leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, successor( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldInitializeInternal() throws Exception
    {
        // WHEN
        TreeNode.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( TreeNode.NODE_TYPE_TREE_NODE, TreeNode.nodeType( cursor ) );
        assertFalse( TreeNode.isLeaf( cursor ) );
        assertTrue( TreeNode.isInternal( cursor ) );
        assertEquals( UNSTABLE_GENERATION, TreeNode.generation( cursor ) );
        assertEquals( 0, TreeNode.keyCount( cursor ) );
        assertEquals( NO_NODE_FLAG, leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, successor( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldWriteAndReadMaxGeneration() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        TreeNode.setGeneration( cursor, GenerationSafePointer.MAX_GENERATION );

        // THEN
        long generation = TreeNode.generation( cursor );
        assertEquals( GenerationSafePointer.MAX_GENERATION, generation );
    }

    @Test
    public void shouldThrowIfWriteTooLargeGeneration() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        try
        {
            TreeNode.setGeneration( cursor, GenerationSafePointer.MAX_GENERATION + 1 );
            fail( "Expected throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    @Test
    public void shouldThrowIfWriteTooSmallGeneration() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        try
        {
            TreeNode.setGeneration( cursor, GenerationSafePointer.MIN_GENERATION - 1 );
            fail( "Expected throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    @Test
    public void keyValueOperationsInLeaf() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong key = layout.newKey();
        MutableLong value = layout.newValue();

        // WHEN
        long firstKey = 10;
        long firstValue = 100;
        key.setValue( firstKey );
        value.setValue( firstValue );
        node.insertKeyValueAt( cursor, key, value, 0, 0 );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0, LEAF ).longValue() );
        assertEquals( firstValue, node.valueAt( cursor, key, 0 ).longValue() );

        // WHEN
        long secondKey = 19;
        long secondValue = 190;
        key.setValue( secondKey );
        value.setValue( secondValue );
        node.insertKeyValueAt( cursor, key, value, 1, 1 );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0, LEAF ).longValue() );
        assertEquals( firstValue, node.valueAt( cursor, key, 0 ).longValue() );
        assertEquals( secondKey, node.keyAt( cursor, key, 1, LEAF ).longValue() );
        assertEquals( secondValue, node.valueAt( cursor, key, 1 ).longValue() );

        // WHEN
        long removedKey = 15;
        long removedValue = 150;
        key.setValue( removedKey );
        value.setValue( removedValue );
        node.insertKeyValueAt( cursor, key, value, 1, 2 );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0, LEAF ).longValue() );
        assertEquals( firstValue, node.valueAt( cursor, key, 0 ).longValue() );
        assertEquals( removedKey, node.keyAt( cursor, key, 1, LEAF ).longValue() );
        assertEquals( removedValue, node.valueAt( cursor, key, 1 ).longValue() );
        assertEquals( secondKey, node.keyAt( cursor, key, 2, LEAF ).longValue() );
        assertEquals( secondValue, node.valueAt( cursor, key, 2 ).longValue() );

        // WHEN
        node.removeKeyValueAt( cursor, 1, 3 );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0, LEAF ).longValue() );
        assertEquals( firstValue, node.valueAt( cursor, key, 0 ).longValue() );
        assertEquals( secondKey, node.keyAt( cursor, key, 1, LEAF ).longValue() );
        assertEquals( secondValue, node.valueAt( cursor, key, 1 ).longValue() );

        // WHEN
        long overwriteValue = 666;
        value.setValue( overwriteValue );
        node.setValueAt( cursor, value, 0 );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0, LEAF ).longValue() );
        assertEquals( overwriteValue, node.valueAt( cursor, key, 0 ).longValue() );
        assertEquals( secondKey, node.keyAt( cursor, key, 1, LEAF ).longValue() );
        assertEquals( secondValue, node.valueAt( cursor, key, 1 ).longValue() );
    }

    private void assertKeysAndChildren( long stable, long unstable, long... keysAndChildren )
    {
        MutableLong key = new MutableLong();
        int pos;
        for ( int i = 0; i < keysAndChildren.length; i++ )
        {
            pos = i / 2;
            if ( i % 2 == 0 )
            {
                assertEquals( keysAndChildren[i], GenerationSafePointerPair.pointer( node.childAt( cursor, pos, stable, unstable ) ) );
            }
            else
            {
                assertEquals( keysAndChildren[i], node.keyAt( cursor, key, pos, INTERNAL ).longValue() );
            }
        }
    }

    @Test
    public void shouldInsertAndRemoveKeyAndChildInInternal() throws Exception
    {
        // GIVEN
        TreeNode.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong key = layout.newKey();
        long stable = 3;
        long unstable = 4;
        long zeroChild = 5;

        // WHEN
        node.setChildAt( cursor, zeroChild, 0, stable, unstable );

        // THEN
        assertKeysAndChildren( stable, unstable, zeroChild );

        // WHEN
        long firstKey = 10;
        long firstChild = 100;
        key.setValue( firstKey );
        node.insertKeyAndRightChildAt( cursor, key, firstChild, 0, 0, stable, unstable );

        // THEN
        assertKeysAndChildren( stable, unstable, zeroChild, firstKey, firstChild );

        // WHEN
        long secondKey = 19;
        long secondChild = 190;
        key.setValue( secondKey );
        node.insertKeyAndRightChildAt( cursor, key, secondChild, 1, 1, stable, unstable );

        // THEN
        assertKeysAndChildren( stable, unstable, zeroChild, firstKey, firstChild, secondKey, secondChild );

        // WHEN
        long removedKey = 20;
        long removedChild = 200;
        key.setValue( removedKey );
        node.insertKeyAndRightChildAt( cursor, key, removedChild, 1, 2, stable, unstable );

        // THEN
        assertKeysAndChildren( stable, unstable, zeroChild, firstKey, firstChild, removedKey, removedChild, secondKey, secondChild );

        // WHEN
        node.removeKeyAndRightChildAt( cursor, 1, 3 );

        // THEN
        assertKeysAndChildren( stable, unstable, zeroChild, firstKey, firstChild, secondKey, secondChild );

        // WHEN
        node.removeKeyAndLeftChildAt( cursor, 0, 2 );

        // THEN
        assertKeysAndChildren( stable, unstable, firstChild, secondKey, secondChild );

        // WHEN
        long overwriteChild = 666;
        node.setChildAt( cursor, overwriteChild, 0, stable, unstable );

        // THEN
        assertKeysAndChildren( stable, unstable, overwriteChild, secondKey, secondChild );
    }

    @Test
    public void shouldSetAndGetKeyCount() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( 0, TreeNode.keyCount( cursor ) );

        // WHEN
        int keyCount = 5;
        TreeNode.setKeyCount( cursor, keyCount );

        // THEN
        assertEquals( keyCount, TreeNode.keyCount( cursor ) );
    }

    @Test
    public void shouldSetAndGetSiblings() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        TreeNode.setLeftSibling( cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION );
        TreeNode.setRightSibling( cursor, 456, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( 123, leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 456, rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldSetAndGetSuccessor() throws Exception
    {
        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        TreeNode.setSuccessor( cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( 123, successor( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldInsertAndRemoveRandomKeysAndValues() throws Exception
    {
        // This test doesn't care about sorting, that's an aspect that lies outside of TreeNode, really

        // GIVEN
        TreeNode.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        int maxKeyCount = node.leafMaxKeyCount();
        // add +1 to these to simplify some array logic in the test itself
        long[] expectedKeys = new long[maxKeyCount + 1];
        long[] expectedValues = new long[maxKeyCount + 1];
        int expectedKeyCount = 0;
        MutableLong key = layout.newKey();
        MutableLong value = layout.newValue();

        // WHEN/THEN
        for ( int i = 0; i < 1000; i++ )
        {
            if ( random.nextFloat() < 0.7 )
            {   // 70% insert
                if ( expectedKeyCount < maxKeyCount )
                {   // there's room
                    int position = expectedKeyCount == 0 ? 0 : random.nextInt( expectedKeyCount );
                    // ensure unique
                    do
                    {
                        key.setValue( random.nextLong() );
                    }
                    while ( contains( expectedKeys, 0, expectedKeyCount, key.longValue() ) );

                    value.setValue( random.nextLong() );
                    node.insertKeyValueAt( cursor, key, value, position, expectedKeyCount );
                    insert( expectedKeys, expectedKeyCount, key.longValue(), position );
                    insert( expectedValues, expectedKeyCount, value.longValue(), position );

                    TreeNode.setKeyCount( cursor, ++expectedKeyCount );
                }
            }
            else
            {   // 30% remove
                if ( expectedKeyCount > 0 )
                {   // there are things to remove
                    int position = random.nextInt( expectedKeyCount );
                    node.keyAt( cursor, key, position, LEAF );
                    node.valueAt( cursor, value, position );
                    node.removeKeyValueAt( cursor, position, expectedKeyCount );
                    long expectedKey = remove( expectedKeys, expectedKeyCount, position );
                    long expectedValue = remove( expectedValues, expectedKeyCount, position );
                    assertEquals( expectedKey, key.longValue() );
                    assertEquals( expectedValue, value.longValue() );

                    TreeNode.setKeyCount( cursor, --expectedKeyCount );
                }
            }
        }

        // THEN
        assertEquals( expectedKeyCount, TreeNode.keyCount( cursor ) );
        for ( int i = 0; i < expectedKeyCount; i++ )
        {
            long expectedKey = expectedKeys[i];
            node.keyAt( cursor, key, i, LEAF );
            assertEquals( expectedKey, key.longValue() );

            long expectedValue = expectedValues[i];
            node.valueAt( cursor, value, i );
            assertEquals( "For key " + key.longValue(), expectedValue, value.longValue() );
        }
    }

    @Test
    public void shouldAssertPageSizeBigEnoughForAtLeastTwoKeys() throws Exception
    {
        // WHEN
        try
        {
            new TreeNodeFixedSize<>( TreeNode.BASE_HEADER_LENGTH + layout.keySize( null ) + layout.valueSize(), layout );
            fail( "Should have failed" );
        }
        catch ( MetadataMismatchException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldReadPointerGenerationFromAbsoluteOffsetSlotA() throws Exception
    {
        // GIVEN
        long generation = UNSTABLE_GENERATION;
        long pointer = 12;
        TreeNode.setRightSibling( cursor, pointer, STABLE_GENERATION, generation );

        // WHEN
        long readResult = TreeNode.rightSibling( cursor, STABLE_GENERATION, generation );
        long readGeneration = node.pointerGeneration( cursor, readResult );

        // THEN
        assertEquals( pointer, pointer( readResult ) );
        assertEquals( generation, readGeneration );
        assertTrue( resultIsFromSlotA( readResult ) );
    }

    @Test
    public void shouldReadPointerGenerationFromAbsoluteOffsetSlotB() throws Exception
    {
        // GIVEN
        long generation = HIGH_GENERATION;
        long oldPointer = 12;
        long pointer = 123;
        TreeNode.setRightSibling( cursor, oldPointer, STABLE_GENERATION, UNSTABLE_GENERATION );
        TreeNode.setRightSibling( cursor, pointer, UNSTABLE_GENERATION, generation );

        // WHEN
        long readResult = TreeNode.rightSibling( cursor, UNSTABLE_GENERATION, generation );
        long readGeneration = node.pointerGeneration( cursor, readResult );

        // THEN
        assertEquals( pointer, pointer( readResult ) );
        assertEquals( generation, readGeneration );
        assertFalse( resultIsFromSlotA( readResult ) );
    }

    @Test
    public void shouldReadPointerGenerationFromLogicalPosSlotA() throws Exception
    {
        // GIVEN
        long generation = UNSTABLE_GENERATION;
        long pointer = 12;
        int childPos = 2;
        node.setChildAt( cursor, pointer, childPos, STABLE_GENERATION, generation );

        // WHEN
        long readResult = node.childAt( cursor, childPos, STABLE_GENERATION, generation );
        long readGeneration = node.pointerGeneration( cursor, readResult );

        // THEN
        assertEquals( pointer, pointer( readResult ) );
        assertEquals( generation, readGeneration );
        assertTrue( resultIsFromSlotA( readResult ) );
    }

    @Test
    public void shouldReadPointerGenerationFromLogicalPosZeroSlotA() throws Exception
    {
        // GIVEN
        long generation = UNSTABLE_GENERATION;
        long pointer = 12;
        int childPos = 0;
        node.setChildAt( cursor, pointer, childPos, STABLE_GENERATION, generation );

        // WHEN
        long readResult = node.childAt( cursor, childPos, STABLE_GENERATION, generation );
        long readGeneration = node.pointerGeneration( cursor, readResult );

        // THEN
        assertEquals( pointer, pointer( readResult ) );
        assertEquals( generation, readGeneration );
        assertTrue( resultIsFromSlotA( readResult ) );
    }

    @Test
    public void shouldReadPointerGenerationFromLogicalPosZeroSlotB() throws Exception
    {
        // GIVEN
        long generation = HIGH_GENERATION;
        long oldPointer = 13;
        long pointer = 12;
        int childPos = 0;
        node.setChildAt( cursor, oldPointer, childPos, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.setChildAt( cursor, pointer, childPos, UNSTABLE_GENERATION, generation );

        // WHEN
        long readResult = node.childAt( cursor, childPos, UNSTABLE_GENERATION, generation );
        long readGeneration = node.pointerGeneration( cursor, readResult );

        // THEN
        assertEquals( pointer, pointer( readResult ) );
        assertEquals( generation, readGeneration );
        assertFalse( resultIsFromSlotA( readResult ) );
    }

    @Test
    public void shouldReadPointerGenerationFromLogicalPosSlotB() throws Exception
    {
        // GIVEN
        long generation = HIGH_GENERATION;
        long oldPointer = 12;
        long pointer = 123;
        int childPos = 2;
        node.setChildAt( cursor, oldPointer, childPos, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.setChildAt( cursor, pointer, childPos, UNSTABLE_GENERATION, generation );

        // WHEN
        long readResult = node.childAt( cursor, childPos, UNSTABLE_GENERATION, generation );
        long readGeneration = node.pointerGeneration( cursor, readResult );

        // THEN
        assertEquals( pointer, pointer( readResult ) );
        assertEquals( generation, readGeneration );
        assertFalse( resultIsFromSlotA( readResult ) );
    }

    @Test
    public void shouldThrowIfReadingPointerGenerationOnWriteResult() throws Exception
    {
        // GIVEN
        long writeResult = GenerationSafePointerPair.write( cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION );

        try
        {
            // WHEN
            node.pointerGeneration( cursor, writeResult );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldThrowIfReadingPointerGenerationOnZeroReadResultHeader() throws Exception
    {
        // GIVEN
        long pointer = 123;

        try
        {
            // WHEN
            node.pointerGeneration( cursor, pointer );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }

    @Test
    public void shouldUseLogicalGenerationPosWhenReadingChild() throws Exception
    {
        // GIVEN
        long child = 101;
        int pos = 3;
        node.setChildAt( cursor, child, pos, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        long result = node.childAt( cursor, pos, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertTrue( GenerationSafePointerPair.isLogicalPos( result ) );
    }

    private static long remove( long[] from, int length, int position )
    {
        long result = from[position];
        for ( int i = position; i < length; i++ )
        {
            from[i] = from[i + 1];
        }
        return result;
    }

    private static void insert( long[] into, int length, long value, int position )
    {
        for ( int i = length - 1; i >= position; i-- )
        {
            into[i + 1] = into[i];
        }
        into[position] = value;
    }

    private static boolean contains( long[] array, int offset, int length, long value )
    {
        for ( int i = 0; i < length; i++ )
        {
            if ( array[offset + i] == value )
            {
                return true;
            }
        }
        return false;
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
}
