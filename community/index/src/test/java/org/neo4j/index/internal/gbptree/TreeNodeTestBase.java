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

import java.io.IOException;

import org.neo4j.index.internal.gbptree.TreeNode.Section;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.resultIsFromSlotA;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

public abstract class TreeNodeTestBase
{
    private static final int STABLE_GENERATION = 1;
    private static final int CRASH_GENERATION = 2;
    private static final int UNSTABLE_GENERATION = 3;
    private static final int HIGH_GENERATION = 4;
    private static final int PAGE_SIZE = 1024;

    protected final PageCursor cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
    protected final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    protected final TreeNode<MutableLong,MutableLong> node = instantiateTreeNode( PAGE_SIZE, layout );
    protected final Section<MutableLong,MutableLong> mainSection = node.main();
    protected final Section<MutableLong,MutableLong> deltaSection = node.delta();

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void prepareCursor() throws IOException
    {
        cursor.next();
    }

    protected abstract TreeNode<MutableLong,MutableLong> instantiateTreeNode( int pageSize, Layout<MutableLong,MutableLong> layout );

    @Test
    public void shouldInitializeLeaf() throws Exception
    {
        // WHEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( node.NODE_TYPE_TREE_NODE, node.nodeType( cursor ) );
        assertTrue( node.isLeaf( cursor ) );
        assertFalse( node.isInternal( cursor ) );
        assertEquals( UNSTABLE_GENERATION, node.generation( cursor ) );
        assertEquals( 0, mainSection.keyCount( cursor ) );
        assertEquals( NO_NODE_FLAG, leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, successor( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldInitializeInternal() throws Exception
    {
        // WHEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( node.NODE_TYPE_TREE_NODE, node.nodeType( cursor ) );
        assertFalse( node.isLeaf( cursor ) );
        assertTrue( node.isInternal( cursor ) );
        assertEquals( UNSTABLE_GENERATION, node.generation( cursor ) );
        assertEquals( 0, mainSection.keyCount( cursor ) );
        assertEquals( NO_NODE_FLAG, leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, successor( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldWriteAndReadMaxGeneration() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        node.setGeneration( cursor, GenerationSafePointer.MAX_GENERATION );

        // THEN
        long generation = node.generation( cursor );
        assertEquals( GenerationSafePointer.MAX_GENERATION, generation );
    }

    @Test
    public void shouldThrowIfWriteTooLargeGeneration() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        try
        {
            node.setGeneration( cursor, GenerationSafePointer.MAX_GENERATION + 1 );
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
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        try
        {
            node.setGeneration( cursor, GenerationSafePointer.MIN_GENERATION - 1 );
            fail( "Expected throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    private void shouldSetAndGetKey() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();

        // WHEN
        long firstKey = 10;
        key.setValue( firstKey );
        mainSection.insertKeyAt( cursor, key, 0, 0 );

        long otherKey = 19;
        key.setValue( otherKey );
        mainSection.insertKeyAt( cursor, key, 1, 1 );

        // THEN
        assertEquals( firstKey, mainSection.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( otherKey, mainSection.keyAt( cursor, key, 1 ).longValue() );
    }

    @Test
    public void shouldSetAndGetKeyInLeaf() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        shouldSetAndGetKey();
    }

    @Test
    public void shouldSetAndGetKeyInInternal() throws Exception
    {
        // GIVEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        shouldSetAndGetKey();
    }

    private void shouldRemoveKey() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();
        long firstKey = 10;
        key.setValue( firstKey );
        mainSection.insertKeyAt( cursor, key, 0, 0 );
        long otherKey = 19;
        key.setValue( otherKey );
        mainSection.insertKeyAt( cursor, key, 1, 1 );
        long thirdKey = 123;
        key.setValue( thirdKey );
        mainSection.insertKeyAt( cursor, key, 2, 2 );

        // WHEN
        mainSection.removeKeyAt( cursor, 1, 3 );

        // THEN
        assertEquals( firstKey, mainSection.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( thirdKey, mainSection.keyAt( cursor, key, 1 ).longValue() );
    }

    @Test
    public void shouldRemoveKeyInLeaf() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        shouldRemoveKey();
    }

    @Test
    public void shouldRemoveKeyInInternal() throws Exception
    {
        // GIVEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        shouldRemoveKey();
    }

    @Test
    public void shouldSetAndGetValue() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newKey();

        // WHEN
        long firstValue = 123456789;
        value.setValue( firstValue );
        mainSection.insertValueAt( cursor, value, 0, 0 );

        long otherValue = 987654321;
        value.setValue( otherValue );
        mainSection.insertValueAt( cursor, value, 1, 1 );

        // THEN
        assertEquals( firstValue, mainSection.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( otherValue, mainSection.valueAt( cursor, value, 1 ).longValue() );
    }

    @Test
    public void shouldRemoveValue() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newKey();
        long firstValue = 123456789;
        value.setValue( firstValue );
        mainSection.insertValueAt( cursor, value, 0, 0 );
        long otherValue = 987654321;
        value.setValue( otherValue );
        mainSection.insertValueAt( cursor, value, 1, 1 );
        long thirdValue = 49756;
        value.setValue( thirdValue );
        mainSection.insertValueAt( cursor, value, 2, 2 );

        // WHEN
        mainSection.removeValueAt( cursor, 1, 3 );

        // THEN
        assertEquals( firstValue, mainSection.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( thirdValue, mainSection.valueAt( cursor, value, 1 ).longValue() );
    }

    @Test
    public void shouldOverwriteValue() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newValue();
        value.setValue( 1 );
        mainSection.insertValueAt( cursor, value, 0, 0 );

        // WHEN
        long overwrittenValue = 2;
        value.setValue( overwrittenValue );
        mainSection.setValueAt( cursor, value, 0 );

        // THEN
        assertEquals( overwrittenValue, mainSection.valueAt( cursor, value, 0 ).longValue() );
    }

    @Test
    public void shouldSetAndGetChild() throws Exception
    {
        // GIVEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        long firstChild = 123456789;
        mainSection.insertChildAt( cursor, firstChild, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION );

        long otherChild = 987654321;
        mainSection.insertChildAt( cursor, otherChild, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( firstChild, childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( otherChild, childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldOverwriteChild() throws Exception
    {
        // GIVEN
        long child = GenerationSafePointer.MIN_POINTER;
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        mainSection.insertChildAt( cursor, child, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        long overwrittenChild = child + 1;
        mainSection.setChildAt( cursor, overwrittenChild, 0, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( overwrittenChild, childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldSetAndGetKeyCount() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( 0, mainSection.keyCount( cursor ) );

        // WHEN
        int keyCount = 5;
        mainSection.setKeyCount( cursor, keyCount );

        // THEN
        assertEquals( keyCount, mainSection.keyCount( cursor ) );
    }

    @Test
    public void shouldSetAndGetSiblings() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        node.setLeftSibling( cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.setRightSibling( cursor, 456, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( 123, leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 456, rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldSetAndGetSuccessor() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        node.setSuccessor( cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( 123, successor( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldReadAndInsertKeys() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong key = layout.newKey();
        key.setValue( 1 );
        mainSection.insertKeyAt( cursor, key, 0, 0 );
        key.setValue( 3 );
        mainSection.insertKeyAt( cursor, key, 1, 1 );

        // WHEN
        key.setValue( 2 );
        mainSection.insertKeyAt( cursor, key, 1, 2 );

        // THEN
        assertEquals( 1, mainSection.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( 2, mainSection.keyAt( cursor, key, 1 ).longValue() );
        assertEquals( 3, mainSection.keyAt( cursor, key, 2 ).longValue() );
    }

    @Test
    public void shouldReadAndInsertValues() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newKey();
        value.setValue( 1 );
        mainSection.insertValueAt( cursor, value, 0, 0 );
        value.setValue( 3 );
        mainSection.insertValueAt( cursor, value, 1, 1 );

        // WHEN
        value.setValue( 2 );
        mainSection.insertValueAt( cursor, value, 1, 2 );

        // THEN
        assertEquals( 1, mainSection.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( 2, mainSection.valueAt( cursor, value, 1 ).longValue() );
        assertEquals( 3, mainSection.valueAt( cursor, value, 2 ).longValue() );
    }

    @Test
    public void shouldReadAndInsertChildren() throws Exception
    {
        // GIVEN
        long firstChild = GenerationSafePointer.MIN_POINTER;
        long secondChild = firstChild + 1;
        long thirdChild = secondChild + 1;
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        mainSection.insertChildAt( cursor, firstChild, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION );
        mainSection.insertChildAt( cursor, thirdChild, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        mainSection.insertChildAt( cursor, secondChild, 1, 2, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( firstChild, childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( secondChild, childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( thirdChild, childAt( cursor, 2, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldInsertAndRemoveRandomKeysAndValues() throws Exception
    {
        // This test doesn't care about sorting, that's an aspect that lies outside of TreeNode, really

        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        int maxKeyCount = mainSection.leafMaxKeyCount();
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

                    mainSection.insertKeyAt( cursor, key, position, expectedKeyCount );
                    insert( expectedKeys, expectedKeyCount, key.longValue(), position );

                    value.setValue( random.nextLong() );
                    mainSection.insertValueAt( cursor, value, position, expectedKeyCount );
                    insert( expectedValues, expectedKeyCount, value.longValue(), position );

                    mainSection.setKeyCount( cursor, ++expectedKeyCount );
                }
            }
            else
            {   // 30% remove
                if ( expectedKeyCount > 0 )
                {   // there are things to remove
                    int position = random.nextInt( expectedKeyCount );
                    mainSection.keyAt( cursor, key, position );
                    mainSection.removeKeyAt( cursor, position, expectedKeyCount );
                    long expectedKey = remove( expectedKeys, expectedKeyCount, position );
                    assertEquals( expectedKey, key.longValue() );

                    mainSection.valueAt( cursor, value, position );
                    mainSection.removeValueAt( cursor, position, expectedKeyCount );
                    long expectedValue = remove( expectedValues, expectedKeyCount, position );
                    assertEquals( expectedValue, value.longValue() );

                    mainSection.setKeyCount( cursor, --expectedKeyCount );
                }
            }
        }

        // THEN
        assertEquals( expectedKeyCount, mainSection.keyCount( cursor ) );
        for ( int i = 0; i < expectedKeyCount; i++ )
        {
            long expectedKey = expectedKeys[i];
            mainSection.keyAt( cursor, key, i );
            assertEquals( expectedKey, key.longValue() );

            long expectedValue = expectedValues[i];
            mainSection.valueAt( cursor, value, i );
            assertEquals( "For key " + key.longValue(), expectedValue, value.longValue() );
        }
    }

    @Test
    public void shouldAssertPageSizeBigEnoughForAtLeastTwoKeys() throws Exception
    {
        // WHEN
        try
        {
            TreeNodes.instantiateTreeNode( TreeNodeV3.HEADER_LENGTH + layout.keySize() + layout.valueSize(), layout );
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
        node.setRightSibling( cursor, pointer, STABLE_GENERATION, generation );

        // WHEN
        long readResult = node.rightSibling( cursor, STABLE_GENERATION, generation );
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
        node.setRightSibling( cursor, oldPointer, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.setRightSibling( cursor, pointer, UNSTABLE_GENERATION, generation );

        // WHEN
        long readResult = node.rightSibling( cursor, UNSTABLE_GENERATION, generation );
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
        mainSection.setChildAt( cursor, pointer, childPos, STABLE_GENERATION, generation );

        // WHEN
        long readResult = mainSection.childAt( cursor, childPos, STABLE_GENERATION, generation );
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
        mainSection.setChildAt( cursor, pointer, childPos, STABLE_GENERATION, generation );

        // WHEN
        long readResult = mainSection.childAt( cursor, childPos, STABLE_GENERATION, generation );
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
        mainSection.setChildAt( cursor, oldPointer, childPos, STABLE_GENERATION, UNSTABLE_GENERATION );
        mainSection.setChildAt( cursor, pointer, childPos, UNSTABLE_GENERATION, generation );

        // WHEN
        long readResult = mainSection.childAt( cursor, childPos, UNSTABLE_GENERATION, generation );
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
        mainSection.setChildAt( cursor, oldPointer, childPos, STABLE_GENERATION, UNSTABLE_GENERATION );
        mainSection.setChildAt( cursor, pointer, childPos, UNSTABLE_GENERATION, generation );

        // WHEN
        long readResult = mainSection.childAt( cursor, childPos, UNSTABLE_GENERATION, generation );
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
        mainSection.setChildAt( cursor, child, pos, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        long result = mainSection.childAt( cursor, pos, STABLE_GENERATION, UNSTABLE_GENERATION );

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

    private long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        return pointer( mainSection.childAt( cursor, pos, stableGeneration, unstableGeneration ) );
    }

    private long rightSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        return pointer( node.rightSibling( cursor, stableGeneration, unstableGeneration ) );
    }

    private long leftSibling( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        return pointer( node.leftSibling( cursor, stableGeneration, unstableGeneration ) );
    }

    private long successor( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        return pointer( node.successor( cursor, stableGeneration, unstableGeneration ) );
    }
}
