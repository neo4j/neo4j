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
package org.neo4j.index.bptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.contains;
import static org.neo4j.index.bptree.ByteArrayPageCursor.wrap;
import static org.neo4j.index.bptree.TreeNode.NO_NODE_FLAG;

public class TreeNodeTest
{
    private static final int STABLE_GENERATION = 1;
    private static final int UNSTABLE_GENERATION = 2;

    private static final int PAGE_SIZE = 512;
    private final PageCursor cursor = wrap( new byte[PAGE_SIZE], 0, PAGE_SIZE );
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( PAGE_SIZE, layout );
    private final byte[] tmp = new byte[PAGE_SIZE];

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldInitializeLeaf() throws Exception
    {
        // WHEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertTrue( node.isLeaf( cursor ) );
        assertFalse( node.isInternal( cursor ) );
        assertEquals( 0, node.keyCount( cursor ) );
        assertEquals( NO_NODE_FLAG, node.leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, node.rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldInitializeInternal() throws Exception
    {
        // WHEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertFalse( node.isLeaf( cursor ) );
        assertTrue( node.isInternal( cursor ) );
        assertEquals( 0, node.keyCount( cursor ) );
        assertEquals( NO_NODE_FLAG, node.leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, node.rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( NO_NODE_FLAG, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    private void shouldSetAndGetKey() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();

        // WHEN
        long firstKey = 10;
        key.setValue( firstKey );
        node.insertKeyAt( cursor, key, 0, 0, tmp );

        long otherKey = 19;
        key.setValue( otherKey );
        node.insertKeyAt( cursor, key, 1, 1, tmp );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( otherKey, node.keyAt( cursor, key, 1 ).longValue() );
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
        node.insertKeyAt( cursor, key, 0, 0, tmp );
        long otherKey = 19;
        key.setValue( otherKey );
        node.insertKeyAt( cursor, key, 1, 1, tmp );
        long thirdKey = 123;
        key.setValue( thirdKey );
        node.insertKeyAt( cursor, key, 2, 2, tmp );

        // WHEN
        node.removeKeyAt( cursor, 1, 3, tmp );

        // THEN
        assertEquals( firstKey, node.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( thirdKey, node.keyAt( cursor, key, 1 ).longValue() );
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
        node.insertValueAt( cursor, value, 0, 0, tmp );

        long otherValue = 987654321;
        value.setValue( otherValue );
        node.insertValueAt( cursor, value, 1, 1, tmp );

        // THEN
        assertEquals( firstValue, node.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( otherValue, node.valueAt( cursor, value, 1 ).longValue() );
    }

    @Test
    public void shouldRemoveValue() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newKey();
        long firstValue = 123456789;
        value.setValue( firstValue );
        node.insertValueAt( cursor, value, 0, 0, tmp );
        long otherValue = 987654321;
        value.setValue( otherValue );
        node.insertValueAt( cursor, value, 1, 1, tmp );
        long thirdValue = 49756;
        value.setValue( thirdValue );
        node.insertValueAt( cursor, value, 2, 2, tmp );

        // WHEN
        node.removeValueAt( cursor, 1, 3, tmp );

        // THEN
        assertEquals( firstValue, node.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( thirdValue, node.valueAt( cursor, value, 1 ).longValue() );
    }

    @Test
    public void shouldOverwriteValue() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newValue();
        value.setValue( 1 );
        node.insertValueAt( cursor, value, 0, 0, tmp );

        // WHEN
        long overwrittenValue = 2;
        value.setValue( overwrittenValue );
        node.setValueAt( cursor, value, 0 );

        // THEN
        assertEquals( overwrittenValue, node.valueAt( cursor, value, 0 ).longValue() );
    }

    @Test
    public void shouldSetAndGetChild() throws Exception
    {
        // GIVEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        long firstChild = 123456789;
        node.insertChildAt( cursor, firstChild, 0, 0, tmp, STABLE_GENERATION, UNSTABLE_GENERATION );

        long otherChild = 987654321;
        node.insertChildAt( cursor, otherChild, 1, 1, tmp, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( firstChild, node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( otherChild, node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldOverwriteChild() throws Exception
    {
        // GIVEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.insertChildAt( cursor, 1, 0, 0, tmp, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        long overwrittenChild = 2;
        node.setChildAt( cursor, overwrittenChild, 0, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( overwrittenChild, node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldSetAndGetKeyCount() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        assertEquals( 0, node.keyCount( cursor ) );

        // WHEN
        int keyCount = 5;
        node.setKeyCount( cursor, keyCount );

        // THEN
        assertEquals( keyCount, node.keyCount( cursor ) );
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
        assertEquals( 123, node.leftSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 456, node.rightSibling( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldSetAndNewGen() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        node.setNewGen( cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        assertEquals( 123, node.newGen( cursor, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldReadAndInsertKeys() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong key = layout.newKey();
        key.setValue( 1 );
        node.insertKeyAt( cursor, key, 0, 0, tmp );
        key.setValue( 3 );
        node.insertKeyAt( cursor, key, 1, 1, tmp );

        // WHEN
        node.readKeysWithInsertRecordInPosition( cursor, c -> c.putLong( 2 ), 1, 3, tmp );
        node.writeKeys( cursor, tmp, 0, 0, 3 );

        // THEN
        assertEquals( 1, node.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( 2, node.keyAt( cursor, key, 1 ).longValue() );
        assertEquals( 3, node.keyAt( cursor, key, 2 ).longValue() );
    }

    @Test
    public void shouldReadAndInsertValues() throws Exception
    {
        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        MutableLong value = layout.newKey();
        value.setValue( 1 );
        node.insertValueAt( cursor, value, 0, 0, tmp );
        value.setValue( 3 );
        node.insertValueAt( cursor, value, 1, 1, tmp );

        // WHEN
        node.readValuesWithInsertRecordInPosition( cursor, c -> c.putLong( 2 ), 1, 3, tmp );
        node.writeValues( cursor, tmp, 0, 0, 3 );

        // THEN
        assertEquals( 1, node.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( 2, node.valueAt( cursor, value, 1 ).longValue() );
        assertEquals( 3, node.valueAt( cursor, value, 2 ).longValue() );
    }

    @Test
    public void shouldReadAndInsertChildren() throws Exception
    {
        // GIVEN
        node.initializeInternal( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.insertChildAt( cursor, 1, 0, 0, tmp, STABLE_GENERATION, UNSTABLE_GENERATION );
        node.insertChildAt( cursor, 3, 1, 1, tmp, STABLE_GENERATION, UNSTABLE_GENERATION );

        // WHEN
        node.readChildrenWithInsertRecordInPosition( cursor, c -> node.writeChild( c, 2, STABLE_GENERATION, UNSTABLE_GENERATION ), 1, 3, tmp );
        node.writeChildren( cursor, tmp, 0, 0, 3 );

        // THEN
        assertEquals( 1, node.childAt( cursor, 0, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 2, node.childAt( cursor, 1, STABLE_GENERATION, UNSTABLE_GENERATION ) );
        assertEquals( 3, node.childAt( cursor, 2, STABLE_GENERATION, UNSTABLE_GENERATION ) );
    }

    @Test
    public void shouldInsertAndRemoveRandomKeysAndValues() throws Exception
    {
        // This test doesn't care about sorting, that's an aspect that lies outside of TreeNode, really

        // GIVEN
        node.initializeLeaf( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
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

                    node.insertKeyAt( cursor, key, position, expectedKeyCount, tmp );
                    insert( expectedKeys, expectedKeyCount, key.longValue(), position );

                    value.setValue( random.nextLong() );
                    node.insertValueAt( cursor, value, position, expectedKeyCount, tmp );
                    insert( expectedValues, expectedKeyCount, value.longValue(), position );

                    node.setKeyCount( cursor, ++expectedKeyCount );
                }
            }
            else
            {   // 30% remove
                if ( expectedKeyCount > 0 )
                {   // there are things to remove
                    int position = random.nextInt( expectedKeyCount );
                    node.keyAt( cursor, key, position );
                    node.removeKeyAt( cursor, position, expectedKeyCount, tmp );
                    long expectedKey = remove( expectedKeys, expectedKeyCount, position );
                    assertEquals( expectedKey, key.longValue() );

                    node.valueAt( cursor, value, position );
                    node.removeValueAt( cursor, position, expectedKeyCount, tmp );
                    long expectedValue = remove( expectedValues, expectedKeyCount, position );
                    assertEquals( expectedValue, value.longValue() );

                    node.setKeyCount( cursor, --expectedKeyCount );
                }
            }
        }

        // THEN
        assertEquals( expectedKeyCount, node.keyCount( cursor ) );
        for ( int i = 0; i < expectedKeyCount; i++ )
        {
            long expectedKey = expectedKeys[i];
            node.keyAt( cursor, key, i );
            assertEquals( expectedKey, key.longValue() );

            long expectedValue = expectedValues[i];
            node.valueAt( cursor, value, i );
            assertEquals( "For key " + key.longValue(), expectedValue, value.longValue() );
        }
    }

    private long remove( long[] from, int length, int position )
    {
        long result = from[position];
        for ( int i = position; i < length; i++ )
        {
            from[i] = from[i + 1];
        }
        return result;
    }

    private void insert( long[] into, int length, long value, int position )
    {
        for ( int i = length - 1; i >= position; i-- )
        {
            into[i + 1] = into[i];
        }
        into[position] = value;
    }
}
