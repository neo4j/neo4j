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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.Modifier.Options.DEFAULTS;
import static org.neo4j.index.ValueAmenders.overwrite;

public class IndexModifierTest
{
    private final int pageSize = 128;

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( pageSize, layout );
    private final IndexModifier<MutableLong,MutableLong> indexModifier = new IndexModifier<>( id, node, layout );

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );

    private final MutableLong insertKey = new MutableLong();
    private final MutableLong insertValue = new MutableLong();
    private final MutableLong readKey = new MutableLong();
    private final MutableLong readValue = new MutableLong();

    @Rule
    public RandomRule random = new RandomRule();

    @Before
    public void setUp() throws IOException
    {
        id.reset();
        cursor.initialize();
        cursor.next( id.acquireNewId() );
        node.initializeLeaf( cursor );
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
        // given
        int maxKeyCount = node.leafMaxKeyCount();

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
        int maxKeyCount = node.leafMaxKeyCount();

        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
            insert( (long) i, (long) i );

            // then
            assertThat( keyAt( i ), is( (long) i ) );
            assertThat( valueAt( i ), is( (long) i ) );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertInMiddleOfLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

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
        int maxKeyCount = node.leafMaxKeyCount();

        // when
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i % 2 == 0 ? i / 2 : maxKeyCount - i / 2;
            insert( key, key );
        }

        // then
        long middle = maxKeyCount / 2;
        assertNotNull( insert( middle, middle ) );
    }

    @Test
    public void modifierMustSplitWhenInsertingLastInFullLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        // when
        long key = 0;
        while ( key < maxKeyCount )
        {
            assertNull( insert( key, key ) );
            key++;
        }

        // then
        assertNotNull( insert( key, key ) ); // Should cause a split
    }

    @Test
    public void modifierMustSplitWhenInsertingFirstInFullLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        // when
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i + 1;
            assertNull( insert( key, key ) );
        }

        // then
        assertNotNull( insert( 0L, 0L ) );
    }

    @Test
    public void modifierMustLeaveCursorOnSamePageAfterSplitInLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();
        long pageId = cursor.getCurrentPageId();
        long key = 0;
        while ( key < maxKeyCount )
        {
            assertNull( insert( key, key ) );
            key++;
        }

        // when
        assertNotNull( insert( key, key ) ); // Should cause a split

        // then
        assertThat( cursor.getCurrentPageId(), is( pageId ) );
    }

    @Test
    public void modifierMustUpdatePointersInSiblingsToSplit() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();
        long someLargeNumber = maxKeyCount * 1000;
        long i = 0;
        while ( i < maxKeyCount )
        {
            insert( someLargeNumber - i, i );
            i++;
        }

        // First split
        SplitResult<MutableLong> split = insert( someLargeNumber - i, i );
        i++;
        byte[] tmp = new byte[pageSize];
        newRootFromSplit( tmp, split );

        // Assert child pointers and sibling pointers are intact after split in root
        long child0 = node.childAt( cursor, 0 );
        long child1 = node.childAt( cursor, 1 );
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
        child0 = node.childAt( cursor, 0 );
        child1 = node.childAt( cursor, 1 );
        long child2 = node.childAt( cursor, 2 ); // <- right sibling to split-node before split

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
        remove( key );

        // then
        assertThat( node.keyCount( cursor ), is( 0 ) );
    }

    @Test
    public void modifierMustRemoveFirstInFullLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        remove( 0 );

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
        int maxKeyCount = node.leafMaxKeyCount();
        int middle = maxKeyCount / 2;
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        remove( middle );

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
        int maxKeyCount = node.leafMaxKeyCount();
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            insert( i, i );
        }

        // when
        remove( maxKeyCount - 1 );

        // then
        assertThat( node.keyCount( cursor ), is( maxKeyCount - 1) );
        for ( int i = 0; i < maxKeyCount - 1; i++ )
        {
            Long actual = keyAt( i );
            assertThat( actual, is( (long) i ) );
        }
    }

    /* REBALANCE (when rebalance is implemented) */
    /* MERGE (when merge is implemented) */

    @Test
    public void modifierMustProduceConsistentTreeWithRandomInserts() throws Exception
    {
        byte[] tmp = new byte[pageSize];

        int numberOfEntries = 100_000;
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            SplitResult<MutableLong> split = insert( random.nextLong(), random.nextLong() );
            if ( split != null )
            {
                newRootFromSplit( tmp, split );
            }
        }

        BPTreeConsistencyChecker<MutableLong> consistencyChecker = new BPTreeConsistencyChecker<>( node, layout );
        consistencyChecker.check( cursor );
    }

    private void newRootFromSplit( byte[] tmp, SplitResult<MutableLong> split ) throws IOException
    {
        cursor.next( id.acquireNewId() );
        node.initializeInternal( cursor );
        node.insertKeyAt( cursor, split.primKey, 0, 0, tmp );
        node.setKeyCount( cursor, 1 );
        node.setChildAt( cursor, split.left, 0 );
        node.setChildAt( cursor, split.right, 1 );
    }

    private void assertSiblingOrderAndPointers( long... children ) throws IOException
    {
        long currentPageId = cursor.getCurrentPageId();
        RightmostInChain<MutableLong> rightmost = new RightmostInChain<>( node );
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

    private SplitResult<MutableLong> insert( long key, long value ) throws IOException
    {
        insertKey.setValue( key );
        insertValue.setValue( value );
        return indexModifier.insert( cursor, insertKey, insertValue, overwrite(), DEFAULTS );
    }

    private void remove( long key ) throws IOException
    {
        insertKey.setValue( key );
        indexModifier.remove( cursor, insertKey );
    }

    private class SimpleIdProvider implements IdProvider
    {
        private long lastId = -1;

        @Override
        public long acquireNewId()
        {
            lastId++;
            return lastId;
        }

        private void reset()
        {
            lastId = -1;
        }
    }
}
