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
import org.neo4j.index.ValueAmender;
import org.neo4j.index.ValueAmenders;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.Modifier.Options.DEFAULTS;
import static org.neo4j.index.ValueAmenders.overwrite;

public class IndexModifierTest
{
    private static final ValueAmender<MutableLong> ADDER = (base,add) -> {
        base.add( add.longValue() );
        return base;
    };

    private static final int STABLE_GENERATION = 1;
    private static final int UNSTABLE_GENERATION = 2;
    private final int pageSize = 256;

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( pageSize, layout );
    private final IndexModifier<MutableLong,MutableLong> indexModifier = new IndexModifier<>( id, node, layout );

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );
    private final int maxKeyCount = node.leafMaxKeyCount();

    private final MutableLong insertKey = new MutableLong();
    private final MutableLong insertValue = new MutableLong();
    private final MutableLong readKey = new MutableLong();
    private final MutableLong readValue = new MutableLong();
    private final byte[] tmp = new byte[pageSize];

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
        newRootFromSplit( split );

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
        SplitResult<MutableLong> split = null;
        for ( int i = 0; split == null; i++ )
        {
            split = insert( i, i );
        }
        long rootId = newRootFromSplit( split );

        // when
        cursor.next( split.left );
        assertThat( keyAt( 0 ), is( 0L ) );
        cursor.next( rootId );
        remove( 0, readValue );

        // then
        cursor.next( split.left );
        assertThat( keyAt( 0 ), is( 1L ) );
    }

    @Test
    public void modifierMustRemoveFromRightChildButNotFromInternalWithHitOnInternalSearch() throws Exception
    {
        // given
        SplitResult<MutableLong> split = null;
        for ( int i = 0; split == null; i++ )
        {
            split = insert( i, i );
        }
        long rootId = newRootFromSplit( split );

        // when key to remove exists in internal
        Long keyToRemove = split.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        cursor.next( split.right );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        cursor.next( rootId );
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        cursor.next( split.right );
        assertThat( node.keyCount( cursor ), is( keyCountInRightChild - 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove + 1 ) );
    }

    @Test
    public void modifierMustLeaveCursorOnInitialPageAfterRemove() throws Exception
    {
        // given
        SplitResult<MutableLong> split = null;
        for ( int i = 0; split == null; i++ )
        {
            split = insert( i, i );
        }
        long rootId = newRootFromSplit( split );

        // when
        assertThat( cursor.getCurrentPageId(), is( rootId) );
        remove( split.primKey.getValue(), readValue );

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
        SplitResult<MutableLong> split = null;
        for ( int i = 0; split == null; i++ )
        {
            split = insert( i, i );
        }
        long rootId = newRootFromSplit( split );

        // when key to remove exists in internal
        Long keyToRemove = split.primKey.getValue();
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and as first key in right child
        cursor.next( split.right );
        int keyCountInRightChild = node.keyCount( cursor );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // and we remove it
        cursor.next( rootId );
        remove( keyToRemove, readValue );

        // then we should still find it in internal
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( keyToRemove ) );

        // but not in right leaf
        cursor.next( split.right );
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
            SplitResult<MutableLong> split = insert( random.nextLong(), random.nextLong() );
            if ( split != null )
            {
                newRootFromSplit( split );
            }
        }

        BPTreeConsistencyChecker<MutableLong> consistencyChecker =
                new BPTreeConsistencyChecker<>( node, layout, STABLE_GENERATION, UNSTABLE_GENERATION );
        consistencyChecker.check( cursor );
    }

    /* TEST AMENDER */

    @Test
    public void modifierMustOverwriteWithOverwriteAmender() throws Exception
    {
        // given
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue );

        // when
        long secondValue = random.nextLong();
        insert( key, secondValue, ValueAmenders.overwrite() );

        // then
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( valueAt( 0 ), is( secondValue ) );
    }

    @Test
    public void modifierMustInsertNewWithInsertNewAmender() throws Exception
    {
        // given
        long key = random.nextLong();
        long firstValue = random.nextLong();
        insert( key, firstValue );

        // when
        long secondValue = random.nextLong();
        insert( key, secondValue, ValueAmenders.insertNew() );

        // then
        assertThat( node.keyCount( cursor ), is( 2 ) );
        Long actualFirst = valueAt( 0 );
        assertThat( actualFirst, anyOf( is( firstValue ), is( secondValue ) ) );
        assertThat( valueAt( 1 ), allOf( not( is( actualFirst ) ), anyOf( is( firstValue ), is( secondValue ) ) ) );
    }

    @Test
    public void shouldAmendValueInRootLeaf() throws Exception
    {
        // GIVEN
        long key = 10;
        long baseValue = 100;
        insert( key, baseValue );

        // WHEN
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        int searchResult = IndexSearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( IndexSearch.isHit( searchResult ) );
        int pos = IndexSearch.positionOf( searchResult );
        assertEquals( 0, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    @Test
    public void shouldAmendValueInLeafLeftOfParentKey() throws Exception
    {
        // GIVEN
        SplitResult<MutableLong> split = null;
        for ( int i = 0; split == null; i++ )
        {
            split = insert( i, i );
        }
        newRootFromSplit( split );

        // WHEN
        long key = 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        cursor.next( split.left );
        int searchResult = IndexSearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( IndexSearch.isHit( searchResult ) );
        int pos = IndexSearch.positionOf( searchResult );
        assertEquals( 1, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    @Test
    public void shouldAmendValueInLeafAtParentKey() throws Exception
    {
        // GIVEN
        SplitResult<MutableLong> split = null;
        for ( int i = 0; split == null; i++ )
        {
            split = insert( i, i );
        }
        newRootFromSplit( split );

        // WHEN
        long key = split.primKey.longValue();
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        cursor.next( split.right );
        int searchResult = IndexSearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( IndexSearch.isHit( searchResult ) );
        int pos = IndexSearch.positionOf( searchResult );
        assertEquals( 0, pos );
        assertEquals( key, keyAt( pos ).longValue() );
        assertEquals( baseValue + toAdd, valueAt( pos ).longValue() );
    }

    @Test
    public void shouldAmendValueInLeafBetweenTwoParentKeys() throws Exception
    {
        // GIVEN
        long rootId = -1;
        long middle = -1;
        long firstSplitPrimKey = -1;
        for ( int i = 0; rootId == -1 || node.keyCount( cursor ) == 1; i++ )
        {
            SplitResult<MutableLong> split = insert( i, i );
            if ( split != null )
            {
                rootId = newRootFromSplit( split );
                middle = split.right;
                firstSplitPrimKey = split.primKey.longValue();
            }
        }

        // WHEN
        long key = firstSplitPrimKey + 1;
        long baseValue = key;
        int toAdd = 5;
        insert( key, toAdd, ADDER );

        // THEN
        cursor.next( middle );
        int searchResult = IndexSearch.search( cursor, node, key( key ), new MutableLong(), node.keyCount( cursor ) );
        assertTrue( IndexSearch.isHit( searchResult ) );
        int pos = IndexSearch.positionOf( searchResult );
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

    private long newRootFromSplit( SplitResult<MutableLong> split ) throws IOException
    {
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

    private SplitResult<MutableLong> insert( long key, long value ) throws IOException
    {
        return insert( key, value, overwrite() );
    }

    private SplitResult<MutableLong> insert( long key, long value, ValueAmender<MutableLong> amender )
            throws IOException
    {
        insertKey.setValue( key );
        insertValue.setValue( value );
        return indexModifier.insert( cursor, insertKey, insertValue, amender, DEFAULTS,
                STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private MutableLong remove( long key, MutableLong into ) throws IOException
    {
        insertKey.setValue( key );
        return indexModifier.remove( cursor, insertKey, into, STABLE_GENERATION, UNSTABLE_GENERATION );
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
