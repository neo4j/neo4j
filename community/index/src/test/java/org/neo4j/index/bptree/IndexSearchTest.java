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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.contains;
import static org.neo4j.index.bptree.ByteArrayPageCursor.wrap;

public class IndexSearchTest
{
    private static final int KEY_COUNT = 10;
    private static final int PAGE_SIZE = 512;
    private final PageCursor cursor = wrap( new byte[PAGE_SIZE], 0, PAGE_SIZE );
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( PAGE_SIZE, layout );
    private final byte[] tmp = new byte[PAGE_SIZE];
    private final MutableLong readKey = layout.newKey();

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void setup()
    {
        // [2,4,8,16,32,64,128,512,1024,2048]
        node.initializeLeaf( cursor );
        MutableLong key = layout.newKey();
        for ( int i = 0; i < KEY_COUNT; i++ )
        {
            key.setValue( key( i ) );
            node.insertKeyAt( cursor, key, i, i, tmp );
        }
        node.setKeyCount( cursor, KEY_COUNT );
    }

    private int key( int i )
    {
        return 2 << i;
    }

    @Test
    public void shouldFindExistingKey() throws Exception
    {
        // WHEN
        MutableLong key = layout.newKey();
        for ( int i = 0; i < KEY_COUNT; i++ )
        {
            key.setValue( key( i ) );
            int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

            // THEN
            // IndexSearch caters for children as well and so even if there's a hit on, say key:0
            // it will return 1. We can parry for this in the index code in general by checking isHit method
            assertSearchResult( true, i, result );
        }
    }

    @Test
    public void shouldReturnFirstPositionOnLesserThen() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();
        key.setValue( 0 );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

        // THEN
        assertSearchResult( false, 0, result );
    }

    @Test
    public void shouldReturnLastPositionOnGreaterThan() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();
        key.setValue( key( KEY_COUNT + 1 ) );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

        // THEN
        assertSearchResult( false, KEY_COUNT, result );
    }

    @Test
    public void shouldReturnCorrectIndexesForKeysInBetweenExisting() throws Exception
    {
        // WHEN
        MutableLong key = layout.newKey();
        for ( int i = 1; i < KEY_COUNT - 1; i++ )
        {
            key.setValue( key( i ) - 1 );
            int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

            // THEN
            assertSearchResult( false, i, result );
        }
    }

    @Test
    public void searchNoKeys()
    {
        // GIVEM
        MutableLong key = layout.newKey();
        key.setValue( 1 );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, layout.newKey(), 0 );

        // THEN
        assertSearchResult( false, 0, result );
    }

    @Test
    public void searchEqualSingleKey()
    {
        // GIVEN
        MutableLong key = layout.newKey();
        key.setValue( 1 );
        node.insertKeyAt( cursor, key, 0, 0, tmp );
        node.setKeyCount( cursor, 1 );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, 1 );

        // THEN
        assertSearchResult( true, 0, result );
    }

    @Test
    public void searchSingleKeyWithKeyCountZero()
    {
        // GIVEN
        MutableLong key = layout.newKey();
        key.setValue( 1 );
        node.insertKeyAt( cursor, key, 0, 0, tmp );
        node.setKeyCount( cursor, 0 );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, 0 );

        // THEN
        assertSearchResult( false, 0, result );
    }

    @Test
    public void searchEqualMultipleKeys()
    {
        // GIVEN [1,2,2]
        MutableLong key = layout.newKey();
        key.setValue( 0 );
        node.insertKeyAt( cursor, key, 0, 0, tmp );
        key.setValue( 1 );
        node.insertKeyAt( cursor, key, 1, 1, tmp );
        node.insertKeyAt( cursor, key, 2, 2, tmp );
        node.setKeyCount( cursor, 3 );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, 3 );

        // THEN find [1,2,2]
        //              ^
        assertSearchResult( true, 1, result );
    }

    @Test
    public void shouldSearchAndFindOnRandomData() throws Exception
    {
        // GIVEN a leaf node with random, although sorted (as of course it must be to binary-search), data
        int internalMaxKeyCount = node.internalMaxKeyCount();
        int half = internalMaxKeyCount / 2;
        int keyCount = random.nextInt( half ) + half;
        long[] keys = new long[keyCount];
        int currentKey = random.nextInt( 10_000 );
        MutableLong key = layout.newKey();
        for ( int i = 0; i < keyCount; i++ )
        {
            keys[i] = currentKey;
            key.setValue( currentKey );
            node.insertKeyAt( cursor, key, i, i, tmp );
            currentKey += random.nextInt( 100 ) + 10;
        }
        node.setKeyCount( cursor, keyCount );

        // WHEN searching for random keys within that general range
        for ( int i = 0; i < 1_000; i++ )
        {
            long searchKey = random.nextInt( currentKey + 10 );
            key.setValue( searchKey );
            int searchResult = IndexSearch.search( cursor, node, key, readKey, keyCount );

            // THEN position should be as expected
            boolean exists = contains( keys, 0, keyCount, searchKey );
            int position = IndexSearch.positionOf( searchResult );
            assertEquals( exists, IndexSearch.isHit( searchResult ) );
            if ( searchKey <= keys[0] )
            {   // Our search key was lower than any of our keys, expect 0
                assertEquals( 0, position );
            }
            else
            {   // step backwards through our expected keys and see where it should fit, assert that fact
                boolean found = false;
                for ( int j = keyCount - 1; j >= 0; j-- )
                {
                    if ( searchKey > keys[j] )
                    {
                        assertEquals( j+1, position );
                        found = true;
                        break;
                    }
                }

                assertTrue( found );
            }
        }
    }

    private void assertSearchResult( boolean hit, int position, int searchResult )
    {
        assertEquals( hit, IndexSearch.isHit( searchResult ) );
        assertEquals( position, IndexSearch.positionOf( searchResult ) );
    }
}
