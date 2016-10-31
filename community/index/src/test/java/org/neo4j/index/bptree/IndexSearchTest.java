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
import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.bptree.ByteArrayPageCursor.wrap;

public class IndexSearchTest
{
    private static final int KEY_COUNT = 10;
    private static final int PAGE_SIZE = 512;
    private final PageCursor cursor = wrap( new byte[PAGE_SIZE], 0, PAGE_SIZE );
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( PAGE_SIZE, layout );
    private final byte[] tmp = new byte[PAGE_SIZE];

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
        MutableLong readKey = layout.newKey();
        for ( int i = 0; i < KEY_COUNT; i++ )
        {
            key.setValue( key( i ) );
            int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

            // THEN
            assertTrue( IndexSearch.isHit( result ) );
            // IndexSearch caters for children as well and so even if there's a hit on, say key:0
            // it will return 1. We can parry for this in the index code in general by checking isHit method
            assertEquals( i + 1, IndexSearch.positionOf( result ) );
        }
    }

    @Test
    public void shouldReturnFirstPositionOnLesserThen() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();
        MutableLong readKey = layout.newKey();
        key.setValue( 0 );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

        // THEN
        assertFalse( IndexSearch.isHit( result ) );
        assertEquals( 0, IndexSearch.positionOf( result ) );
    }

    @Test
    public void shouldReturnLastPositionOnGreaterThan() throws Exception
    {
        // GIVEN
        MutableLong key = layout.newKey();
        MutableLong readKey = layout.newKey();
        key.setValue( key( KEY_COUNT + 1 ) );

        // WHEN
        int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

        // THEN
        assertFalse( IndexSearch.isHit( result ) );
        assertEquals( KEY_COUNT, IndexSearch.positionOf( result ) );
    }

    @Test
    public void shouldReturnCorrectIndexesForKeysInBetweenExisting() throws Exception
    {
        // WHEN
        MutableLong key = layout.newKey();
        MutableLong readKey = layout.newKey();
        for ( int i = 1; i < KEY_COUNT - 1; i++ )
        {
            key.setValue( key( i ) - 1 );
            int result = IndexSearch.search( cursor, node, key, readKey, KEY_COUNT );

            // THEN
            assertFalse( IndexSearch.isHit( result ) );
            // IndexSearch caters for children as well and so even if there's a hit on, say key:0
            // it will return 1. We can parry for this in the index code in general by checking isHit method
            assertEquals( i, IndexSearch.positionOf( result ) );
        }
    }
}
