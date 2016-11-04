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
import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class SeekCursorTest
{
    private static final int PAGE_SIZE = 256;
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> treeNode = spy( new TreeNode<>( PAGE_SIZE, layout ) );
    private final byte[] tmp = new byte[PAGE_SIZE];
    private final MutableLong key = layout.newKey();
    private final MutableLong value = layout.newValue();

    @Test
    public void shouldFindEntriesWithinRangeInSingleLeaf() throws Exception
    {
        // GIVEN
        PageCursor pageCursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE], 0, PAGE_SIZE );
        treeNode.initializeLeaf( pageCursor );
        int keyCount = 10;
        insertKeysAndValues( pageCursor, keyCount );
        MutableLong from = layout.newKey();
        MutableLong to = layout.newKey();
        from.setValue( 2 );
        to.setValue( 7 );

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = new SeekCursor<>( pageCursor, key, value,
                treeNode, from, to, layout, 1, keyCount ) )
        {
            // THEN
            long expectedKey = from.longValue();
            while ( cursor.next() )
            {
                MutableLong foundKey = cursor.get().key();
                MutableLong foundValue = cursor.get().value();
                assertEquals( expectedKey, foundKey.longValue() );
                assertEquals( expectedKey * 10, foundValue.longValue() );
                expectedKey++;
            }
            assertEquals( 7, expectedKey );
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldRereadHeadersOnRetry() throws Exception
    {
        // GIVEN
        TestPageCursor pageCursor = new TestPageCursor( ByteArrayPageCursor.wrap( new byte[PAGE_SIZE], 0, PAGE_SIZE ) );
        treeNode.initializeLeaf( pageCursor );
        int keyCount = 10;
        insertKeysAndValues( pageCursor, keyCount );
        MutableLong from = layout.newKey();
        MutableLong to = layout.newKey();
        from.setValue( 2 );
        to.setValue( keyCount + 1 ); // +1 because we're adding one more down below

        // WHEN
        try ( SeekCursor<MutableLong,MutableLong> cursor = new SeekCursor<>( pageCursor, key, value,
                treeNode, from, to, layout, 0, keyCount ) )
        {
            // reading a couple of keys
            assertTrue( cursor.next() );
            assertEquals( 2, cursor.get().key().longValue() );
            assertTrue( cursor.next() );
            assertEquals( 3, cursor.get().key().longValue() );

            // and WHEN a change happens
            addKeyAndValue( pageCursor, keyCount, valueForKey( keyCount ) );
            pageCursor.changed();

            // THEN at least keyCount should be re-read on next()
            reset( treeNode );
            assertTrue( cursor.next() );
            verify( treeNode ).keyCount( any( PageCursor.class ) );

            // and the new key should be found in the end as well
            assertEquals( 4, cursor.get().key().longValue() );
            long lastFoundKey = 4;
            while ( cursor.next() )
            {
                assertEquals( lastFoundKey + 1, cursor.get().key().longValue() );
                lastFoundKey = cursor.get().key().longValue();
            }
            assertEquals( keyCount, lastFoundKey );
        }
    }

    // TODO: should go to right sibling when exhausted current leaf, if there's more

    private static long valueForKey( long key )
    {
        return key * 10;
    }

    private void insertKeysAndValues( PageCursor pageCursor, int keyCount )
    {
        for ( int i = 0; i < keyCount; i++ )
        {
            key.setValue( i );
            value.setValue( valueForKey( i ) );
            addKeyAndValue( pageCursor, i, valueForKey( i ) );
        }
    }

    private void addKeyAndValue( PageCursor pageCursor, long k, long v )
    {
        int keyCount = treeNode.keyCount( pageCursor );
        key.setValue( k );
        value.setValue( v );
        treeNode.insertKeyAt( pageCursor, key, keyCount, keyCount, tmp );
        treeNode.insertValueAt( pageCursor, value, keyCount, keyCount, tmp );
        treeNode.setKeyCount( pageCursor, keyCount + 1 );
    }
}
