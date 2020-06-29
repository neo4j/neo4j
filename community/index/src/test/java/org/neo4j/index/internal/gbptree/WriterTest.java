/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.cursor.RawCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriterTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private GBPTree<MutableLong,MutableLong> tree;

    @Before
    public void setupTree()
    {
        PageCache pageCache = pageCacheRule.getPageCache( directory.getFileSystem() );
        tree = new GBPTreeBuilder<>( pageCache, directory.file( "tree" ), new SimpleLongLayout( 0, "", true, 1, 2, 3 ) ).build();
    }

    @After
    public void closeTree() throws IOException
    {
        tree.close();
    }

    @Test
    public void shouldPutEntry() throws IOException
    {
        // when
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.get().key().longValue() );
            assertEquals( value, cursor.get().value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldMergeNonExistentEntry() throws IOException
    {
        // when
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.merge( new MutableLong( key ), new MutableLong( value ), ValueMergers.overwrite() );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.get().key().longValue() );
            assertEquals( value, cursor.get().value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldNotChangeEntryOnMergeExistentEntryWithUnchangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.merge( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.keepExisting() );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.get().key().longValue() );
            assertEquals( value, cursor.get().value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldChangeEntryOnMergeExistentEntryWithChangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.merge( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.overwrite() );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.get().key().longValue() );
            assertEquals( value + 1, cursor.get().value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldNotCreateEntryOnMergeIfExistsForNonExistentEntry() throws IOException
    {
        // when
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.mergeIfExists( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.overwrite() );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldNotChangeEntryOnMergeIfExistsForExistentEntryWithUnchangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.mergeIfExists( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.keepExisting() );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.get().key().longValue() );
            assertEquals( value, cursor.get().value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldChangeEntryOnMergeIfExistsForExistentEntryWithChangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer() )
        {
            writer.mergeIfExists( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.overwrite() );
        }

        // then
        try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ) ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.get().key().longValue() );
            assertEquals( value + 1, cursor.get().value().longValue() );
            assertFalse( cursor.next() );
        }
    }
}
