/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@PageCacheExtension
class WriterTest
{
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    private GBPTree<MutableLong,MutableLong> tree;

    @BeforeEach
    void setupTree()
    {
        tree = new GBPTreeBuilder<>( pageCache, directory.file( "tree" ), longLayout().withFixedSize( true ).build() ).build();
    }

    @AfterEach
    void closeTree() throws IOException
    {
        tree.close();
    }

    @Test
    void shouldPutEntry() throws IOException
    {
        // when
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.key().longValue() );
            assertEquals( value, cursor.value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldMergeNonExistentEntry() throws IOException
    {
        // when
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.merge( new MutableLong( key ), new MutableLong( value ), ValueMergers.overwrite() );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.key().longValue() );
            assertEquals( value, cursor.value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldNotChangeEntryOnMergeExistentEntryWithUnchangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.merge( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.keepExisting() );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.key().longValue() );
            assertEquals( value, cursor.value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldChangeEntryOnMergeExistentEntryWithChangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.merge( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.overwrite() );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.key().longValue() );
            assertEquals( value + 1, cursor.value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldNotCreateEntryOnMergeIfExistsForNonExistentEntry() throws IOException
    {
        // when
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.mergeIfExists( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.overwrite() );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldNotChangeEntryOnMergeIfExistsForExistentEntryWithUnchangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.mergeIfExists( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.keepExisting() );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.key().longValue() );
            assertEquals( value, cursor.value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldChangeEntryOnMergeIfExistsForExistentEntryWithChangingMerger() throws IOException
    {
        // given
        long key = 0;
        long value = 10;
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
        {
            writer.mergeIfExists( new MutableLong( key ), new MutableLong( value + 1 ), ValueMergers.overwrite() );
        }

        // then
        try ( Seeker<MutableLong,MutableLong> cursor = tree.seek( new MutableLong( key ), new MutableLong( key ), NULL ) )
        {
            assertTrue( cursor.next() );
            assertEquals( key, cursor.key().longValue() );
            assertEquals( value + 1, cursor.value().longValue() );
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldReturnNullRemovedValueIfNotFound() throws IOException
    {
        // given
        long key = 999;
        long value = 888;
        try ( Writer<MutableLong, MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong, MutableLong> writer = tree.writer( NULL ) )
        {
            MutableLong removedValue = writer.remove( new MutableLong( key + 1 ) );
            // then
            assertNull( removedValue );
        }
    }

    @Test
    void shouldReturnRemovedValueIfFound() throws IOException
    {
        // given
        long key = 999;
        long value = 888;
        try ( Writer<MutableLong, MutableLong> writer = tree.writer( NULL ) )
        {
            writer.put( new MutableLong( key ), new MutableLong( value ) );
        }

        // when
        try ( Writer<MutableLong, MutableLong> writer = tree.writer( NULL ) )
        {
            MutableLong removedValue = writer.remove( new MutableLong( key ) );
            // then
            assertEquals( new MutableLong( value ), removedValue );
        }
    }
}
