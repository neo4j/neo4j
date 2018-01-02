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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.io.pagecache.StubPageCursor;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFile.maxPage;
import static org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFileTest.CataloguePage.findPage;
import static org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFileTest.CataloguePage.page;

public class KeyValueStoreFileTest
{
    @Test
    public void shouldFindPageInPageCatalogue() throws Exception
    {
        assertEquals( "(single page) in middle of range", 0, findPage( 50, page( 1, 100 ) ) );
        assertEquals( "(single page) at beginning of range", 0, findPage( 1, page( 1, 100 ) ) );
        assertEquals( "(single page) at end of range", 0, findPage( 100, page( 1, 100 ) ) );
        assertEquals( "(single page) before range", 0, findPage( 1, page( 10, 100 ) ) );
        assertEquals( "(single page) after range", 1, findPage( 200, page( 1, 100 ) ) );

        assertEquals( "(two pages) at beginning of second page", 1, findPage( 11, page( 1, 10 ), page( 11, 20 ) ) );
        assertEquals( "(two pages) at end of first page", 0, findPage( 10, page( 1, 10 ), page( 11, 20 ) ) );
        assertEquals( "(two pages) between pages (-> second page)", 1, findPage( 11, page( 1, 10 ), page( 21, 30 ) ) );
        assertEquals( "(two pages) between pages (-> second page)", 1, findPage( 11, page( 1, 10 ), page( 12, 30 ) ) );
        assertEquals( "(two pages) after pages", 2, findPage( 31, page( 1, 10 ), page( 21, 30 ) ) );

        assertEquals( "(three pages) after pages", 3, findPage( 100, page( 1, 10 ), page( 21, 30 ), page( 41, 50 ) ) );

        assertEquals( "overlapping page boundary", 0, findPage( 17, page( 2, 17 ), page( 17, 32 ), page( 32, 50 ) ) );
        assertEquals( "multiple pages with same key", 1,
                      findPage( 3, page( 1, 2 ), page( 2, 3 ),
                                page( 3, 3 ), page( 3, 3 ), page( 3, 3 ), page( 3, 3 ), page( 3, 3 ), page( 3, 3 ),
                                page( 3, 4 ), page( 5, 6 ) ) );
    }

    /** key size = 1 byte */
    static class CataloguePage
    {
        static int findPage( int key, CataloguePage... pages )
        {
            assert key >= 0 && key <= 0xFF : "invalid usage";
            byte[] catalogue = new byte[pages.length * 2];
            for ( int i = 0, min = 0; i < pages.length; i++ )
            {
                CataloguePage page = pages[i];
                assert (page.first & 0xFF) >= min : "invalid catalogue";
                catalogue[i * 2] = page.first;
                catalogue[i * 2 + 1] = page.last;
                min = page.last & 0xFF;
            }
            return KeyValueStoreFile.findPage( new BigEndianByteArrayBuffer( new byte[]{(byte) key} ), catalogue );
        }

        static CataloguePage page( int first, int last )
        {
            assert first >= 0 && last >= 0 && first <= 0xFF && last <= 0xFF && first <= last : "invalid usage";
            return new CataloguePage( (byte) first, (byte) last );
        }

        final byte first, last;

        CataloguePage( byte first, byte last )
        {
            this.first = first;
            this.last = last;
        }
    }

    @Test
    public void shouldComputeMaxPage() throws Exception
    {
        assertEquals( "less than one page", 0, maxPage( 1024, 4, 100 ) );
        assertEquals( "exactly one page", 0, maxPage( 1024, 4, 256 ) );
        assertEquals( "just over one page", 1, maxPage( 1024, 4, 257 ) );
        assertEquals( "exactly two pages", 1, maxPage( 1024, 4, 512 ) );
        assertEquals( "over two pages", 2, maxPage( 1024, 4, 700 ) );
    }

    @Test
    public void shouldFindRecordInPage() throws Exception
    {
        // given
        byte[] key = new byte[1], value = new byte[3];
        DataPage page = new DataPage( 4096, 5, 256, key, value )
        {
            @Override
            void writeDataEntry( int record, WritableBuffer key, WritableBuffer value )
            {
                key.putByte( 0, (byte) record );
            }
        };

        // when/then
        for ( int i = 0; i < 256; i++ )
        {
            assertEquals( i + 5, page.findOffset( i ) );
            assertEquals( i, key[0] & 0xFF );
        }
    }

    @Test
    public void shouldFindRecordInPageWithDuplicates() throws Exception
    {
        // given
        final byte[] keys = new byte[]{1, 2, 2, 3, 4};
        byte[] key = new byte[1], value = new byte[3];
        DataPage page = new DataPage( 4096, 0, 5, key, value )
        {
            @Override
            void writeDataEntry( int record, WritableBuffer key, WritableBuffer value )
            {
                key.putByte( 0, keys[record] );
                value.putByte( 0, (byte) record );
            }
        };

        // when/then
        assertEquals( 0, page.findOffset( 0 ) );
        assertEquals( 0, value[0] & 0xFF );
        assertEquals( 1, page.findOffset( 1 ) );
        assertEquals( 1, value[0] & 0xFF );
        assertEquals( 1, page.findOffset( 2 ) );
        assertEquals( 1, value[0] & 0xFF );
        assertEquals( 3, page.findOffset( 3 ) );
        assertEquals( 3, value[0] & 0xFF );
        assertEquals( 4, page.findOffset( 4 ) );
        assertEquals( 4, value[0] & 0xFF );
    }

    @Test
    public void shouldFindFirstRecordGreaterThanIfNoExactMatch() throws Exception
    {
        // given
        byte[] key = new byte[1], value = new byte[3];
        final AtomicInteger delta = new AtomicInteger( 1 );
        DataPage page = new DataPage( 4096, 3, 128, key, value )
        {
            @Override
            void writeDataEntry( int record, WritableBuffer key, WritableBuffer value )
            {
                key.putByte( 0, (byte) (record * 2 + delta.get()) );
            }
        };
        delta.set( 0 );

        // when / then
        for ( int i = 0; i < 128; i++ )
        {
            assertEquals( i + 3, page.findOffset( i ) );
            assertEquals( (i * 2) + 1, key[0] & 0xFF );
        }
    }

    static abstract class DataPage extends StubPageCursor
    {
        private final int headerRecords;
        private final int dataRecords;
        private final byte[] key;
        private final byte[] value;

        DataPage( int pageSize, int headerRecords, int dataRecords, byte[] key, byte[] value )
        {
            super( 0, pageSize );
            int recordSize = key.length + value.length;
            assert /*power of two*/(recordSize & -recordSize) == recordSize : "invalid usage";
            assert recordSize * (headerRecords + dataRecords) <= pageSize : "invalid usage";
            assert dataRecords <= (1 << (key.length * 8)) : "invalid usage";
            this.key = key;
            this.value = value;
            this.headerRecords = headerRecords;
            this.dataRecords = dataRecords;
            BigEndianByteArrayBuffer k = new BigEndianByteArrayBuffer( key );
            BigEndianByteArrayBuffer v = new BigEndianByteArrayBuffer( value );
            for ( int record = 0; record < dataRecords; record++ )
            {
                writeDataEntry( record, k, v );
                for ( int i = 0; i < key.length; i++ )
                {
                    putByte( (record + headerRecords) * recordSize + i, key[i] );
                }
                for ( int i = 0; i < value.length; i++ )
                {
                    putByte( (record + headerRecords) * recordSize + key.length + i, value[i] );
                }
                Arrays.fill( key, (byte) 0 );
                Arrays.fill( value, (byte) 0 );
            }
        }

        int findOffset( int key ) throws IOException
        {
            BigEndianByteArrayBuffer searchKey = new BigEndianByteArrayBuffer( this.key.length );
            BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( this.value );
            writeDataEntry( key, searchKey, value );
            Arrays.fill( this.value, (byte) 0 );
            return KeyValueStoreFile.findEntryOffset( this, searchKey, new BigEndianByteArrayBuffer( this.key ), value,
                                                      headerRecords, headerRecords + dataRecords );
        }

        abstract void writeDataEntry( int record, WritableBuffer key, WritableBuffer value );
    }
}