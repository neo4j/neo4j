/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.store.kvstore.BigEndianByteArrayBuffer.buffer;
import static org.neo4j.kernel.impl.store.kvstore.BigEndianByteArrayBuffer.compare;

/**
 * Stores Key/Value pairs sorted by the key in unsigned big-endian order.
 *
 * @param <META> the type of object to hold the metadata of a store file.
 */
public class KeyValueStoreFile<META> implements Closeable
{
    public META metadata()
    {
        return metadata;
    }

    /**
     * Visit key value pairs that are greater than or equal to the specified key. Visitation will continue as long as
     * the visitor {@link KeyValueVisitor#visit(ReadableBuffer, ReadableBuffer) returns true}.
     *
     * @return {@code true} if an exact match was found, meaning that the first visited key/value pair was a perfect
     * match for the specified key.
     */
    public boolean scan( SearchKey search, KeyValueVisitor visitor ) throws IOException
    {
        BigEndianByteArrayBuffer searchKey = buffer( keySize ), key = buffer( keySize ), value = buffer( valueSize );
        search.searchKey( searchKey );
        int page = findPage( searchKey, pageCatalogue );
        if ( page < 0 )
        {
            return false;
        }
        try ( PageCursor cursor = file.io( page, PF_NO_GROW | PF_SHARED_LOCK ) )
        {
            if ( !cursor.next() )
            {
                return false;
            }
            // finds and reads the first key/value pair
            int offset = findOffset( cursor, searchKey, key, value );
            try
            {
                return Arrays.equals( searchKey.buffer, key.buffer );
            }
            finally
            {
                visitKeyValuePairs( file.pageSize(), cursor, offset, visitor, false, key, value );
            }
        }
    }

    public DataProvider dataProvider() throws IOException
    {
        int pageId = headerRecords * (keySize + valueSize) / file.pageSize();
        final PageCursor cursor = file.io( pageId, PF_NO_GROW | PF_SHARED_LOCK );
        return new DataProvider()
        {
            int offset = headerRecords * (keySize + valueSize);
            boolean done = !cursor.next();

            @Override
            public boolean visit( WritableBuffer key, WritableBuffer value ) throws IOException
            {
                if ( done )
                {
                    return false;
                }
                readKeyValuePair( cursor, offset, key, value );
                if ( key.allZeroes() )
                {
                    done = true;
                    return false;
                }
                offset += key.size() + value.size();
                if ( offset >= file.pageSize() )
                {
                    offset = 0;
                    if ( !cursor.next() )
                    {
                        done = true;
                    }
                }
                return true;
            }

            @Override
            public void close() throws IOException
            {
                cursor.close();
            }
        };
    }

    public void scan( KeyValueVisitor visitor ) throws IOException
    {
        scanAll( file, headerRecords * (keySize + valueSize), visitor,
                 new BigEndianByteArrayBuffer( new byte[keySize] ),
                 new BigEndianByteArrayBuffer( new byte[valueSize] ) );
    }

    public int recordCount()
    {
        return totalRecords - headerRecords;
    }

    @Override
    public void close() throws IOException
    {
        file.close();
    }

    private final PagedFile file;
    private final int keySize;
    private final int valueSize;
    private final META metadata;
    private final int headerRecords;
    /** Includes header records (and data records), but not the trailer record. */
    private final int totalRecords;
    /**
     * The page catalogue is used to find the appropriate (first) page without having to do I/O.
     * <p/>
     * <b>Content:</b> {@code (minKey, maxKey)+}, one entry (at {@code 2 x} {@link #keySize}) for each page.
     */
    private final byte[] pageCatalogue;

    KeyValueStoreFile( PagedFile file, int keySize, int valueSize, Metadata<META> metadata )
    {
        this.file = file;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.headerRecords = metadata.headerRecords();
        this.totalRecords = metadata.totalRecords();
        this.metadata = metadata.metadata();
        this.pageCatalogue = metadata.pageCatalogue();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + file + "]";
    }

    static <Buffer extends BigEndianByteArrayBuffer> void scanAll(
            PagedFile file, int startOffset, EntryVisitor<? super Buffer> visitor,
            Buffer key, Buffer value )
            throws IOException
    {
        boolean visitMeta = !(visitor instanceof KeyValueVisitor);
        try ( PageCursor cursor = file.io( startOffset / file.pageSize(), PF_NO_GROW | PF_SHARED_LOCK ) )
        {
            if ( !cursor.next() )
            {
                return;
            }
            readKeyValuePair( cursor, startOffset, key, value );
            visitKeyValuePairs( file.pageSize(), cursor, startOffset, visitor, visitMeta, key, value );
        }
    }

    /** Expects the first key/value-pair to be read into the buffers already, reads subsequent pairs (if requested). */
    private static <Buffer extends BigEndianByteArrayBuffer> void visitKeyValuePairs(
            int pageSize, PageCursor cursor, int offset, EntryVisitor<? super Buffer> visitor, boolean visitMeta,
            Buffer key, Buffer value )
            throws IOException
    {
        while ( visitable( key, visitMeta ) && visitor.visit( key, value ) )
        {
            offset += key.size() + value.size();
            if ( offset >= pageSize )
            {
                offset = 0;
                if ( !cursor.next() )
                {
                    return;
                }
            }
            // reads the next key/value pair
            readKeyValuePair( cursor, offset, key, value );
        }
    }

    private static boolean visitable( BigEndianByteArrayBuffer key, boolean acceptZeroKey )
    {
        if ( !acceptZeroKey )
        {
            if ( key.allZeroes() )
            {
                return false;
            }
        }
        return true;
    }

    private static void readKeyValuePair( PageCursor cursor, int offset, WritableBuffer key, WritableBuffer value )
            throws IOException
    {
        do
        {
            cursor.setOffset( offset );
            key.getFrom( cursor );
            value.getFrom( cursor );
        } while ( cursor.shouldRetry() );
    }

    /**
     * Finds the page that would contain the given key from the {@linkplain #pageCatalogue page catalogue}.
     *
     * @param key The key to look for.
     * @return {@code -1} if the key is not contained in any page,
     * otherwise the id of the page that would contain the key is returned.
     */
    static int findPage( BigEndianByteArrayBuffer key, byte[] catalogue )
    {
        int max = catalogue.length / (key.size() * 2) - 1;
        int min = 0;
        for ( int mid; min <= max; )
        {
            mid = min + (max - min) / 2;
            // look at the low mark for the page
            int cmp = compare( key.buffer, catalogue, mid * key.size() * 2 );
            if ( cmp == 0 )
            {// this page starts with the key
                max = mid; // the previous page might also contain mid the key
            }
            if ( cmp > 0 )
            {
                // look at the high mark for the page
                cmp = compare( key.buffer, catalogue, mid * key.size() * 2 + key.size() );
                if ( cmp <= 0 )
                {
                    return mid; // the key is within the range of this page
                }
                else // look at pages after 'mid'
                {
                    min = mid + 1;
                }
            }
            else // look at pages before 'mid'
            {
                max = mid - 1;
            }
        }
        return min; // the first page after the value that was smaller than the key (mid + 1, you know...)
    }

    /**
     * @param cursor    the cursor for the page to search for the key in.
     * @param searchKey the key to search for.
     * @param key       a buffer to write the key into.
     * @param value     a buffer to write the value into.
     * @return the offset (in the given page) of the first entry with a key that is greater than or equal to the given
     * key.
     */
    private int findOffset( PageCursor cursor, BigEndianByteArrayBuffer searchKey,
                            BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value ) throws IOException
    {
        int recordSize = searchKey.size() + value.size(), last = maxPage( file.pageSize(), recordSize, totalRecords );
        int record = recordOffset( cursor, searchKey, key, value,
                        /* min: */ (cursor.getCurrentPageId() == 0) ? headerRecords : 0/*skip header in first page*/,
                        /* max: */ (cursor.getCurrentPageId() == last)/*exclude trailer in last page*/
                                   ? (totalRecords % (file.pageSize() / recordSize)) - 1
                                   : file.pageSize() / recordSize );
        return record * recordSize;
    }

    static int maxPage( int pageSize, int recordSize, int totalRecords )
    {
        int maxPage = totalRecords / (pageSize / recordSize);
        return maxPage * (pageSize / recordSize) == totalRecords ? maxPage - 1 : maxPage;
    }

    static int recordOffset( PageCursor cursor, BigEndianByteArrayBuffer searchKey,
                             BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value, int min, int max )
            throws IOException
    {
        int recordSize = key.size() + value.size();
        for ( int mid; min <= max; )
        {
            mid = min + (max - min) / 2;
            readKeyValuePair( cursor, mid * recordSize, key, value );
            if ( min == max )
            {
                break; // break here instead of in the loop condition to ensure the right key is read
            }
            int cmp = compare( searchKey.buffer, key.buffer, 0 );
            if ( cmp > 0 ) // search key bigger than found key, continue after 'mid'
            {
                min = mid + 1;
            }
            else // search key smaller (or equal to) than found key, continue before 'mid'
            {
                max = mid; // don't add, greater than are to be included...
            }
        }
        return max;
    }
}
