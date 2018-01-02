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
 */
public class KeyValueStoreFile implements Closeable
{
    private final PagedFile file;
    private final int keySize;
    private final int valueSize;
    private final Headers headers;
    private final int headerEntries;
    /** Includes header, data and trailer entries. */
    private final int totalEntries;
    /**
     * The page catalogue is used to find the appropriate (first) page without having to do I/O.
     * <p>
     * <b>Content:</b> {@code (minKey, maxKey)+}, one entry (at {@code 2 x} {@link #keySize}) for each page.
     */
    private final byte[] pageCatalogue;

    KeyValueStoreFile( PagedFile file, int keySize, int valueSize, Metadata metadata )
    {
        this.file = file;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.headerEntries = metadata.headerEntries();
        this.totalEntries = metadata.totalEntries();
        this.headers = metadata.headers();
        this.pageCatalogue = metadata.pageCatalogue();
    }

    public Headers headers()
    {
        return headers;
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
        if ( page < 0 || (page >= pageCatalogue.length / (keySize * 2)) )
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
            int offset = findByteOffset( cursor, searchKey, key, value );
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
        int pageId = headerEntries * (keySize + valueSize) / file.pageSize();
        final PageCursor cursor = file.io( pageId, PF_NO_GROW | PF_SHARED_LOCK );
        return new DataProvider()
        {
            int offset = headerEntries * (keySize + valueSize);
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
        scanAll( file, headerEntries * (keySize + valueSize), visitor,
                new BigEndianByteArrayBuffer( new byte[keySize] ),
                new BigEndianByteArrayBuffer( new byte[valueSize] ) );
    }

    public int entryCount()
    {
        return totalEntries - headerEntries;
    }

    @Override
    public void close() throws IOException
    {
        file.close();
    }


    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + file + "]";
    }

    static <Buffer extends BigEndianByteArrayBuffer> void scanAll( PagedFile file, int startOffset,
            EntryVisitor<? super Buffer> visitor, Buffer key, Buffer value ) throws IOException
    {
        boolean visitHeaders = !(visitor instanceof KeyValueVisitor);
        try ( PageCursor cursor = file.io( startOffset / file.pageSize(), PF_NO_GROW | PF_SHARED_LOCK ) )
        {
            if ( !cursor.next() )
            {
                return;
            }
            readKeyValuePair( cursor, startOffset, key, value );
            visitKeyValuePairs( file.pageSize(), cursor, startOffset, visitor, visitHeaders, key, value );
        }
    }

    /** Expects the first key/value-pair to be read into the buffers already, reads subsequent pairs (if requested). */
    private static <Buffer extends BigEndianByteArrayBuffer> void visitKeyValuePairs( int pageSize, PageCursor cursor,
            int offset, EntryVisitor<? super Buffer> visitor, boolean visitHeaders, Buffer key, Buffer value )
            throws IOException
    {
        while ( visitable( key, visitHeaders ) && visitor.visit( key, value ) )
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
        return acceptZeroKey || !key.allZeroes();
    }

    private static void readKeyValuePair( PageCursor cursor, int offset, WritableBuffer key, WritableBuffer value )
            throws IOException
    {
        do
        {
            cursor.setOffset( offset );
            key.getFrom( cursor );
            value.getFrom( cursor );
        }
        while ( cursor.shouldRetry() );
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
     * @param cursor the cursor for the page to search for the key in.
     * @param searchKey the key to search for.
     * @param key a buffer to write the key into.
     * @param value a buffer to write the value into.
     * @return the offset (in bytes within the given page) of the first entry with a key that is greater than or equal
     * to the given key.
     */
    private int findByteOffset( PageCursor cursor, BigEndianByteArrayBuffer searchKey, BigEndianByteArrayBuffer key,
            BigEndianByteArrayBuffer value ) throws IOException
    {
        int entrySize = searchKey.size() + value.size(), last = maxPage( file.pageSize(), entrySize, totalEntries );
        int firstEntry = (cursor.getCurrentPageId() == 0) ? headerEntries : 0; // skip header in first page
        int entryCount = totalEntries % (file.pageSize() / entrySize);
        // If the last page is full, 'entryCount' will be 0 at this point.
        if ( cursor.getCurrentPageId() != last || entryCount == 0 )
        { // The current page is a full page (either because it has pages after it, or the last page is actually full).
            entryCount = file.pageSize() / entrySize;
        }
        int entryOffset = findEntryOffset( cursor, searchKey, key, value, firstEntry, /*lastEntry=*/entryCount - 1 );
        return entryOffset * entrySize;
    }

    static int maxPage( int pageSize, int entrySize, int totalEntries )
    {
        int maxPage = totalEntries / (pageSize / entrySize);
        return maxPage * (pageSize / entrySize) == totalEntries ? maxPage - 1 : maxPage;
    }

    /**
     * @param cursor the cursor for the page to search for the key in.
     * @param searchKey the key to search for.
     * @param key a buffer to write the key into.
     * @param value a buffer to write the value into.
     * @param min the offset (in number of entries within the page) of the first entry in the page.
     * @param max the offset (in number of entries within the page) of the last entry in the page.
     * @return the offset (in number of entries within the page) of the first entry with a key that is greater than or
     * equal to the given key.
     */
    static int findEntryOffset( PageCursor cursor, BigEndianByteArrayBuffer searchKey, BigEndianByteArrayBuffer key,
            BigEndianByteArrayBuffer value, int min, int max ) throws IOException
    {
        int entrySize = key.size() + value.size();
        for ( int mid; min <= max; )
        {
            mid = min + (max - min) / 2;
            readKeyValuePair( cursor, mid * entrySize, key, value );
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
