/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.register.Register;

class CountsStore<K extends Comparable<K>, VR> implements Closeable
{
    interface Writer<K extends Comparable<K>, VR> extends RecordVisitor<K>, Closeable
    {
        CountsStore<K, VR> openForReading() throws IOException;
    }

    static final int RECORD_SIZE /*bytes*/ = 16 /*key*/ + 16 /*value*/;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final File file;
    private final PagedFile pages;
    private final CountsStoreHeader header;
    private final int totalRecords;
    private final RecordSerializer<K, VR> recordSerializer;

    CountsStore( FileSystemAbstraction fs, PageCache pageCache, File file, PagedFile pages, CountsStoreHeader header,
                 RecordSerializer<K, VR> recordSerializer )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.file = file;
        this.pages = pages;
        this.header = header;
        this.recordSerializer = recordSerializer;
        this.totalRecords = header.dataRecords();
    }

    @Override
    public String toString()
    {
        return String.format( "%s[file=%s,%s]", getClass().getSimpleName(), file, header );
    }

    static void createEmpty( PageCache pageCache, File storeFile, String version )
    {
        try
        {
            PagedFile pages = mapCountsStore( pageCache, storeFile );
            try
            {
                CountsStoreHeader.empty( version ).write( pages );
            }
            finally
            {
                pages.flush();
                pageCache.unmap( storeFile );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    static <K extends Comparable<K>, VR> CountsStore<K, VR> open( FileSystemAbstraction fs,
                                                                  PageCache pageCache,
                                                                  File storeFile,
                                                                  RecordSerializer<K, VR> recordSerializer )
            throws IOException
    {
        PagedFile pages = mapCountsStore( pageCache, storeFile );
        CountsStoreHeader header = CountsStoreHeader.read( pages );
        return new CountsStore<>( fs, pageCache, storeFile, pages, header, recordSerializer );
    }

    private static PagedFile mapCountsStore( PageCache pageCache, File storeFile ) throws IOException
    {
        int pageSize = pageCache.pageSize() - (pageCache.pageSize() % RECORD_SIZE);
        return pageCache.map( storeFile, pageSize );
    }

    public void get( K key, VR value )
    {
        int min = header.headerRecords();
        int max = min + totalRecords - 1;
        int mid;
        try ( PageCursor cursor = pages.io( 0, PF_SHARED_LOCK ) )
        {
            while ( min <= max )
            {
                mid = min + (max - min) / 2;
                int cmp = compareKeyAndReadValue( cursor, key, mid, value );
                if ( cmp == 0 )
                {
                    return;
                }
                else if ( cmp < 0 )
                {
                    max = mid - 1;
                }
                else
                {
                    min = mid + 1;
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        recordSerializer.writeDefaultValue( value );
    }

    private int compareKeyAndReadValue( PageCursor cursor, K target, int record, VR count )
            throws IOException
    {
        int pageId = (record * RECORD_SIZE) / pages.pageSize();
        int offset = (record * RECORD_SIZE) % pages.pageSize();
        if ( pageId == cursor.getCurrentPageId() || cursor.next( pageId ) )
        {
            K key;
            do
            {
                cursor.setOffset( offset );
                key = recordSerializer.readRecord( cursor, count );
            } while ( cursor.shouldRetry() );
            return target.compareTo( key );
        }
        else
        {
            throw new IllegalStateException( "Could not fetch page: " + pageId );
        }
    }

    public File file()
    {
        return file;
    }

    public long lastTxId()
    {
        return header.lastTxId();
    }
    public long totalRecordsStored()
    {
        return header.dataRecords();
    }

    public void accept( RecordVisitor<K> visitor )
    {
        try ( InputStream in = fs.openAsInputStream( file ) )
        {
            // skip the header
            for ( long bytes = header.headerRecords() * RECORD_SIZE; bytes != 0; )
            {
                bytes -= in.skip( bytes );
            }
            byte[] record = new byte[RECORD_SIZE];
            ByteBuffer buffer = ByteBuffer.wrap( record );
            boolean readNext = true;
            for ( int read, offset = 0; readNext && (read = in.read( record, offset, record.length - offset )) != -1; )
            {
                if ( read != record.length )
                {
                    offset = read;
                    continue;
                }
                buffer.position( 0 );
                readNext = recordSerializer.visitRecord( buffer, visitor );
                offset = 0;
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public Writer<CountsKey, Register.Long.Out> newWriter( File targetFile, long lastCommittedTxId ) throws IOException
    {
        return new CountsStoreWriter( fs, pageCache, header, targetFile, lastCommittedTxId );
    }

    public void close() throws IOException
    {
        pageCache.unmap( file );
    }

}
