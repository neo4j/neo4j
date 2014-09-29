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
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

class CountsStore implements Closeable
{
    interface Writer extends RecordVisitor, Closeable
    {
        CountsStore openForReading() throws IOException;
    }

    static final int RECORD_SIZE = (3 * 4) + 8/*bytes*/;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final File file;
    private final PagedFile pages;
    private final CountsStoreHeader header;
    private final int totalRecords;

    private CountsStore( FileSystemAbstraction fs, PageCache pageCache, File file, PagedFile pages,
                         CountsStoreHeader header )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.file = file;
        this.pages = pages;
        this.header = header;
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
                unMapCountsStore( pageCache, storeFile, pages );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    static CountsStore open( FileSystemAbstraction fs, PageCache pageCache, File storeFile ) throws IOException
    {
        PagedFile pages = mapCountsStore( pageCache, storeFile );
        CountsStoreHeader header = CountsStoreHeader.read( pages );
        return new CountsStore( fs, pageCache, storeFile, pages, header );
    }

    private static PagedFile mapCountsStore( PageCache pageCache, File storeFile ) throws IOException
    {
        int pageSize = pageCache.pageSize() - (pageCache.pageSize() % RECORD_SIZE);
        return pageCache.map( storeFile, pageSize );
    }

    private static void unMapCountsStore( PageCache pageCache, File storeFile, PagedFile pages ) throws IOException
    {
        pages.flush();
        pageCache.unmap( storeFile );
    }

    public long get( CountsKey key )
    {
        Register.LongRegister value = Registers.newLongRegister();
        int min = header.headerRecords(), mid, max = min + totalRecords;
        try ( PageCursor cursor = pages.io( 0, PF_SHARED_LOCK ) )
        {
            while ( min <= max )
            {
                mid = min + (max - min) / 2;
                int cmp = compareKeyAndReadValue( cursor, key, mid, value );
                if ( cmp == 0 )
                {
                    return value.read();
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
        return 0;
    }

    private int compareKeyAndReadValue( PageCursor cursor, CountsKey target, int record, Register.Long.Out value )
            throws IOException
    {
        int pageId = (record * RECORD_SIZE) / pages.pageSize();
        int offset = (record * RECORD_SIZE) % pages.pageSize();
        if ( pageId == cursor.getCurrentPageId() || cursor.next( pageId ) )
        {
            CountsKey key;
            do
            {
                cursor.setOffset( offset );
                int startLabelId = cursor.getInt();
                int typeId = cursor.getInt();
                int endLabelId = cursor.getInt();
                long count = cursor.getLong();
                if ( count < 0 )
                {
                    value.write( -count );
                    key = nodeKey( typeId );
                }
                else
                {
                    value.write( count );
                    key = relationshipKey( startLabelId, typeId, endLabelId );
                }
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

    public void accept( RecordVisitor visitor )
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
            for ( int read, offset = 0; (read = in.read( record, offset, record.length - offset )) != -1; )
            {
                if ( read != record.length )
                {
                    offset = read;
                    continue;
                }
                buffer.position( 0 );
                visitRecord( buffer, visitor );
                offset = 0;
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void visitRecord( ByteBuffer buffer, RecordVisitor visitor )
    {
        CountsKey key;
        long value;
        int startLabelId = buffer.getInt();
        int typeId = buffer.getInt();
        int endLabelId = buffer.getInt();
        long count = buffer.getLong();

        // if the count is zero then it is unused
        if ( count == 0 )
        {
            return;
        }

        if ( count < 0 )
        {
            value = -count;
            key = nodeKey( typeId );
        }
        else
        {
            value = count;
            key = relationshipKey( startLabelId, typeId, endLabelId );
        }
        visitor.visit( key, value );
    }

    public Writer newWriter( File targetFile, long lastCommittedTxId ) throws IOException
    {
        return new StoreWriter( targetFile, lastCommittedTxId );
    }

    public void close() throws IOException
    {
        pageCache.unmap( file );
    }

    private class StoreWriter implements Writer, CountsVisitor
    {
        private PageCursor page;
        private final PagedFile pagedFile;
        private final File targetFile;
        private final long txId;
        private int totalRecords;

        StoreWriter( File targetFile, long lastCommittedTxId ) throws IOException
        {
            this.targetFile = targetFile;
            this.txId = lastCommittedTxId;
            this.pagedFile = pageCache.map( targetFile, pages.pageSize() );
            if ( !(this.page = pagedFile.io( 0, PF_EXCLUSIVE_LOCK )).next() )
            {
                throw new IOException( "Could not acquire page." );
            }
            page.setOffset( header.headerRecords() * RECORD_SIZE );
        }

        @Override
        public void visit( CountsKey key, long value )
        {
            if ( value != 0 ) // only writeToBuffer values that count
            {
                totalRecords++;
                key.accept( this, value );
            }
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            assert count > 0 : String
                    .format( "visitNodeCount(labelId=%d, count=%d) - count must be positive", labelId, count );
            write( 0, labelId, 0, -count );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            assert count > 0 : String
                    .format( "visitRelationshipCount(startLabelId=%d, typeId=%d, endLabelId=%d, count=%d)" +
                             " - count must be positive", startLabelId, typeId, endLabelId, count );
            write( startLabelId, typeId, endLabelId, count );
        }

        private void write( int startLabelId, int typeId, int endLabelId, long count )
        {
            try
            {
                int offset = page.getOffset();
                if ( offset >= pages.pageSize() )
                { // we've reached the end of this page
                    page.next();
                    offset = 0;
                }
                do
                {
                    page.setOffset( offset );
                    page.putInt( startLabelId );
                    page.putInt( typeId );
                    page.putInt( endLabelId );
                    page.putLong( count );
                } while ( page.shouldRetry() );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        @Override
        public CountsStore openForReading() throws IOException
        {
            return new CountsStore( fs, pageCache, targetFile, pagedFile, newHeader() );
        }

        @Override
        public void close() throws IOException
        {
            if ( !page.next( 0 ) )
            {
                throw new IOException( "Could not update header." );
            }
            newHeader().write( page );
            page.close();
            page = null;
            pagedFile.flush();
        }

        private CountsStoreHeader newHeader()
        {
            return header.update( totalRecords, txId );
        }
    }
}
