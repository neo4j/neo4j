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
import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

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
import org.neo4j.register.Registers;

class CountsStore implements Closeable
{
    interface Writer extends RecordVisitor, Closeable
    {
        CountsStore openForReading() throws IOException;
    }

    static final byte EMPTY_RECORD_KEY = 0;
    static final byte NODE_KEY = 1;
    static final byte RELATIONSHIP_KEY = 2;

    static final int RECORD_SIZE /*bytes*/ = 16 /*key*/ + 16 /*value*/;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final File file;
    private final PagedFile pages;
    private final CountsStoreHeader header;
    private final int totalRecords;

    CountsStore( FileSystemAbstraction fs, PageCache pageCache, File file, PagedFile pages, CountsStoreHeader header )
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
                pages.flush();
                pageCache.unmap( storeFile );
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

    public long get( CountsKey key )
    {
        Register.LongRegister value = Registers.newLongRegister();
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

    private int compareKeyAndReadValue( PageCursor cursor, CountsKey target, int record, Register.Long.Out count )
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
                key = readRecord( cursor, count );
            } while ( cursor.shouldRetry() );
            return target.compareTo( key );
        }
        else
        {
            throw new IllegalStateException( "Could not fetch page: " + pageId );
        }
    }

    /**
     * Node Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, , , , , , , , , , , ,x,x,x,x]
     *  _                       _ _ _ _
     *  |                          |
     * entry                      label
     * type                        id
     *
     * Relationship Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, ,x,x,x,x, ,x,x,x,x, ,x,x,x,x]
     *  _   _ _ _ _   _ _ _ _   _ _ _ _
     *  |      |         |         |
     * entry  label      rel      label
     * type    id        type      id
     *
     * Count value:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [ , , , , , , , ,x,x,x,x,x,x,x,x]
     *                  _ _ _ _ _ _ _ _
     *                         |
     *                       value
     */
    private CountsKey readRecord( PageCursor cursor, Register.Long.Out value )
    {
        // read type
        byte type = cursor.getByte();

        // read key
        cursor.getByte(); // skip unused byte
        int one = cursor.getInt();
        cursor.getByte(); // skip unused byte
        int two = cursor.getInt();
        cursor.getByte(); // skip unused byte
        int three = cursor.getInt();

        // read value
        cursor.getLong(); // skip unused long
        long count =  cursor.getLong();

        CountsKey key;
        switch ( type )
        {
            case EMPTY_RECORD_KEY:
                throw new IllegalStateException( "Reading empty record" );
            case NODE_KEY:
                assert one == 0;
                assert two == 0;
                key = nodeKey( three /* label id*/ );
                value.write( count );
                break;
            case RELATIONSHIP_KEY:
                key = relationshipKey( one /* start label id */, two /* rel type id */, three /* end label id */ );
                value.write( count );
                break;
            default:
                throw new IllegalStateException( "Unknown counts key type: " + type );
        }
        return key;
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
            boolean readNext = true;
            for ( int read, offset = 0; readNext && (read = in.read( record, offset, record.length - offset )) != -1; )
            {
                if ( read != record.length )
                {
                    offset = read;
                    continue;
                }
                buffer.position( 0 );
                readNext = visitRecord( buffer, visitor );
                offset = 0;
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Node Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, , , , , , , , , , , ,x,x,x,x]
     *  _                       _ _ _ _
     *  |                          |
     * entry                      label
     * type                        id
     *
     * Relationship Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, ,x,x,x,x, ,x,x,x,x, ,x,x,x,x]
     *  _   _ _ _ _   _ _ _ _   _ _ _ _
     *  |      |         |         |
     * entry  label      rel      label
     * type    id        type      id
     *
     * Count value:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [ , , , , , , , ,x,x,x,x,x,x,x,x]
     *                  _ _ _ _ _ _ _ _
     *                         |
     *                       value
     */
    private boolean visitRecord( ByteBuffer buffer, RecordVisitor visitor )
    {
        // read type
        byte type = buffer.get();

        // read key
        buffer.get(); // skip unused byte
        int one = buffer.getInt();
        buffer.get(); // skip unused byte
        int two = buffer.getInt();
        buffer.get(); // skip unused byte
        int three = buffer.getInt();

        // read value
        buffer.getLong(); // skip unused long
        long count =  buffer.getLong();

        CountsKey key;
        switch ( type )
        {
            case EMPTY_RECORD_KEY:
                assert one == 0;
                assert two == 0;
                assert three == 0;
                assert count == 0;
                return false;

            case NODE_KEY:
                assert one == 0;
                assert two == 0;
                key = nodeKey( three /* label id*/ );
                break;

            case RELATIONSHIP_KEY:
                key = relationshipKey( one /* start label id */, two /* rel type id */, three /* end label id */ );
                break;

            default:
                throw new IllegalStateException( "Unknown counts key type: " + type );
        }
        visitor.visit( key, count );
        return true;
    }

    public Writer newWriter( File targetFile, long lastCommittedTxId ) throws IOException
    {
        return new CountsStoreWriter( fs, pageCache, header, targetFile, lastCommittedTxId );
    }

    public void close() throws IOException
    {
        pageCache.unmap( file );
    }

}
