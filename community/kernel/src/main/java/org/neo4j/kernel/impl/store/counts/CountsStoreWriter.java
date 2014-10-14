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

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.INDEX;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.NODE;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.RELATIONSHIP;

public class CountsStoreWriter implements SortedKeyValueStore.Writer<CountsKey, Register.LongRegister>, CountsVisitor
{
    public static class Factory implements SortedKeyValueStore.WriterFactory<CountsKey, Register.LongRegister>
    {
        @Override
        public CountsStoreWriter create( FileSystemAbstraction fs, PageCache pageCache,
                                         SortedKeyValueStoreHeader header, File targetFile,
                                         long lastCommittedTxId )
                throws IOException
        {
            return new CountsStoreWriter( fs, pageCache, header, targetFile, lastCommittedTxId );
        }
    }

    private final Register.LongRegister valueRegister = Registers.newLongRegister();
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final SortedKeyValueStoreHeader oldHeader;
    private final PagedFile pagedFile;
    private final File targetFile;
    private final long txId;
    private final long minorVersion;

    private int totalRecords;
    private PageCursor page;

    CountsStoreWriter( FileSystemAbstraction fs, PageCache pageCache, SortedKeyValueStoreHeader oldHeader,
                       File targetFile, long lastCommittedTxId ) throws IOException
    {
        this.fs = fs;
        this.pageCache = pageCache;
        int pageSize = pageCache.pageSize();
        if ( pageSize % SortedKeyValueStore.RECORD_SIZE != 0 )
        {
            throw new IllegalStateException( "page size must a multiple of the record size" );
        }
        this.pagedFile = pageCache.map( targetFile, pageSize );
        this.oldHeader = oldHeader;
        this.targetFile = targetFile;
        this.txId = lastCommittedTxId;
        this.minorVersion = txId == oldHeader.lastTxId() ? oldHeader.minorVersion() + 1 : SortedKeyValueStoreHeader.BASE_MINOR_VERSION;
        if ( !(this.page = pagedFile.io( 0, PF_EXCLUSIVE_LOCK )).next() )
        {
            throw new IOException( "Could not acquire page." );
        }
        page.setOffset( this.oldHeader.headerRecords() * SortedKeyValueStore.RECORD_SIZE );
    }

    @Override
    public Register.LongRegister valueRegister()
    {
        return valueRegister;
    }

    @Override
    public void visit( CountsKey key )
    {
        if ( valueRegister.read() != 0 /* only writeToBuffer values that count */ )
        {
            totalRecords++;
            key.accept( this, valueRegister );
        }
    }

    @Override
    public void visitNodeCount( int labelId, long count )
    {
        assert count > 0 :
                String.format( "visitNodeCount(labelId=%d, count=%d) - count must be positive", labelId, count );
        write( NODE, 0, 0, labelId, count );
    }

    @Override
    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        assert count > 0 :
                String.format( "visitRelationshipCount(startLabelId=%d, typeId=%d, endLabelId=%d, count=%d)" +
                        " - count must be positive", startLabelId, typeId, endLabelId, count );
        write( RELATIONSHIP, startLabelId, typeId, endLabelId, count );
    }

    @Override
    public void visitIndexCount( int labelId, int propertyKeyId, long count )
    {
        assert count > 0 :
                String.format( "visitIndexCount(labelId=%d, propertyKeyId=%d, count=%d)" +
                               " - count must be positive", labelId, propertyKeyId, count );
        write( INDEX, 0, propertyKeyId, labelId, count );
    }

    /**
     * Node Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, , , , , , , , , , , ,x,x,x,x]
     *  _                       _ _ _ _
     *  |                          |
     *  entry                      label
     *  type                        id
     * <p/>
     *
     * Relationship Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, ,x,x,x,x, ,x,x,x,x, ,x,x,x,x]
     *  _   _ _ _ _   _ _ _ _   _ _ _ _
     *  |      |         |         |
     *  entry  label      rel      label
     *  type    id        type      id
     *  id
     * <p/>
     *
     * Index Key:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [x, , , , , , ,x,x,x,x, ,x,x,x,x]
     *  _             _ _ _ _   _ _ _ _
     *  |                |         |
     *  entry       property key   label
     *  type             id        id
     *  id
     *
     * Count value:
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
     * [ , , , , , , , ,x,x,x,x,x,x,x,x]
     * _ _ _ _ _ _ _ _
     * |
     * value
     */
    private void write( CountsRecordType keyType, int startLabelId, int relTypeId, int endLabelId, long count )
    {
        try
        {
            int offset = page.getOffset();
            if ( offset >= pagedFile.pageSize() )
            { // we've reached the end of this page
                page.next();
                offset = 0;
            }
            do
            {
                page.setOffset( offset );

                page.putByte( keyType.code );
                page.putByte( (byte) 0 );
                page.putInt( startLabelId );
                page.putByte( (byte) 0 );
                page.putInt( relTypeId );
                page.putByte( (byte) 0 );
                page.putInt( endLabelId );

                page.putLong( 0 );
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

    private SortedKeyValueStoreHeader newHeader()
    {
        return oldHeader.update( totalRecords, txId, minorVersion );
    }
}
