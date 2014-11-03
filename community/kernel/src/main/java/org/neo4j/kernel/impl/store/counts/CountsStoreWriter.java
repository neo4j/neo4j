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
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.kernel.impl.store.counts.CountsKeyType.EMPTY;
import static org.neo4j.kernel.impl.store.counts.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.CountsKeyType.INDEX_COUNTS;
import static org.neo4j.kernel.impl.store.counts.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.CountsStore.RECORD_SIZE;

public class CountsStoreWriter implements SortedKeyValueStore.Writer<CountsKey, Register.DoubleLongRegister>, CountsVisitor
{
    public static class Factory implements SortedKeyValueStore.WriterFactory<CountsKey, Register.DoubleLongRegister>
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

    private final Register.DoubleLongRegister valueRegister = Registers.newDoubleLongRegister();
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
        if ( pageSize % RECORD_SIZE != 0 )
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
        page.setOffset( this.oldHeader.headerRecords() * RECORD_SIZE );
    }

    @Override
    public Register.DoubleLongRegister valueRegister()
    {
        return valueRegister;
    }

    @Override
    public void visit( CountsKey key )
    {
        // only writeToBuffer values that count
        if ( valueRegister.readFirst() != 0 || valueRegister.readSecond() != 0 )
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
        write( ENTITY_NODE, 0, 0, labelId, 0, count );
    }

    @Override
    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        assert count > 0 :
                String.format( "visitRelationshipCount(startLabelId=%d, typeId=%d, endLabelId=%d, count=%d)" +
                        " - count must be positive", startLabelId, typeId, endLabelId, count );
        write( ENTITY_RELATIONSHIP, startLabelId, typeId, endLabelId, 0, count );
    }

    @Override
    public void visitIndexCounts( int labelId, int propertyKeyId, long updates, long size )
    {
        assert updates >= 0 :
                String.format( "visitIndexSizeCount(labelId=%d, propertyKeyId=%d, updates=%d, size=%d)" +
                        " - updates must be positive or zero", labelId, propertyKeyId, updates, size );
        assert size >= 0 :
                String.format( "visitIndexSizeCount(labelId=%d, propertyKeyId=%d, updates=%d, size=%d)" +
                               " - size must be positive or zero", labelId, propertyKeyId, updates, size );
        write( INDEX_COUNTS, 0, propertyKeyId, labelId, updates, size );
    }

    @Override
    public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        assert unique >= 0 :
                String.format( "visitIndexSampleCount(labelId=%d, propertyKeyId=%d, unique=%d, size=%d)" +
                        " - unique must be zero or positive", labelId, propertyKeyId, unique, size );
        assert size >= 0 :
                String.format( "visitIndexSampleCount(labelId=%d, propertyKeyId=%d, unique=%d, size=%d)" +
                        " - size must be zero or positive", labelId, propertyKeyId, unique, size );
        assert unique <= size :
                String.format( "visitIndexSampleCount(labelId=%d, propertyKeyId=%d, unique=%d, size=%d)" +
                        " - unique must be less than or equal to size", labelId, propertyKeyId, unique, size );
        write( INDEX_SAMPLE, 0, propertyKeyId, labelId, unique, size );
    }


    // See CountsRecordSerializer for format
    private void write( CountsKeyType keyType, int firstId, int secondId, int thirdId, long first, long second )
    {
        try
        {
            int offset = page.getOffset();
            if ( offset >= pagedFile.pageSize() )
            { // we've reached the end of this page
                if ( !page.next() )
                {
                    throw new IOException( "Could not acquire next page." );
                }
                offset = 0;
            }
            do
            {
                page.setOffset( offset );

                page.putByte( keyType.code );
                page.putByte( (byte) 0 );
                page.putInt( firstId );
                page.putByte( (byte) 0 );
                page.putInt( secondId );
                page.putByte( (byte) 0 );
                page.putInt( thirdId );

                page.putLong( first );
                page.putLong( second );
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
        zeroPadding( pagedFile, page );
        if ( !page.next( 0 ) )
        {
            throw new IOException( "Could not update header." );
        }
        newHeader().write( page );
        page.close();
        page = null;
        pagedFile.flush();
    }

    private void zeroPadding( PagedFile pagedFile, PageCursor page ) throws IOException
    {
        final long lastPageId = pagedFile.getLastPageId();
        final int pageSize = pagedFile.pageSize();
        while ( page.getCurrentPageId() < lastPageId || page.getOffset() < pageSize )
        {
           write( EMPTY, 0, 0, 0, 0, 0 );
        }
    }

    private SortedKeyValueStoreHeader newHeader()
    {
        return oldHeader.update( totalRecords, txId, minorVersion );
    }
}
