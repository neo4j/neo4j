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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.BASE_MINOR_VERSION;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.with;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class SortedKeyValueStoreHeaderTest
{
    @Test
    public void shouldCreateAnEmptyHeader()
    {
        // when
        SortedKeyValueStoreHeader header = with( RECORD_SIZE, ALL_STORES_VERSION, BASE_TX_ID, BASE_MINOR_VERSION );

        // then
        assertEquals( BASE_TX_ID, header.lastTxId() );
        assertEquals( 0, header.dataRecords() );
        assertEquals( 1, header.headerRecords() );
        assertEquals( ALL_STORES_VERSION, header.storeFormatVersion() );
    }

    @Test
    public void shouldUpdateHeader()
    {
        // given
        SortedKeyValueStoreHeader header = with( RECORD_SIZE, ALL_STORES_VERSION, BASE_TX_ID, BASE_MINOR_VERSION );

        // when
        SortedKeyValueStoreHeader newHeader = header.update( 42, 24, 12 );

        // then
        assertEquals( 24, newHeader.lastTxId() );
        assertEquals( 12, newHeader.minorVersion() );
        assertEquals( 42, newHeader.dataRecords() );
        assertEquals( 1, newHeader.headerRecords() );
        assertEquals( ALL_STORES_VERSION, newHeader.storeFormatVersion() );
    }

    @Test
    public void shouldWriteHeaderInPageFile() throws IOException
    {
        // given
        SortedKeyValueStoreHeader header =
                with( RECORD_SIZE, ALL_STORES_VERSION, BASE_TX_ID, BASE_MINOR_VERSION ).update( 42, 24, 12 );

        // when
        try ( PagedFile pagedFile = pageCache.map( file, pageCache.pageSize() ) )
        {
            header.write( pagedFile );
            pagedFile.flush();

            // then
            assertEquals( header, SortedKeyValueStoreHeader.read( RECORD_SIZE, pagedFile ) );
        }
    }

    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private File file = new File( "file" );
    private PageCache pageCache;
    private static final int RECORD_SIZE = 32;

    @Before
    public void setup()
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }
}
