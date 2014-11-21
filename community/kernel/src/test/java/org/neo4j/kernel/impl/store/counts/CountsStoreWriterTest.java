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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.counts.CountsStore.RECORD_SIZE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexCountsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.BASE_MINOR_VERSION;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.with;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.register.Register.DoubleLongRegister;

public class CountsStoreWriterTest
{
    @Test
    public void shouldWriteEntriesToTheCountsStore() throws IOException
    {
        // given
        final CountsStoreWriter writer = new CountsStoreWriter( fs, pageCache, emptyHeader, file, lastTxId );

        // when
        DoubleLongRegister valueRegister = Registers.newDoubleLongRegister();
        valueRegister.write( 0, 42 );
        writer.visit( nodeKey( 0 ), valueRegister );
        valueRegister.write( 0, 24 );
        writer.visit( relationshipKey( 1, 2, 3 ), valueRegister );
        valueRegister.write( 9, 11 );
        writer.visit( indexCountsKey( 4, 5 ), valueRegister );
        valueRegister.write( 24, 84 );
        writer.visit( indexSampleKey( 4, 5 ), valueRegister );
        writer.close();

        // then
        try
        {
            final SortedKeyValueStore counts = writer.openForReading();

            assertEquals( lastTxId, counts.lastTxId() );
            assertEquals( 4, counts.totalRecordsStored() );
            assertEquals( file, counts.file() );
            counts.accept( new KeyValueRecordVisitor<CountsKey, DoubleLongRegister>()
            {
                @Override
                public void visit( CountsKey key, DoubleLongRegister valueRegister )
                {
                    key.accept( new CountsVisitor()
                    {
                        @Override
                        public void visitNodeCount( int labelId, long count )
                        {
                            assertEquals( 0, labelId );
                            assertEquals( 42, count );
                        }

                        @Override
                        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
                        {
                            assertEquals( 1, startLabelId );
                            assertEquals( 2, typeId );
                            assertEquals( 3, endLabelId );
                            assertEquals( 24, count );
                        }

                        @Override
                        public void visitIndexCounts( int labelId, int propertyKeyId, long updates, long size )
                        {
                            assertEquals( 4, labelId );
                            assertEquals( 5, propertyKeyId );
                            assertEquals( 9, updates );
                            assertEquals( 11, size );
                        }


                        @Override
                        public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
                        {
                            assertEquals( 4, labelId );
                            assertEquals( 5, propertyKeyId );
                            assertEquals( 24, unique );
                            assertEquals( 84, size );
                        }
                    }, valueRegister.readFirst(), valueRegister.readSecond() );
                }
            }, Registers.newDoubleLongRegister() );

        }
        finally
        {
            pageCache.unmap( file );
        }
    }


    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private final SortedKeyValueStoreHeader emptyHeader =
            with( RECORD_SIZE, ALL_STORES_VERSION, BASE_TX_ID, BASE_MINOR_VERSION );
    private final File file = new File( "file" );
    private final long lastTxId = 100;
    private FileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }
}
