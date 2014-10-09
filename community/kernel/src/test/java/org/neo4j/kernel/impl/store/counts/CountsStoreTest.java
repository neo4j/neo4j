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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

public class CountsStoreTest
{
    @Test
    public void shouldCreateAnEmptyStore() throws IOException
    {
        // when
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        try ( CountsStore<CountsKey, Register.Long.Out> counts =
                      CountsStore.open( fs, pageCache, alpha, RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            // then
            assertEquals( 0, get( counts, nodeKey( 0 ) ) );
            assertEquals( 0, get( counts, CountsKey.relationshipKey( 1, 2, 3 ) ) );
            assertEquals( BASE_TX_ID, counts.lastTxId() );
            assertEquals( 0, counts.totalRecordsStored() );
            assertEquals( alpha, counts.file() );
            counts.accept( new RecordVisitor<CountsKey>()
            {
                @Override
                public void visit( CountsKey key, long value )
                {
                    fail( "should not have been called" );
                }
            } );
        }
    }

    @Test
    public void shouldUpdateTheStore() throws IOException
    {
        // given
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        CountsStore.Writer<CountsKey, Register.Long.Out> writer;
        try ( CountsStore<CountsKey, Register.Long.Out> counts =
                      CountsStore.open( fs, pageCache, alpha, RECORD_SERIALIZER, WRITER_FACTORY ) )
        {
            // when
            writer = counts.newWriter( beta, lastCommittedTxId );
            writer.visit( nodeKey( 0 ), 21 );
            writer.visit( CountsKey.relationshipKey( 1, 2, 3 ), 32 );
            writer.close();
        }

        try ( CountsStore<CountsKey, Register.Long.Out> updated = writer.openForReading() )
        {
            // then
            assertEquals( 21, get( updated, nodeKey( 0 ) ) );
            assertEquals( 32, get( updated, CountsKey.relationshipKey( 1, 2, 3 ) ) );
            assertEquals( lastCommittedTxId, updated.lastTxId() );
            assertEquals( 2, updated.totalRecordsStored() );
            assertEquals( beta, updated.file() );
            updated.accept( new RecordVisitor<CountsKey>()
            {
                @Override
                public void visit( CountsKey key, long value )
                {
                    key.accept( new CountsVisitor()
                    {
                        @Override
                        public void visitNodeCount( int labelId, long count )
                        {
                            assertEquals( 0, labelId );
                            assertEquals( 21, count );
                        }

                        @Override
                        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
                        {
                            assertEquals( 1, startLabelId );
                            assertEquals( 2, typeId );
                            assertEquals( 3, endLabelId );
                            assertEquals( 32, count );
                        }
                    }, value );
                }
            } );
        }
    }

    private static final CountsRecordSerializer RECORD_SERIALIZER = new CountsRecordSerializer();
    private static final CountsStoreWriter.Factory WRITER_FACTORY = new CountsStoreWriter.Factory();

    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    private final File alpha = new File( "alpha" );
    private final File beta = new File( "beta" );
    private final int lastCommittedTxId = 42;
    private FileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs, new Config() );
    }

    private <K extends Comparable<K>> long get( CountsStore<K, Register.Long.Out> store, K key )
    {
        Register.LongRegister value = Registers.newLongRegister();
        store.get( key, value );
        return value.read();
    }
}
