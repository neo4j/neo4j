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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.BASE_MINOR_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class CountsStoreTest
{
    @Test
    public void shouldCreateAnEmptyStore() throws IOException
    {
        // when
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // then
            assertEquals( 0, get( counts, nodeKey( 0 ) ) );
            assertEquals( 0, get( counts, relationshipKey( 1, 2, 3 ) ) );
            assertEquals( BASE_TX_ID, counts.lastTxId() );
            assertEquals( BASE_MINOR_VERSION, counts.minorVersion() );
            assertEquals( 0, counts.totalRecordsStored() );
            assertEquals( alpha, counts.file() );
            counts.accept( new KeyValueRecordVisitor<CountsKey, Register.LongRegister>()
            {
                private final Register.LongRegister valueRegister = Registers.newLongRegister();

                @Override
                public Register.LongRegister valueRegister()
                {
                    return valueRegister;
                }

                @Override
                public void visit( CountsKey key )
                {
                    fail( "should not have been called" );
                }
            } );
        }
    }

    @Test
    public void shouldBumpMinorVersion() throws IOException
    {
        // when
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // when
            long initialMinorVersion = counts.minorVersion();

            SortedKeyValueStore.Writer<CountsKey, Register.LongRegister> writer = counts.newWriter( beta, counts.lastTxId() );
            writer.close();

            try ( CountsStore updated = (CountsStore) writer.openForReading() )
            {
                assertEquals( initialMinorVersion + 1l, updated.minorVersion() );
            }
        }
    }

    @Test
    public void shouldUpdateTheStore() throws IOException
    {
        // given
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        SortedKeyValueStore.Writer<CountsKey, Register.LongRegister> writer;
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // when
            writer = counts.newWriter( beta, lastCommittedTxId );
            writer.valueRegister().write( 21 );
            writer.visit( nodeKey( 0 ) );
            writer.valueRegister().write( 32 );
            writer.visit( relationshipKey( 1, 2, 3 )  );
            writer.close();
        }

        try ( CountsStore updated = (CountsStore) writer.openForReading() )
        {
            // then
            assertEquals( 21, get( updated, nodeKey( 0 ) ) );
            assertEquals( 32, get( updated, relationshipKey( 1, 2, 3 ) ) );
            assertEquals( lastCommittedTxId, updated.lastTxId() );
            assertEquals( BASE_MINOR_VERSION, updated.minorVersion() );
            assertEquals( 2, updated.totalRecordsStored() );
            assertEquals( beta, updated.file() );
            updated.accept( new KeyValueRecordVisitor<CountsKey, Register.LongRegister>()
            {
                private final Register.LongRegister valueRegister = Registers.newLongRegister();

                @Override
                public Register.LongRegister valueRegister()
                {
                    return valueRegister;
                }

                @Override
                public void visit( CountsKey key )
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

                        @Override
                        public void visitIndexCount( int labelId, int propertyKeyId, long count )
                        {
                            // nothing to check here
                        }
                    }, valueRegister );
                }
            } );
        }
    }

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

    private long get( CountsStore store, CountsKey key )
    {
        Register.LongRegister value = Registers.newLongRegister();
        store.get( key, value );
        return value.read();
    }
}
