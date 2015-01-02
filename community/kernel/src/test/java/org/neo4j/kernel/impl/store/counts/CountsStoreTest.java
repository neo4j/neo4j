/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.nio.ByteBuffer;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader;
import org.neo4j.register.Register.CopyableDoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.StoreFactory.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.store.counts.CountsStore.RECORD_SIZE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexCountsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.BASE_MINOR_VERSION;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.META_HEADER_SIZE;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.with;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.register.Register.DoubleLongRegister;

public class CountsStoreTest
{
    @Test
    public void shouldCreateAnEmptyStore() throws IOException
    {
        // when
        CountsStore.createEmpty( pageCache, alpha, header );
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // then
            assertEquals( 0, get( counts, nodeKey( 0 ) ) );
            assertEquals( 0, get( counts, relationshipKey( 1, 2, 3 ) ) );
            assertEquals( BASE_TX_ID, counts.lastTxId() );
            assertEquals( BASE_MINOR_VERSION, counts.minorVersion() );
            assertEquals( 0, counts.totalRecordsStored() );
            assertEquals( alpha, counts.file() );
            counts.accept( new KeyValueRecordVisitor<CountsKey, CopyableDoubleLongRegister>()
            {
                @Override
                public void visit( CountsKey key, CopyableDoubleLongRegister valueRegister )
                {
                    fail( "should not have been called" );
                }
            }, Registers.newDoubleLongRegister() );
        }
    }

    @Test
    public void shouldBumpMinorVersion() throws IOException
    {
        // when
        CountsStore.createEmpty( pageCache, alpha, header );
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // when
            long initialMinorVersion = counts.minorVersion();

            SortedKeyValueStore.Writer<CountsKey, CopyableDoubleLongRegister> writer =
                    counts.newWriter( beta, counts.lastTxId() );
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
        CountsStore.createEmpty( pageCache, alpha, header );
        SortedKeyValueStore.Writer<CountsKey, CopyableDoubleLongRegister> writer;
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // when
            DoubleLongRegister valueRegister = Registers.newDoubleLongRegister();
            writer = counts.newWriter( beta, lastCommittedTxId );
            valueRegister.write( 0, 21 );
            writer.visit( nodeKey( 0 ), valueRegister );
            valueRegister.write( 0, 32 );
            writer.visit( relationshipKey( 1, 2, 3 ), valueRegister );
            valueRegister.write( 9, 11 );
            writer.visit( indexCountsKey( 4, 5 ), valueRegister );
            valueRegister.write( 24, 84 );
            writer.visit( indexSampleKey( 4, 5 ), valueRegister );
            writer.close();
        }

        try ( CountsStore updated = (CountsStore) writer.openForReading() )
        {
            // then
            assertEquals( 21, get( updated, nodeKey( 0 ) ) );
            assertEquals( 32, get( updated, relationshipKey( 1, 2, 3 ) ) );
            assertEquals( lastCommittedTxId, updated.lastTxId() );
            assertEquals( BASE_MINOR_VERSION, updated.minorVersion() );
            assertEquals( 4, updated.totalRecordsStored() );
            assertEquals( beta, updated.file() );
            updated.accept( new KeyValueRecordVisitor<CountsKey, CopyableDoubleLongRegister>()
            {
                private final DoubleLongRegister target = Registers.newDoubleLongRegister();
                @Override
                public void visit( CountsKey key, CopyableDoubleLongRegister valueRegister )
                {
                    valueRegister.copyTo( target );
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
                    }, target.readFirst(), target.readSecond() );
                }
            }, Registers.newDoubleLongRegister() );
        }
    }

    @Test
    public void shouldThrowAnExceptionIfTheStoredEntriesDiffersFromTheDataRecordsSavedInTheHeader() throws IOException
    {
        // given an empty counts store with an header saying that there are 3 entries
        byte[] version = UTF8.encode( buildTypeDescriptorAndVersion( CountsTracker.STORE_DESCRIPTOR ) );
        int headerBytes = META_HEADER_SIZE + version.length;
        headerBytes += RECORD_SIZE - (headerBytes % RECORD_SIZE);
        short headerRecords = (short) (headerBytes / RECORD_SIZE);

        int headerSize = RECORD_SIZE * headerRecords;

        try ( StoreChannel channel = fs.open( alpha, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( headerSize );
            buffer.putShort( headerRecords );
            buffer.putShort( (short) version.length );
            buffer.putInt( 3 );
            buffer.putLong( 1 );
            buffer.putLong( 1 );
            buffer.put( version );
            for ( int i = headerSize; i > META_HEADER_SIZE + version.length; i-- )
            {
                buffer.put( (byte) 0 );
            }
            buffer.flip();
            channel.write( buffer );
            channel.force( false );
        }

        try
        {
            // when
            CountsStore.open( fs, pageCache, alpha );
            fail( "should have thrown" );
        }
        catch ( UnderlyingStorageException  ex )
        {
            // then
            assertEquals( "Counts store is corrupted", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowAnExceptionIfTheStoreContainsZeroValues() throws IOException
    {
        // given an empty counts store with an header saying that there are 3 entries
        byte[] version = UTF8.encode( buildTypeDescriptorAndVersion( CountsTracker.STORE_DESCRIPTOR ) );
        int headerBytes = META_HEADER_SIZE + version.length;
        headerBytes += RECORD_SIZE - (headerBytes % RECORD_SIZE);
        short headerRecords = (short) (headerBytes / RECORD_SIZE);

        int headerSize = RECORD_SIZE * headerRecords;

        try ( StoreChannel channel = fs.open( alpha, "rw" ) )
        {
            // header
            ByteBuffer buffer = ByteBuffer.allocate( headerSize + RECORD_SIZE );
            buffer.putShort( headerRecords );
            buffer.putShort( (short) version.length );
            buffer.putInt( 1 );
            buffer.putLong( 1 );
            buffer.putLong( 1 );
            buffer.put( version );
            for ( int i = headerSize; i > META_HEADER_SIZE + version.length; i-- )
            {
                buffer.put( (byte) 0 );
            }

            // entry
            buffer.put( ENTITY_NODE.code ); // type

            buffer.put( (byte) 0 ); // node key
            buffer.putInt( 0 );
            buffer.put( (byte) 0 );
            buffer.putInt( 0 );
            buffer.put( (byte) 0 );
            buffer.putInt( 1 );

            buffer.putLong( 0 ); // value
            buffer.putLong( 0 );

            buffer.flip();
            channel.write( buffer );
            channel.force( false );
        }

        try
        {
            // when
            CountsStore.open( fs, pageCache, alpha );
            fail( "should have thrown" );
        }
        catch ( UnderlyingStorageException  ex )
        {
            // then
            assertEquals( "Counts store contains unexpected value (0,0)", ex.getMessage() );
        }
    }

    @Test
    public void shouldCloseFileIfOpenFailsWithIOExceptionOnHeaderFormat() throws Exception
    {
        // given
           // A header with invalid data that will lead to IOException being thrown
        PageCache myCache = spy( pageCache );
        try ( StoreChannel channel = fs.open( alpha, "rw" ) )
        {
            // header
            ByteBuffer buffer = ByteBuffer.allocate( 4 );
            buffer.putInt( 0xA );
            buffer.flip();
            channel.write( buffer );
            channel.force( false );
        }

        // when
           // opening the file fails while verifying contents
        try
        {
            CountsStore.open( fs, myCache, alpha );
            fail("Test setup error, this should have thrown an IOException");
        }
        catch( IOException expected )
        {
            // totally expected, continue
        }

        // then
           // ensure that whatever was opened is unmapped before returning
        fs.assertNoOpenFiles();
    }

    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    private final File alpha = new File( "a" );
    private final File beta = new File( "b" );
    private final int lastCommittedTxId = 42;
    private EphemeralFileSystemAbstraction fs;
    private PageCache pageCache;
    private final SortedKeyValueStoreHeader header =
            with( RECORD_SIZE, ALL_STORES_VERSION, BASE_TX_ID, BASE_MINOR_VERSION );

    @Before
    public void setup()
    {
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    private long get( CountsStore store, CountsKey key )
    {
        DoubleLongRegister value = Registers.newDoubleLongRegister();
        store.get( key, value );
        return value.readSecond();
    }
}
