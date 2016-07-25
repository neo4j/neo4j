/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.countStore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotSerializer.serialize;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.EMPTY;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;

public class InMemoryCountsStoreCountsSnapshotSerializerTest
{
    InMemoryClosableChannel logChannel;
    CountsSnapshot countsSnapshot;
    ByteBuffer expectedBytes;
    ByteBuffer serializedBytes;

    @Before
    public void setup() throws IOException
    {
        logChannel = new InMemoryClosableChannel();
        countsSnapshot = new CountsSnapshot( 1, new ConcurrentHashMap<>() );
    }

    @After
    public void after() throws IOException
    {
        logChannel.close();
    }

    @Test
    public void correctlySerializesTxId() throws IOException
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( Long.BYTES );
        expectedBytes = ByteBuffer.allocate( Long.BYTES );
        writeExpectedTxID( expectedBytes, 1 );
        writeAndSerializeEntityNode( 1, 1 ); //Something needs to be written to test this.
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedBytes.limit() );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesTxIdAndMapSize() throws IOException
    {
        //GIVEN
        ByteBuffer serializedBytes = ByteBuffer.allocate( Long.BYTES + Integer.BYTES );
        ByteBuffer expectedBytes = ByteBuffer.allocate( Long.BYTES + Integer.BYTES );
        writeExpectedTxID( expectedBytes, 1 );
        writeExpectedCountStoreSize( expectedBytes, 1 );
        writeAndSerializeEntityNode( 1, 1 ); //Something needs to be written to test this.
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedBytes.limit() );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesEntityNode() throws IOException
    {
        //GIVEN
        int serializedLength = Long.BYTES + Integer.BYTES //Serialization Prefix
                + Byte.BYTES + Integer.BYTES + Long.BYTES; //A single ENTITY_NODE from count store.
        initializeBuffers( serializedLength );
        writeAndSerializeEntityNode( 1, 1 );
        expectedBytes.put( ENTITY_NODE.code );
        expectedBytes.putInt( 1 );
        expectedBytes.putLong( 1 );
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedLength );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesEntityRelationship() throws IOException
    {
        //GIVEN
        int serializedLength = Long.BYTES + Integer.BYTES //Serialization Prefix
                + Byte.BYTES + (3 * Integer.BYTES) + Long.BYTES; //A single ENTITY_RELATIONSHIP from count store.
        initializeBuffers( serializedLength );
        writeAndSerializeEntityRelationship( 1, 1, 1, 1 );
        expectedBytes.put( ENTITY_RELATIONSHIP.code );
        expectedBytes.putInt( 1 );
        expectedBytes.putInt( 1 );
        expectedBytes.putInt( 1 );
        expectedBytes.putLong( 1 );
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedLength );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesIndexSample() throws IOException
    {
        //GIVEN
        int serializedLength = Long.BYTES + Integer.BYTES //Serialization Prefix
                + Byte.BYTES + (2 * Integer.BYTES) + (2 * Long.BYTES); //A single INDEX_SAMPLE from count store.
        writeAndSerializeIndexSample( 1, 1, 1 );
        initializeBuffers( serializedLength );
        expectedBytes.put( INDEX_SAMPLE.code );
        expectedBytes.putInt( 1 );
        expectedBytes.putInt( 1 );
        expectedBytes.putLong( 1 );
        expectedBytes.putLong( 1 );
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedLength );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesIndexStatistics() throws IOException
    {
        //GIVEN
        int serializedLength = Long.BYTES + Integer.BYTES //Serialization Prefix
                + Byte.BYTES + (2 * Integer.BYTES) + (2 * Long.BYTES); //A single INDEX_STATISTICS from count store.
        writeAndSerializeIndexStatistics( 1, 1, 1 );
        initializeBuffers( serializedLength );
        expectedBytes.put( INDEX_STATISTICS.code );
        expectedBytes.putInt( 1 );
        expectedBytes.putInt( 1 );
        expectedBytes.putLong( 1 );
        expectedBytes.putLong( 1 );
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedLength );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test( expected = IllegalArgumentException.class )
    public void throwsExceptionOnWrongValueLengthForEntityNode() throws IOException
    {
        Map<CountsKey,long[]> brokenMap = new ConcurrentHashMap<>();
        brokenMap.put( nodeKey( 1 ), new long[]{1, 1} );
        CountsSnapshot brokenSnapshot = new CountsSnapshot( 1, brokenMap );
        serialize( logChannel, brokenSnapshot );
    }

    @Test( expected = IllegalArgumentException.class )
    public void throwsExceptionOnWrongValueLengthForEntityRelationship() throws IOException
    {
        Map<CountsKey,long[]> brokenMap = new ConcurrentHashMap<>();
        brokenMap.put( relationshipKey( 1, 1, 1 ), new long[]{1, 1} );
        CountsSnapshot brokenSnapshot = new CountsSnapshot( 1, brokenMap );
        serialize( logChannel, brokenSnapshot );
    }

    @Test( expected = IllegalArgumentException.class )
    public void throwsExceptionOnWrongValueLengthForIndexSample() throws IOException
    {
        Map<CountsKey,long[]> brokenMap = new ConcurrentHashMap<>();
        brokenMap.put( indexSampleKey( 1, 1 ), new long[]{1} );
        CountsSnapshot brokenSnapshot = new CountsSnapshot( 1, brokenMap );
        serialize( logChannel, brokenSnapshot );
    }

    @Test( expected = IllegalArgumentException.class )
    public void throwsExceptionOnWrongValueLengthForIndexStatistics() throws IOException
    {
        Map<CountsKey,long[]> brokenMap = new ConcurrentHashMap<>();
        brokenMap.put( indexStatisticsKey( 1, 1 ), new long[]{1} );
        CountsSnapshot brokenSnapshot = new CountsSnapshot( 1, brokenMap );
        serialize( logChannel, brokenSnapshot );
    }

    @Test
    public void onlyHandleExpectedRecordTypes()
    {
        Assert.assertArrayEquals( CountsKeyType.values(),
                new CountsKeyType[]{EMPTY, ENTITY_NODE, ENTITY_RELATIONSHIP, INDEX_STATISTICS, INDEX_SAMPLE} );
    }

    private void initializeBuffers( int serializedLength )
    {
        serializedBytes = ByteBuffer.allocate( serializedLength );
        expectedBytes = ByteBuffer.allocate( serializedLength );
        writeExpectedTxID( expectedBytes, 1 );
        writeExpectedCountStoreSize( expectedBytes, 1 );
    }

    private void writeExpectedTxID( ByteBuffer buffer, long txId )
    {
        buffer.putLong( txId );
    }

    private void writeExpectedCountStoreSize( ByteBuffer buffer, int size )
    {
        buffer.putInt( size );
    }

    private void writeAndSerializeEntityNode( int labelId, long count ) throws IOException
    {
        countsSnapshot.getMap().put( nodeKey( labelId ), new long[]{count} );
        serialize( logChannel, countsSnapshot );
    }

    private void writeAndSerializeEntityRelationship( int startId, int type, int endId, long count )
            throws IOException
    {
        countsSnapshot.getMap().put( relationshipKey( startId, type, endId ), new long[]{count} );
        serialize( logChannel, countsSnapshot );
    }

    private void writeAndSerializeIndexSample( int labelId, int propertyKeyId, long count )
            throws IOException
    {
        countsSnapshot.getMap()
                .put( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ), new long[]{count, count} );
        serialize( logChannel, countsSnapshot );
    }

    private void writeAndSerializeIndexStatistics( int labelId, int propertyKeyId, long count )
            throws IOException
    {
        countsSnapshot.getMap()
                .put( CountsKeyFactory.indexStatisticsKey( labelId, propertyKeyId ), new long[]{count, count} );
        serialize( logChannel, countsSnapshot );
    }
}
