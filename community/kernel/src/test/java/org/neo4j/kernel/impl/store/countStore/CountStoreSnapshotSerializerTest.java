/*
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
package org.neo4j.kernel.impl.store.countStore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;

import static org.neo4j.kernel.impl.store.countStore.Snapshot.serialize;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;

public class CountStoreSnapshotSerializerTest
{
    InMemoryLogChannel logChannel;
    Snapshot snapshot;
    ByteBuffer expectedBytes;
    ByteBuffer serializedBytes;

    @Before
    public void setup() throws IOException, UnknownKey
    {
        logChannel = new InMemoryLogChannel();
        snapshot = new Snapshot( 1, new ConcurrentHashMap<>() );
    }

    @After
    public void after() throws IOException
    {
        logChannel.close();
    }

    @Test
    public void correctlySerializesTxId() throws IOException, UnknownKey
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( Long.BYTES );
        expectedBytes = ByteBuffer.allocate( Long.BYTES );
        writeExpectedTxID( expectedBytes, 1 );
        writeAndSerializeEntityNode( 1, 1 ); //Something needs to be writing to test this.
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedBytes.limit() );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesTxIdAndMapSize() throws IOException, UnknownKey
    {
        //GIVEN
        ByteBuffer serializedBytes = ByteBuffer.allocate( Long.BYTES + Integer.BYTES );
        ByteBuffer expectedBytes = ByteBuffer.allocate( Long.BYTES + Integer.BYTES );
        writeExpectedTxID( expectedBytes, 1 );
        writeExpectedCountStoreSize( expectedBytes, 1 );
        writeAndSerializeEntityNode( 1, 1 ); //Something needs to be writing to test this.
        expectedBytes.position( 0 );

        //WHEN
        logChannel.get( serializedBytes.array(), serializedBytes.limit() );

        //THEN
        Assert.assertEquals( expectedBytes, serializedBytes );
    }

    @Test
    public void correctlySerializesEntityNode() throws IOException, UnknownKey
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
    public void correctlySerializesEntityRelationship() throws IOException, UnknownKey
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
    public void correctlySerializesIndexSample() throws IOException, UnknownKey
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
    public void correctlySerializesIndexStatistics() throws IOException, UnknownKey
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

    @Test
    public void onlyHandleExpectedRecordTypes()
    {
        Assert.assertArrayEquals( CountsKeyType.values(),
                new CountsKeyType[]{ENTITY_NODE, ENTITY_RELATIONSHIP, INDEX_STATISTICS, INDEX_SAMPLE} );
    }

    @Test
    public void correctlyDeserializeTxId() throws IOException, UnknownKey
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryLogChannel logChannel = new InMemoryLogChannel( serializedBytes.array() );
        logChannel.putLong( 72 );
        logChannel.putInt( 0 );

        //WHEN
        Snapshot snapshot = Snapshot.deserialize( logChannel );

        //THEN
        Assert.assertEquals( 72, snapshot.getTxId() );
    }

    @Test
    public void correctlyDeserializeTxIdAndMapSize() throws IOException, UnknownKey
    {
        //GIVEN
        CountStore countStore = new CountStore();
        Map<CountsKey,long[]> updates = new HashMap<>();
        updates.put( CountsKeyFactory.nodeKey( 1 ), new long[]{1} );
        updates.put( CountsKeyFactory.nodeKey( 2 ), new long[]{1} );
        updates.put( CountsKeyFactory.nodeKey( 3 ), new long[]{1} );
        countStore.updateAll( 1, updates );
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryLogChannel logChannel = new InMemoryLogChannel( serializedBytes.array() );
        Snapshot.serialize( logChannel, countStore.snapshot( 1 ) );

        //WHEN
        serializedBytes.position( 8 );
        serializedBytes.putInt( 2 );//We serialized 3, but now the deserialization should only expect 2.

        Snapshot snapshot = Snapshot.deserialize( logChannel );
        Assert.assertEquals( 2, snapshot.getMap().size() );
    }

    @Test
    public void correctlyDeserializeEntityNode() throws IOException, UnknownKey
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryLogChannel logChannel = new InMemoryLogChannel( serializedBytes.array() );
        writeSimpleHeader( logChannel );
        logChannel.put( ENTITY_NODE.code );
        logChannel.putInt( 1 );
        logChannel.putLong( 1 );

        //WHEN
        NodeKey expectedNode = CountsKeyFactory.nodeKey( 1 );
        Snapshot snapshot = Snapshot.deserialize( logChannel );

        //THEN
        Assert.assertNotNull( snapshot.getMap().get( expectedNode ) );
        Assert.assertArrayEquals( new long[]{1}, snapshot.getMap().get( expectedNode ) );

    }

    @Test
    public void correctlyDeserializeEntityRelationship() throws IOException, UnknownKey
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryLogChannel logChannel = new InMemoryLogChannel( serializedBytes.array() );
        writeSimpleHeader( logChannel );
        logChannel.put( ENTITY_RELATIONSHIP.code );
        logChannel.putInt( 1 );
        logChannel.putInt( 1 );
        logChannel.putInt( 1 );
        logChannel.putLong( 1 );

        //WHEN
        RelationshipKey expectedNode = CountsKeyFactory.relationshipKey( 1, 1, 1 );
        Snapshot snapshot = Snapshot.deserialize( logChannel );

        //THEN
        Assert.assertNotNull( snapshot.getMap().get( expectedNode ) );
        Assert.assertArrayEquals( new long[]{1}, snapshot.getMap().get( expectedNode ) );
    }

    @Test
    public void correctlyDeserializeIndexSample() throws IOException, UnknownKey
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryLogChannel logChannel = new InMemoryLogChannel( serializedBytes.array() );
        writeSimpleHeader( logChannel );
        logChannel.put( INDEX_SAMPLE.code );
        logChannel.putInt( 1 );
        logChannel.putInt( 1 );
        logChannel.putLong( 1 );
        logChannel.putLong( 1 );

        //WHEN
        IndexSampleKey expectedNode = CountsKeyFactory.indexSampleKey( 1, 1 );
        Snapshot snapshot = Snapshot.deserialize( logChannel );

        //THEN
        Assert.assertNotNull( snapshot.getMap().get( expectedNode ) );
        Assert.assertArrayEquals( new long[]{1, 1}, snapshot.getMap().get( expectedNode ) );
    }

    @Test
    public void correctlyDeserializeIndexStatistics() throws IOException, UnknownKey
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryLogChannel logChannel = new InMemoryLogChannel( serializedBytes.array() );
        writeSimpleHeader( logChannel );
        logChannel.put( INDEX_STATISTICS.code );
        logChannel.putInt( 1 );
        logChannel.putInt( 1 );
        logChannel.putLong( 1 );
        logChannel.putLong( 1 );

        //WHEN
        IndexStatisticsKey expectedNode = CountsKeyFactory.indexStatisticsKey( 1, 1 );
        Snapshot snapshot = Snapshot.deserialize( logChannel );

        //THEN
        Assert.assertNotNull( snapshot.getMap().get( expectedNode ) );
        Assert.assertArrayEquals( new long[]{1, 1}, snapshot.getMap().get( expectedNode ) );
    }

    private void initializeBuffers( int serializedLength )
    {
        serializedBytes = ByteBuffer.allocate( serializedLength );
        expectedBytes = ByteBuffer.allocate( serializedLength );
        writeExpectedTxID( expectedBytes, 1 );
        writeExpectedCountStoreSize( expectedBytes, 1 );
    }

    private void writeSimpleHeader( InMemoryLogChannel logChannel ) throws IOException
    {
        logChannel.putLong( 1 );
        logChannel.putInt( 1 );
    }

    private void writeExpectedTxID( ByteBuffer buffer, long txId )
    {
        buffer.putLong( txId );
    }

    private void writeExpectedCountStoreSize( ByteBuffer buffer, int size )
    {
        buffer.putInt( size );
    }

    private void writeAndSerializeEntityNode( int labelId, long count ) throws IOException, UnknownKey
    {
        snapshot.getMap().put( CountsKeyFactory.nodeKey( labelId ), new long[]{count} );
        serialize( logChannel, snapshot );
    }

    private void writeAndSerializeEntityRelationship( int startId, int type, int endId, long count )
            throws IOException, UnknownKey
    {
        snapshot.getMap().put( CountsKeyFactory.relationshipKey( startId, type, endId ), new long[]{count} );
        serialize( logChannel, snapshot );
    }

    private void writeAndSerializeIndexSample( int labelId, int propertyKeyId, long count )
            throws IOException, UnknownKey
    {
        snapshot.getMap().put( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ), new long[]{count, count} );
        serialize( logChannel, snapshot );
    }

    private void writeAndSerializeIndexStatistics( int labelId, int propertyKeyId, long count )
            throws IOException, UnknownKey
    {
        snapshot.getMap()
                .put( CountsKeyFactory.indexStatisticsKey( labelId, propertyKeyId ), new long[]{count, count} );
        serialize( logChannel, snapshot );
    }
}
