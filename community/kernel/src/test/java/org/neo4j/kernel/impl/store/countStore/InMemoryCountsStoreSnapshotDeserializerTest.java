/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotDeserializer.deserialize;
import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotSerializer.serialize;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;


public class InMemoryCountsStoreSnapshotDeserializerTest
{
    InMemoryClosableChannel logChannel;
    CountsSnapshot countsSnapshot;
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
    public void correctlyDeserializeTxId() throws IOException
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel( serializedBytes.array(), false );
        logChannel.putLong( 72 );
        logChannel.putInt( 0 );

        //WHEN
        CountsSnapshot countsSnapshot = deserialize( logChannel );

        //THEN
        assertEquals( 72, countsSnapshot.getTxId() );
    }

    @Test
    public void correctlyDeserializeTxIdAndMapSize() throws IOException
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        Map<CountsKey,long[]> updates = new HashMap<>();
        updates.put( CountsKeyFactory.nodeKey( 1 ), new long[]{1} );
        updates.put( CountsKeyFactory.nodeKey( 2 ), new long[]{1} );
        updates.put( CountsKeyFactory.nodeKey( 3 ), new long[]{1} );
        countStore.updateAll( 1, updates );
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel( serializedBytes.array(), false );
        serialize( logChannel, countStore.snapshot( 1 ) );

        //WHEN
        serializedBytes.position( 8 );
        serializedBytes.putInt( 2 );//We serialized 3, but now the deserialization should only expect 2.

        //THEN
        CountsSnapshot countsSnapshot = deserialize( logChannel );
        assertEquals( 2, countsSnapshot.getMap().size() );
    }

    @Test
    public void correctlyDeserializeEntityNode() throws IOException
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel( serializedBytes.array(), false );
        writeSimpleHeader( logChannel );
        logChannel.put( ENTITY_NODE.code );
        logChannel.putInt( 1 );
        logChannel.putLong( 1 );

        //WHEN
        NodeKey expectedNode = CountsKeyFactory.nodeKey( 1 );
        CountsSnapshot countsSnapshot = deserialize( logChannel );

        //THEN
        assertNotNull( countsSnapshot.getMap().get( expectedNode ) );
        assertArrayEquals( new long[]{1}, countsSnapshot.getMap().get( expectedNode ) );

    }

    @Test
    public void correctlyDeserializeEntityRelationship() throws IOException
    {
        //GIVEN
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel( serializedBytes.array(), false );
        writeSimpleHeader( logChannel );
        logChannel.put( ENTITY_RELATIONSHIP.code );
        logChannel.putInt( 1 );
        logChannel.putInt( 1 );
        logChannel.putInt( 1 );
        logChannel.putLong( 1 );

        //WHEN
        RelationshipKey expectedNode = CountsKeyFactory.relationshipKey( 1, 1, 1 );
        CountsSnapshot countsSnapshot = deserialize( logChannel );

        //THEN
        assertNotNull( countsSnapshot.getMap().get( expectedNode ) );
        assertArrayEquals( new long[]{1}, countsSnapshot.getMap().get( expectedNode ) );
    }

    @Test
    public void correctlyDeserializeIndexSample() throws IOException
    {
        //GIVEN
        long indexId = 1;
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel( serializedBytes.array(), false );
        writeSimpleHeader( logChannel );
        logChannel.put( INDEX_SAMPLE.code );
        logChannel.putLong( indexId );
        logChannel.putLong( 1 );
        logChannel.putLong( 1 );

        //WHEN
        IndexSampleKey expectedNode = CountsKeyFactory.indexSampleKey( indexId );
        CountsSnapshot countsSnapshot = deserialize( logChannel );

        //THEN
        assertNotNull( countsSnapshot.getMap().get( expectedNode ) );
        assertArrayEquals( new long[]{1, 1}, countsSnapshot.getMap().get( expectedNode ) );
    }

    @Test
    public void correctlyDeserializeIndexStatistics() throws IOException
    {
        //GIVEN
        long indexId = 1;
        serializedBytes = ByteBuffer.allocate( 1000 );
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel( serializedBytes.array(), false );
        writeSimpleHeader( logChannel );
        logChannel.put( INDEX_STATISTICS.code );
        logChannel.putLong( indexId );
        logChannel.putLong( 1 );
        logChannel.putLong( 1 );

        //WHEN
        IndexStatisticsKey expectedNode = CountsKeyFactory.indexStatisticsKey( indexId );
        CountsSnapshot countsSnapshot = deserialize( logChannel );

        //THEN
        assertNotNull( countsSnapshot.getMap().get( expectedNode ) );
        assertArrayEquals( new long[]{1, 1}, countsSnapshot.getMap().get( expectedNode ) );
    }

    private void writeSimpleHeader( InMemoryClosableChannel logChannel ) throws IOException
    {
        logChannel.putLong( 1 );
        logChannel.putInt( 1 );
    }
}
