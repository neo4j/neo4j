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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotDeserializer.deserialize;
import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotSerializer.serialize;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

/**
 * Serializes and deserialize count stores to test that they produce the same count stores.
 */
public class InMemoryCountsStoreCountsSnapshotSerializerIntegrationTest
{
    @Test
    public void smallWorkloadOnInMemoryLogTest() throws IOException
    {
        //GIVEN
        InMemoryClosableChannel tempChannel = new InMemoryClosableChannel();
        Map<CountsKey,long[]> map = CountsStoreMapGenerator.simpleCountStoreMap( 1 );
        CountsSnapshot countsSnapshot = new CountsSnapshot( 1, map );

        //WHEN
        serialize( tempChannel, countsSnapshot );
        CountsSnapshot recovered = deserialize( tempChannel );

        //THEN
        Assert.assertEquals( countsSnapshot.getTxId(), recovered.getTxId() );
        for ( Map.Entry<CountsKey, long[]> pair : countsSnapshot.getMap().entrySet() )
        {
            long[] value = recovered.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertArrayEquals( value, pair.getValue() );
        }

        for ( Map.Entry<CountsKey, long[]> pair : recovered.getMap().entrySet() )
        {
            long[] value = countsSnapshot.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertArrayEquals( value, pair.getValue() );
        }
    }


    @Test
    public void largeWorkloadOnPhysicalLogTest() throws IOException
    {
        //GIVEN
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File tempFile = File.createTempFile( "temp", "tmp" );
        StoreChannel rawChannel = fs.create( tempFile );
        final LogHeader header = new LogHeader( CURRENT_LOG_VERSION, 1, 42l );
        PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( rawChannel, header.logVersion, header.logFormatVersion );

        Map<CountsKey, long[]> map = CountsStoreMapGenerator.simpleCountStoreMap( 100000 );
        CountsSnapshot countsSnapshot = new CountsSnapshot( 1, map );
        CountsSnapshot recovered;

        //WHEN
        try ( PositionAwarePhysicalFlushableChannel tempChannel =
                      new PositionAwarePhysicalFlushableChannel( physicalLogVersionedStoreChannel ) )
        {

            serialize( tempChannel, countsSnapshot );
        }

        physicalLogVersionedStoreChannel.position( 0 );

        try ( ReadAheadLogChannel readAheadChannel = new ReadAheadLogChannel( physicalLogVersionedStoreChannel,
                LogVersionBridge.NO_MORE_CHANNELS ) )
        {
            recovered = deserialize( readAheadChannel );
        }

        //THEN
        Assert.assertEquals( countsSnapshot.getTxId(), recovered.getTxId() );
        for ( Map.Entry<CountsKey, long[]> pair : countsSnapshot.getMap().entrySet() )
        {
            long[] value = recovered.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertArrayEquals( value, pair.getValue() );
        }

        for ( Map.Entry<CountsKey, long[]> pair : recovered.getMap().entrySet() )
        {
            long[] value = countsSnapshot.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertArrayEquals( value, pair.getValue() );
        }
    }


}