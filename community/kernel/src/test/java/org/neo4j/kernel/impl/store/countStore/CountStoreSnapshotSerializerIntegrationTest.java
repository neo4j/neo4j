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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static org.neo4j.kernel.impl.store.countStore.Snapshot.deserialize;
import static org.neo4j.kernel.impl.store.countStore.Snapshot.serialize;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

/**
 * Serializes and deserialize count stores to test that they produce the same count stores.
 */
public class CountStoreSnapshotSerializerIntegrationTest
{
    @Test
    public void smallWorkloadOnInMemoryLogTest() throws IOException, UnknownKey
    {
        //GIVEN
        InMemoryLogChannel tempChannel = new InMemoryLogChannel();
        ConcurrentHashMap<CountsKey,long[]> map = CountStoreMapGenerator.simpleCountStoreMap( 1 );
        Snapshot snapshot = new Snapshot( 1, map );

        //WHEN
        serialize( tempChannel, snapshot );
        Snapshot recovered = deserialize( tempChannel );

        //THEN
        Assert.assertEquals( snapshot.getTxId(), recovered.getTxId() );
        for ( Map.Entry<CountsKey,long[]> pair : snapshot.getMap().entrySet() )
        {
            long[] value = recovered.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertTrue( Arrays.equals( value, pair.getValue() ) );
        }

        for ( Map.Entry<CountsKey,long[]> pair : recovered.getMap().entrySet() )
        {
            long[] value = snapshot.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertTrue( Arrays.equals( value, pair.getValue() ) );
        }
    }


    @Test
    public void largeWorkloadOnPhysicalLogTest() throws IOException, UnknownKey
    {
        //GIVEN
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File tempFile = new File( "temp.dat" );
        tempFile.deleteOnExit();
        StoreChannel rawChannel = fs.create( tempFile );
        final LogHeader header = new LogHeader( CURRENT_LOG_VERSION, 1, 42l );
        PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( rawChannel, header.logVersion, header.logFormatVersion );

        ConcurrentHashMap<CountsKey,long[]> map = CountStoreMapGenerator.simpleCountStoreMap( 100000 );
        Snapshot snapshot = new Snapshot( 1, map );
        Snapshot recovered;

        //WHEN
        try ( PhysicalWritableLogChannel tempChannel = new PhysicalWritableLogChannel(
                physicalLogVersionedStoreChannel ) )
        {

            serialize( tempChannel, snapshot );
        }

        physicalLogVersionedStoreChannel.position( 0 );

        try ( ReadAheadLogChannel readAheadLogChannel = new ReadAheadLogChannel( physicalLogVersionedStoreChannel,
                LogVersionBridge.NO_MORE_CHANNELS ) )
        {
            recovered = deserialize( readAheadLogChannel );
        }

        //THEN
        Assert.assertEquals( snapshot.getTxId(), recovered.getTxId() );
        for ( Map.Entry<CountsKey,long[]> pair : snapshot.getMap().entrySet() )
        {
            long[] value = recovered.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertTrue( Arrays.equals( value, pair.getValue() ) );
        }

        for ( Map.Entry<CountsKey,long[]> pair : recovered.getMap().entrySet() )
        {
            long[] value = snapshot.getMap().get( pair.getKey() );
            Assert.assertNotNull( value );
            Assert.assertTrue( Arrays.equals( value, pair.getValue() ) );
        }
    }


}