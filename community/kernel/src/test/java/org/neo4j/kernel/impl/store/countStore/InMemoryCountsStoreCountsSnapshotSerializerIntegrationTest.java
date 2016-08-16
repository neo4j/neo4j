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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotDeserializer.deserialize;
import static org.neo4j.kernel.impl.store.countStore.CountsSnapshotSerializer.serialize;

/**
 * Serializes and deserialize count stores to test that they produce the same count stores.
 */
public class InMemoryCountsStoreCountsSnapshotSerializerIntegrationTest
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

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
        File tempFile = new File( testDir.directory(), "temp" );
        StoreChannel rawChannel = fs.create( tempFile );

        Map<CountsKey, long[]> map = CountsStoreMapGenerator.simpleCountStoreMap( 100000 );
        CountsSnapshot countsSnapshot = new CountsSnapshot( 1, map );
        CountsSnapshot recovered;

        //WHEN
        try( FlushableChannel tempChannel = new PhysicalFlushableChannel( rawChannel ) )
        {
            serialize( tempChannel, countsSnapshot );
        } // close() here is necessary to flush the temp buffer into the channel so we can read it next

        rawChannel = fs.open( tempFile, "r" ); // The try-with-resources closes the channel, need to reopen
        try ( ReadAheadChannel<StoreChannel> readAheadChannel = new ReadAheadChannel<>( rawChannel ) )
        {
            recovered = deserialize( readAheadChannel );

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
}
