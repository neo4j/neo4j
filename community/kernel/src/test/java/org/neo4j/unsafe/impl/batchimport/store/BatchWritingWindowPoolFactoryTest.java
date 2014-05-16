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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.store.BatchWritingWindowPoolFactory;
import org.neo4j.unsafe.impl.batchimport.store.Monitor;

import static org.junit.Assert.assertArrayEquals;

import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class BatchWritingWindowPoolFactoryTest
{
    @Test
    public void shouldWriteACoupleOfBatches() throws Exception
    {
        // GIVEN
        int recordSize = 20;
        int bufferedRecords = 10;
        int numberOfReservedLowIds = 0;
        File file = new File( "store" );
        FileSystemAbstraction fsa = fs.get();
        try ( StoreChannel channel = fsa.open( file, "rw" ) )
        {
            // WHEN
            WindowPoolFactory factory = new BatchWritingWindowPoolFactory( recordSize*bufferedRecords + recordSize/2,
                    Monitor.NO_MONITOR );
            WindowPool pool = factory.create( file, recordSize, channel, new Config(), DEV_NULL, numberOfReservedLowIds );
            byte[] staticRecordBytes = ladderBytes( recordSize );
            int recordsToWrite = bufferedRecords*2 + bufferedRecords/2;
            for ( int i = 0; i < recordsToWrite; i++ )
            {
                PersistenceWindow window = pool.acquire( i, OperationType.WRITE );
                try
                {
                    Buffer buffer = window.getOffsettedBuffer( i );
                    buffer.put( staticRecordBytes );
                }
                finally
                {
                    pool.release( window );
                }
            }
            pool.close();

            // THEN
            channel.position( recordSize*numberOfReservedLowIds );
            verifyChannel( staticRecordBytes, recordsToWrite, channel );
        }
    }

    private void verifyChannel( byte[] staticRecordBytes, int recordsToWrite, StoreChannel channel )
            throws IOException
    {
        byte[] readBytes = new byte[staticRecordBytes.length];
        for ( int i = 0; i < recordsToWrite; i++ )
        {
            channel.read( ByteBuffer.wrap( readBytes ) );
            assertArrayEquals( staticRecordBytes, readBytes );
        }
    }

    private byte[] ladderBytes( int recordSize )
    {
        byte[] bytes = new byte[recordSize];
        for ( int i = 0; i < recordSize; i++ )
        {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
