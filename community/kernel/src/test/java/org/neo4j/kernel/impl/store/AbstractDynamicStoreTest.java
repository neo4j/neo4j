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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AbstractDynamicStoreTest
{
    @Test
    public void shouldRecognizeDesignatedInUseBit() throws Exception
    {
        // GIVEN
        AbstractDynamicStore store = newTestableDynamicStore();
        try
        {
            // WHEN
            byte otherBitsInTheInUseByte = 0;
            for ( int i = 0; i < 8; i++ )
            {
                // THEN
                assertRecognizesByteAsInUse( store, otherBitsInTheInUseByte );
                otherBitsInTheInUseByte <<= 1;
                otherBitsInTheInUseByte |= 1;
            }
        }
        finally
        {
            store.close();
        }
    }

    private void assertRecognizesByteAsInUse( AbstractDynamicStore store, byte inUseByte )
    {
        assertTrue(  store.isInUse( (byte) (inUseByte | 0x10) ) );
        assertFalse( store.isInUse( (byte) (inUseByte & ~0x10) ) );
    }

    private AbstractDynamicStore newTestableDynamicStore()
    {
        return new AbstractDynamicStore( fileName, new Config(), IdType.ARRAY_BLOCK, new DefaultIdGeneratorFactory(),
                new BatchingPageCache( fsr.get(), 1000, 1, BatchingPageCache.SYNCHRONOUS, mock( Monitor.class ) ),
                fsr.get(), StringLogger.DEV_NULL, StoreVersionMismatchHandler.ALLOW_OLD_VERSION, new Monitors() )
        {
            @Override
            public void accept( Processor processor, DynamicRecord record )
            {   // Ignore
            }

            @Override
            public String getTypeDescriptor()
            {
                return "TestDynamicStore";
            }
        };
    }

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final File fileName = new File( "store" );

    @Before
    public void before() throws IOException
    {
        StoreChannel channel = fsr.get().create( fileName );
        try
        {
            ByteBuffer buffer = ByteBuffer.allocate( 4 );
            buffer.putInt( 60 );
            buffer.flip();
            channel.write( buffer );
        }
        finally
        {
            channel.close();
        }
    }
}
