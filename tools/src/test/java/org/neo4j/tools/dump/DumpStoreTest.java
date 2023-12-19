/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.dump;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class DumpStoreTest
{
    @Test
    public void dumpStoreShouldPrintBufferWithContent()
    {
        // Given
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        DumpStore dumpStore = new DumpStore( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        for ( byte i = 0; i < 10; i++ )
        {
            buffer.put( i );
        }
        buffer.flip();

        AbstractBaseRecord record = Mockito.mock( AbstractBaseRecord.class );

        // When
        //when( record.inUse() ).thenReturn( true );
        dumpStore.dumpHex( record, buffer, 2, 4 );

        // Then
        Assert.assertEquals( String.format( "@ 0x00000008: 00 01 02 03  04 05 06 07  08 09%n" ), outStream.toString() );
    }

    @Test
    public void dumpStoreShouldPrintShorterMessageForAllZeroBuffer()
    {
        // Given
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        DumpStore dumpStore = new DumpStore( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        AbstractBaseRecord record = Mockito.mock( AbstractBaseRecord.class );

        // When
        //when( record.inUse() ).thenReturn( true );
        dumpStore.dumpHex( record, buffer, 2, 4 );

        // Then
        Assert.assertEquals( String.format( ": all zeros @ 0x8 - 0xc%n" ), outStream.toString() );
    }
}
