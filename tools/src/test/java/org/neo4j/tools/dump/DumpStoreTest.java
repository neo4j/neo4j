/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.dump;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
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
