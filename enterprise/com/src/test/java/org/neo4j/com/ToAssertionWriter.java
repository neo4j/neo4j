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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static org.junit.Assert.assertEquals;

public class ToAssertionWriter implements MadeUpWriter
{
    private int index;

    @Override
    public void write( ReadableByteChannel data )
    {
        ByteBuffer intermediate = ByteBuffer.allocate( 1000 );
        while ( true )
        {
            try
            {
                intermediate.clear();
                if ( data.read( intermediate ) == -1 )
                {
                    break;
                }
                intermediate.flip();
                while ( intermediate.remaining() > 0 )
                {
                    byte value = intermediate.get();
                    assertEquals( (index++) % 10, value );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
