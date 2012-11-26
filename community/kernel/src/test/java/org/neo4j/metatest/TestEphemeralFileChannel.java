/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.metatest;

import static java.nio.ByteBuffer.allocateDirect;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestEphemeralFileChannel
{
    @Test
    public void smoke() throws Exception
    {
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        FileChannel channel = fs.open( new File("yo"), "rw" );
        
        // Clear it because we depend on it to be zeros where we haven't written
        ByteBuffer buffer = allocateDirect( 23 );
        buffer.put( new byte[23] ); // zeros
        buffer.flip();
        channel.write( buffer );
        channel = fs.open( new File("yo"), "rw" );
        long longValue = 1234567890L;
        
        // [1].....[2]........[1234567890L]...
        
        buffer.clear();
        buffer.limit( 1 );
        buffer.put( (byte) 1 );
        buffer.flip();
        channel.write( buffer );
        
        buffer.clear();
        buffer.limit( 1 );
        buffer.put( (byte) 2 );
        buffer.flip();
        channel.position( 6 );
        channel.write( buffer );

        buffer.clear();
        buffer.limit( 8 );
        buffer.putLong( longValue );
        buffer.flip();
        channel.position( 15 );
        channel.write( buffer );
        assertEquals( 23, channel.size() );
        
        // Read with position
        // byte 0
        buffer.clear();
        buffer.limit( 1 );
        channel.read( buffer, 0 );
        buffer.flip();
        assertEquals( (byte) 1, buffer.get() );
        
        // bytes 5-7
        buffer.clear();
        buffer.limit( 3 );
        channel.read( buffer, 5 );
        buffer.flip();
        assertEquals( (byte) 0, buffer.get() );
        assertEquals( (byte) 2, buffer.get() );
        assertEquals( (byte) 0, buffer.get() );
        
        // bytes 15-23
        buffer.clear();
        buffer.limit( 8 );
        channel.read( buffer, 15 );
        buffer.flip();
        assertEquals( longValue, buffer.getLong() );
    }
}
