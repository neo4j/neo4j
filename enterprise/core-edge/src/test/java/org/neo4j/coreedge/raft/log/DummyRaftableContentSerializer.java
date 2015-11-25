/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Serializer;

public class DummyRaftableContentSerializer implements Serializer
{
    @Override
    public ByteBuffer serialize( ReplicatedContent content )
    {
        if ( content instanceof ReplicatedInteger )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 1 + 4 );
            buffer.put( (byte) 0 );
            buffer.putInt( ((ReplicatedInteger) content).get() );
            buffer.flip();
            return buffer;
        }
        else if ( content instanceof ReplicatedString )
        {
            String value = ((ReplicatedString) content).get();
            byte[] stringBytes = value.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate( 1 + stringBytes.length );
            buffer.put( (byte) 1 );
            buffer.put( stringBytes );
            buffer.flip();
            return buffer;
        }
        throw new IllegalArgumentException( "Unknown content type: " + content );
    }

    @Override
    public ReplicatedContent deserialize( ByteBuffer buffer )
    {
        byte type = buffer.get();

        if ( type == (byte) 0 )
        {
            return ReplicatedInteger.valueOf( buffer.getInt() );
        }
        else if ( type == (byte) 1 )
        {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get( bytes );
            return ReplicatedString.valueOf( new String( bytes ) );
        }
        throw new IllegalArgumentException( "Unknown content type: " + type );
    }
}
