/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.EndOfStreamException;
import org.neo4j.coreedge.raft.state.SafeChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class DummyRaftableContentSerializer extends SafeChannelMarshal<ReplicatedContent>
{
    public static final int REPLICATED_INTEGER_TYPE = 0;
    public static final int REPLICATED_STRING_TYPE = 1;

    @Override
    public void marshal( ReplicatedContent content, WritableChannel channel ) throws IOException
    {
        if ( content instanceof ReplicatedInteger )
        {
            channel.put( (byte) REPLICATED_INTEGER_TYPE );
            channel.putInt( ((ReplicatedInteger) content).get() );
        }
        else if ( content instanceof ReplicatedString )
        {
            String value = ((ReplicatedString) content).get();
            byte[] stringBytes = value.getBytes();
            channel.put( (byte) REPLICATED_STRING_TYPE );
            channel.putInt( stringBytes.length );
            channel.put( stringBytes, stringBytes.length );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type: " + content );
        }
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        byte type = channel.get();
        switch ( type )
        {
        case REPLICATED_INTEGER_TYPE:
            return ReplicatedInteger.valueOf( channel.getInt() );
        case REPLICATED_STRING_TYPE:
            int length = channel.getInt();
            byte[] bytes = new byte[length];
            channel.get( bytes, length );
            return ReplicatedString.valueOf( new String( bytes ) );
        default:
            throw new IllegalArgumentException( "Unknown content type: " + type );
        }
    }
}
