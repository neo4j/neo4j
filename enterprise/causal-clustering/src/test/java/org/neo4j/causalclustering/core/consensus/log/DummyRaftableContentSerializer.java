/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class DummyRaftableContentSerializer extends SafeChannelMarshal<ReplicatedContent>
{
    private static final int REPLICATED_INTEGER_TYPE = 0;
    private static final int REPLICATED_STRING_TYPE = 1;

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
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException
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
