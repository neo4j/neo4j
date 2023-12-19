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
package org.neo4j.causalclustering.core.state.machines.dummy;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class DummyRequest implements CoreReplicatedContent
{
    private final byte[] data;

    public DummyRequest( byte[] data )
    {
        this.data = data;
    }

    @Override
    public boolean hasSize()
    {
        return true;
    }

    @Override
    public long size()
    {
        return data.length;
    }

    public long byteCount()
    {
        return data != null ? data.length : 0;
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    public static class Marshal extends SafeChannelMarshal<DummyRequest>
    {
        public static final Marshal INSTANCE = new Marshal();

        @Override
        public void marshal( DummyRequest dummy, WritableChannel channel ) throws IOException
        {
            if ( dummy.data != null )
            {
                channel.putInt( dummy.data.length );
                channel.put( dummy.data, dummy.data.length );
            }
            else
            {
                channel.putInt( 0 );
            }
        }

        @Override
        protected DummyRequest unmarshal0( ReadableChannel channel ) throws IOException
        {
            int length = channel.getInt();
            byte[] data;
            if ( length > 0 )
            {
                data = new byte[length];
                channel.get( data, length );
            }
            else
            {
                data = null;
            }
            return new DummyRequest( data );
        }
    }
}
