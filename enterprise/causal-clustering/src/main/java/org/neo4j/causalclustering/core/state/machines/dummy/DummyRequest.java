/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.state.machines.dummy;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.ByteArrayChunkedEncoder;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
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
    public OptionalLong size()
    {
        return OptionalLong.of( (long) data.length );
    }

    @Override
    public void handle( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
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

    public ChunkedInput<ByteBuf> encoder()
    {
        byte[] array = data;
        if ( array == null )
        {
            array = new byte[0];
        }
        return new ByteArrayChunkedEncoder( array );
    }

    public static DummyRequest decode( ByteBuf byteBuf )
    {
        int length = byteBuf.readableBytes();
        byte[] array = new byte[length];
        byteBuf.readBytes( array );
        return new DummyRequest( array );
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

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DummyRequest that = (DummyRequest) o;
        return Arrays.equals( data, that.data );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( data );
    }
}
