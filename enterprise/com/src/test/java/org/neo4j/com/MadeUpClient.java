/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.com;

import static org.neo4j.com.Protocol.writeString;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.MadeUpServer.MadeUpRequestType;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class MadeUpClient extends Client<MadeUpCommunicationInterface> implements MadeUpCommunicationInterface
{
    private final StoreId storeIdToExpect;
    private final byte internalProtocolVersion;

    public MadeUpClient( int port, StoreId storeIdToExpect, byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        super( "localhost", port, new NotYetExistingGraphDatabase( "target/something" ),
                Protocol.DEFAULT_FRAME_LENGTH, applicationProtocolVersion,
                Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT );
        this.storeIdToExpect = storeIdToExpect;
        this.internalProtocolVersion = internalProtocolVersion;
    }

    @Override
    protected byte getInternalProtocolVersion()
    {
        return internalProtocolVersion;
    }

    @Override
    public Response<Integer> multiply( final int value1, final int value2 )
    {
        return sendRequest( MadeUpRequestType.MULTIPLY, SlaveContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeInt( value1 );
                buffer.writeInt( value2 );
            }
        }, Protocol.INTEGER_DESERIALIZER );
    }

    @Override
    public Response<Void> streamSomeData( final MadeUpWriter writer, final int dataSize )
    {
        return sendRequest( MadeUpRequestType.STREAM_SOME_DATA, SlaveContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeInt( dataSize );
            }
        }, new Deserializer<Void>()
        {
            @Override
            public Void read( ChannelBuffer buffer, ByteBuffer temporaryBuffer )
                    throws IOException
            {
                writer.write( new BlockLogReader( buffer ) );
                return null;
            }
        } );
    }

    @Override
    public Response<Integer> throwException( final String messageInException )
    {
        return sendRequest( MadeUpRequestType.THROW_EXCEPTION, SlaveContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                writeString( buffer, messageInException );
            }
        }, new Deserializer<Integer>()
        {
            public Integer read( ChannelBuffer buffer, ByteBuffer temporaryBuffer )
                    throws IOException
            {
                return buffer.readInt();
                    }
        } );
    }

    @Override
    protected StoreId getMyStoreId()
    {
        return storeIdToExpect;
    }
}
