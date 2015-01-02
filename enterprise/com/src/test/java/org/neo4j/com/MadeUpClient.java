/**
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
package org.neo4j.com;

import static org.neo4j.com.MadeUpServer.FRAME_LENGTH;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.com.RequestContext.EMPTY;
import static org.neo4j.com.RequestContext.lastAppliedTx;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.MadeUpServer.MadeUpRequestType;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;

public class MadeUpClient extends Client<MadeUpCommunicationInterface> implements MadeUpCommunicationInterface
{
    private final byte internalProtocolVersion;

    public MadeUpClient( int port, StoreId storeIdToExpect,
            byte internalProtocolVersion, byte applicationProtocolVersion, int chunkSize )
    {
        super( localhost(), port, new DevNullLoggingService(), new Monitors(), storeIdToExpect, FRAME_LENGTH,
                applicationProtocolVersion, Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS * 1000,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT,
                chunkSize );
        this.internalProtocolVersion = internalProtocolVersion;
    }

    private static String localhost()
    {
        try
        {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected byte getInternalProtocolVersion()
    {
        return internalProtocolVersion;
    }

    @Override
    public Response<Integer> multiply( final int value1, final int value2 )
    {
        return sendRequest( MadeUpRequestType.MULTIPLY, getRequestContext(), new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
            {
                buffer.writeInt( value1 );
                buffer.writeInt( value2 );
            }
        }, Protocol.INTEGER_DESERIALIZER );
    }

    private RequestContext getRequestContext()
    {
        return new RequestContext( EMPTY.getEpoch(), EMPTY.machineId(), EMPTY.getEventIdentifier(),
                new RequestContext.Tx[] { lastAppliedTx( DEFAULT_DATA_SOURCE_NAME, 1 ) }, EMPTY.getMasterId(),
                EMPTY.getChecksum() );
    }

    @Override
    public Response<Void> fetchDataStream( final MadeUpWriter writer, final int dataSize )
    {
        return sendRequest( MadeUpRequestType.FETCH_DATA_STREAM, getRequestContext(), new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
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
    public Response<Void> sendDataStream( final ReadableByteChannel data )
    {
        return sendRequest( MadeUpRequestType.SEND_DATA_STREAM, getRequestContext(), new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
            {
                BlockLogBuffer writer = new BlockLogBuffer( buffer, new Monitors().newMonitor( ByteCounterMonitor.class ) );
                try
                {
                    writer.write( data );
                }
                finally
                {
                    writer.done();
                }
            }
        }, Protocol.VOID_DESERIALIZER );
    }

    @Override
    public Response<Integer> throwException( final String messageInException )
    {
        return sendRequest( MadeUpRequestType.THROW_EXCEPTION, getRequestContext(), new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
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
}
