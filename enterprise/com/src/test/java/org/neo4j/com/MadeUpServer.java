/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.com.Protocol.readString;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.tooling.RealClock;

public class MadeUpServer extends Server<MadeUpCommunicationInterface, Void>
{
    private volatile boolean responseWritten;
    private volatile boolean responseFailureEncountered;
    private final byte internalProtocolVersion;
    public static final int FRAME_LENGTH = 1024 * 1024 * 1;

    public MadeUpServer( MadeUpCommunicationInterface requestTarget, final int port, byte internalProtocolVersion,
                         byte applicationProtocolVersion, TxChecksumVerifier txVerifier, final int chunkSize )
    {
        super( requestTarget, new Server.Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS * 1000;
            }

            @Override
            public int getMaxConcurrentTransactions()
            {
                return DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS;
            }

            @Override
            public int getChunkSize()
            {
                return chunkSize;
            }

            @Override
            public HostnamePort getServerAddress()
            {
                return new HostnamePort( null, port );
            }
        }, new DevNullLoggingService(), FRAME_LENGTH, applicationProtocolVersion, txVerifier, new RealClock());
        this.internalProtocolVersion = internalProtocolVersion;
    }

    @Override
    protected void responseWritten( RequestType<MadeUpCommunicationInterface> type, Channel channel,
                                    RequestContext context )
    {
        responseWritten = true;
    }

    @Override
    protected void writeFailureResponse( Throwable exception, ChunkingChannelBuffer buffer )
    {
        responseFailureEncountered = true;
        super.writeFailureResponse( exception, buffer );
    }

    @Override
    protected byte getInternalProtocolVersion()
    {
        return internalProtocolVersion;
    }

    @Override
    protected RequestType<MadeUpCommunicationInterface> getRequestContext( byte id )
    {
        return MadeUpRequestType.values()[id];
    }

    @Override
    protected void finishOffChannel( Channel channel, RequestContext context )
    {
    }

    public boolean responseHasBeenWritten()
    {
        return responseWritten;
    }

    public boolean responseFailureEncountered()
    {
        return responseFailureEncountered;
    }

    static enum MadeUpRequestType implements RequestType<MadeUpCommunicationInterface>
    {
        MULTIPLY( new TargetCaller<MadeUpCommunicationInterface, Integer>()
        {
            @Override
            public Response<Integer> call( MadeUpCommunicationInterface master,
                                           RequestContext context, ChannelBuffer input, ChannelBuffer target )
            {
                int value1 = input.readInt();
                int value2 = input.readInt();
                return master.multiply( value1, value2 );
            }
        }, Protocol.INTEGER_SERIALIZER ),

        FETCH_DATA_STREAM( new TargetCaller<MadeUpCommunicationInterface, Void>()
        {
            @Override
            public Response<Void> call( MadeUpCommunicationInterface master,
                                        RequestContext context, ChannelBuffer input, ChannelBuffer target )
            {
                int dataSize = input.readInt();
                return master.fetchDataStream( new ToChannelBufferWriter( target ), dataSize );
            }
        }, Protocol.VOID_SERIALIZER ),

        SEND_DATA_STREAM( new TargetCaller<MadeUpCommunicationInterface, Void>()
        {
            @Override
            public Response<Void> call( MadeUpCommunicationInterface master,
                                        RequestContext context, ChannelBuffer input, ChannelBuffer target )
            {
                BlockLogReader reader = new BlockLogReader( input );
                try
                {
                    Response<Void> response = master.sendDataStream( reader );
                    return response;
                }
                finally
                {
                    try
                    {
                        reader.close();
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }, Protocol.VOID_SERIALIZER ),

        THROW_EXCEPTION( new TargetCaller<MadeUpCommunicationInterface, Integer>()
        {
            @Override
            public Response<Integer> call( MadeUpCommunicationInterface master,
                                           RequestContext context, ChannelBuffer input, ChannelBuffer target )
            {
                return master.throwException( readString( input ) );
            }
        }, Protocol.VOID_SERIALIZER );

        private final TargetCaller masterCaller;
        private final ObjectSerializer serializer;

        MadeUpRequestType( TargetCaller masterCaller, ObjectSerializer serializer )
        {
            this.masterCaller = masterCaller;
            this.serializer = serializer;
        }

        @Override
        public TargetCaller getTargetCaller()
        {
            return this.masterCaller;
        }

        @Override
        public ObjectSerializer getObjectSerializer()
        {
            return this.serializer;
        }

        @Override
        public byte id()
        {
            return (byte) ordinal();
        }
    }
}
