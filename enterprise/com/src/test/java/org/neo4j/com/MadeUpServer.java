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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

public class MadeUpServer extends Server<MadeUpCommunicationInterface, Void>
{
    private volatile boolean responseWritten;
    private volatile boolean responseFailureEncountered;
    private final byte internalProtocolVersion;

    public MadeUpServer( MadeUpCommunicationInterface realMaster, int port, byte internalProtocolVersion,
            byte applicationProtocolVersion, TxChecksumVerifier txVerifier )
    {
        super( realMaster, port, null, Protocol.DEFAULT_FRAME_LENGTH, applicationProtocolVersion,
                DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS, Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS, txVerifier );
        this.internalProtocolVersion = internalProtocolVersion;
    }

    @Override
    protected void responseWritten( RequestType<MadeUpCommunicationInterface> type, Channel channel,
            SlaveContext context )
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
    protected void finishOffChannel( Channel channel, SlaveContext context )
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
        MULTIPLY( new MasterCaller<MadeUpCommunicationInterface, Integer>()
        {
            @Override
            public Response<Integer> callMaster( MadeUpCommunicationInterface master,
                    SlaveContext context, ChannelBuffer input, ChannelBuffer target )
            {
                int value1 = input.readInt();
                int value2 = input.readInt();
                return master.multiply( value1, value2 );
            }
        }, Protocol.INTEGER_SERIALIZER ),
        
        STREAM_SOME_DATA( new MasterCaller<MadeUpCommunicationInterface, Void>()
        {
            @Override
            public Response<Void> callMaster( MadeUpCommunicationInterface master,
                    SlaveContext context, ChannelBuffer input, ChannelBuffer target )
            {
                int dataSize = input.readInt();
                return master.streamSomeData( new ToChannelBufferWriter( target ), dataSize );
            }
        }, Protocol.VOID_SERIALIZER ),
        
        THROW_EXCEPTION( new MasterCaller<MadeUpCommunicationInterface, Integer>()
        {
            @Override
            public Response<Integer> callMaster( MadeUpCommunicationInterface master,
                    SlaveContext context, ChannelBuffer input, ChannelBuffer target )
            {
                return master.throwException( readString( input ) );
            }
        }, Protocol.VOID_SERIALIZER );
        
        private final MasterCaller masterCaller;
        private final ObjectSerializer serializer;
        
        MadeUpRequestType( MasterCaller masterCaller, ObjectSerializer serializer )
        {
            this.masterCaller = masterCaller;
            this.serializer = serializer;
        }

        @Override
        public MasterCaller getMasterCaller()
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
