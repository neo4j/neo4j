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
package org.neo4j.com;

import org.jboss.netty.channel.Channel;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.time.Clocks;

import static org.neo4j.com.Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class MadeUpServer extends Server<MadeUpCommunicationInterface, Void>
{
    private volatile boolean responseWritten;
    private volatile boolean responseFailureEncountered;
    private final byte internalProtocolVersion;
    public static final int FRAME_LENGTH = 1024 * 1024;

    public MadeUpServer( MadeUpCommunicationInterface requestTarget, final int port, byte internalProtocolVersion,
                         byte applicationProtocolVersion, TxChecksumVerifier txVerifier, final int chunkSize )
    {
        super( requestTarget, new Configuration()
                {
                    @Override
                    public long getOldChannelThreshold()
                    {
                        return DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS * 1000;
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
                }, getInstance(), FRAME_LENGTH,
                new ProtocolVersion( applicationProtocolVersion, ProtocolVersion.INTERNAL_PROTOCOL_VERSION ),
                txVerifier, Clocks.systemClock(), new Monitors().newMonitor( ByteCounterMonitor.class ),
                new Monitors().newMonitor( RequestMonitor.class ) );
        this.internalProtocolVersion = internalProtocolVersion;
    }

    @Override
    protected void responseWritten( RequestType type, Channel channel,
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
    protected RequestType getRequestContext( byte id )
    {
        return MadeUpRequestType.values()[id];
    }

    @Override
    protected void stopConversation( RequestContext context )
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

    enum MadeUpRequestType implements RequestType
    {
        MULTIPLY( ( master, context, input, target ) ->
        {
            int value1 = input.readInt();
            int value2 = input.readInt();
            return master.multiply( value1, value2 );
        }, Protocol.INTEGER_SERIALIZER, 0 ),

        FETCH_DATA_STREAM( ( master, context, input, target ) ->
        {
            int dataSize = input.readInt();
            return master.fetchDataStream( new ToChannelBufferWriter( target ), dataSize );
        }, Protocol.VOID_SERIALIZER ),

        SEND_DATA_STREAM( ( master, context, input, target ) ->
        {
            try ( BlockLogReader reader = new BlockLogReader( input ) )
            {
                return master.sendDataStream( reader );
            }
        }, Protocol.VOID_SERIALIZER )
        {
            @Override
            public boolean responseShouldBeUnpacked()
            {
                return false;
            }
        },

        THROW_EXCEPTION( ( master, context, input, target ) -> master.throwException(
                readString( input ) ), Protocol.VOID_SERIALIZER, 0 ),

        CAUSE_READ_CONTEXT_EXCEPTION( ( master, context, input, target ) ->
        {
            throw new AssertionError(
                    "Test should not reach this far, it should fail while reading the request context." );
        }, Protocol.VOID_SERIALIZER ),

        STREAM_BACK_TRANSACTIONS(
                ( master, context, input, target ) -> master.streamBackTransactions(
                        input.readInt(), input.readInt() ), Protocol.INTEGER_SERIALIZER, 0 ),

        INFORM_ABOUT_TX_OBLIGATIONS(
                ( master, context, input, target ) -> master.informAboutTransactionObligations(
                        input.readInt(), input.readLong() ), Protocol.INTEGER_SERIALIZER, 0 );

        private final TargetCaller masterCaller;
        private final ObjectSerializer serializer;

        MadeUpRequestType( TargetCaller<MadeUpCommunicationInterface,Integer> masterCaller, ObjectSerializer serializer,
                           int ignore )
        {
            this.masterCaller = masterCaller;
            this.serializer = serializer;
        }

        MadeUpRequestType( TargetCaller<MadeUpCommunicationInterface,Void> masterCaller, ObjectSerializer serializer )
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

        @Override
        public boolean responseShouldBeUnpacked()
        {
            return true;
        }
    }
}
