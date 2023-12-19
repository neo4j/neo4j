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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.com.MadeUpServer.MadeUpRequestType;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.com.MadeUpServer.FRAME_LENGTH;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.com.RequestContext.EMPTY;

public abstract class MadeUpClient extends Client<MadeUpCommunicationInterface> implements MadeUpCommunicationInterface
{
    public MadeUpClient( int port, StoreId storeIdToExpect, int chunkSize, ResponseUnpacker responseUnpacker )
    {
        super( localhost(), port, null, NullLogProvider.getInstance(), storeIdToExpect, FRAME_LENGTH,
                Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS * 1000,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT,
                chunkSize, responseUnpacker,
                new Monitors().newMonitor( ByteCounterMonitor.class ),
                new Monitors().newMonitor( RequestMonitor.class ),
                new VersionAwareLogEntryReader<>() );
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
        return getProtocolVersion().getInternalProtocol();
    }

    @Override
    public Response<Integer> multiply( final int value1, final int value2 )
    {
        Serializer serializer = buffer ->
        {
            buffer.writeInt( value1 );
            buffer.writeInt( value2 );
        };
        return sendRequest( MadeUpServer.MadeUpRequestType.MULTIPLY, getRequestContext(), serializer,
                Protocol.INTEGER_DESERIALIZER );
    }

    private RequestContext getRequestContext()
    {
        return new RequestContext( EMPTY.getEpoch(), EMPTY.machineId(), EMPTY.getEventIdentifier(), 2,
                EMPTY.getChecksum() );
    }

    @Override
    public Response<Void> fetchDataStream( final MadeUpWriter writer, final int dataSize )
    {
        Serializer serializer = buffer -> buffer.writeInt( dataSize );
        Deserializer<Void> deserializer = ( buffer, temporaryBuffer ) ->
        {
            writer.write( new BlockLogReader( buffer ) );
            return null;
        };
        return sendRequest( MadeUpServer.MadeUpRequestType.FETCH_DATA_STREAM, getRequestContext(),
                serializer, deserializer );
    }

    @Override
    public Response<Void> sendDataStream( final ReadableByteChannel data )
    {
        Serializer serializer = buffer ->
        {
            try ( BlockLogBuffer writer = new BlockLogBuffer( buffer,
                    new Monitors().newMonitor( ByteCounterMonitor.class ) ) )
            {
                writer.write( data );
            }
        };
        return sendRequest( MadeUpServer.MadeUpRequestType.SEND_DATA_STREAM, getRequestContext(), serializer,
                Protocol.VOID_DESERIALIZER );
    }

    @Override
    public Response<Integer> throwException( final String messageInException )
    {
        Serializer serializer = buffer -> writeString( buffer, messageInException );
        Deserializer<Integer> deserializer = ( buffer, temporaryBuffer ) -> buffer.readInt();
        return sendRequest( MadeUpServer.MadeUpRequestType.THROW_EXCEPTION, getRequestContext(),
                serializer, deserializer );
    }

    @Override
    public Response<Integer> streamBackTransactions( final int responseToSendBack, final int txCount )
    {
        Serializer serializer = buffer ->
        {
            buffer.writeInt( responseToSendBack );
            buffer.writeInt( txCount );
        };
        Deserializer<Integer> integerDeserializer = ( buffer, temporaryBuffer ) -> buffer.readInt();
        return sendRequest( MadeUpRequestType.STREAM_BACK_TRANSACTIONS, getRequestContext(), serializer,
                integerDeserializer );
    }

    @Override
    public Response<Integer> informAboutTransactionObligations( final int responseToSendBack,
            final long desiredObligation )
    {
        Serializer serializer = buffer ->
        {
            buffer.writeInt( responseToSendBack );
            buffer.writeLong( desiredObligation );
        };
        Deserializer<Integer> deserializer = ( buffer, temporaryBuffer ) -> buffer.readInt();
        return sendRequest( MadeUpRequestType.INFORM_ABOUT_TX_OBLIGATIONS, getRequestContext(), serializer,
                deserializer );
    }
}
