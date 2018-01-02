/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.master;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.cluster.InstanceId;
import org.neo4j.com.Client;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static org.neo4j.com.Protocol.VOID_SERIALIZER;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;
import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_RESPONSE_UNPACKER;

public class SlaveClient extends Client<Slave> implements Slave
{
    private final InstanceId machineId;

    public SlaveClient( InstanceId machineId, String destinationHostNameOrIp, int destinationPort,
                        String originHostNameOrIp, LogProvider logProvider,
                        StoreId storeId, int maxConcurrentChannels, int chunkSize,
                        ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider, storeId,
                Protocol.DEFAULT_FRAME_LENGTH,
                new ProtocolVersion( SlaveServer.APPLICATION_PROTOCOL_VERSION, INTERNAL_PROTOCOL_VERSION ),
                HaSettings.read_timeout.apply( Functions.<String,String>nullFunction() ), maxConcurrentChannels,
                chunkSize, NO_OP_RESPONSE_UNPACKER, byteCounterMonitor, requestMonitor );
        this.machineId = machineId;
    }

    @Override
    public Response<Void> pullUpdates( final long upToAndIncludingTxId )
    {
        return sendRequest( SlaveRequestType.PULL_UPDATES, RequestContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
            {
                writeString( buffer, NeoStoreDataSource.DEFAULT_DATA_SOURCE_NAME );
                buffer.writeLong( upToAndIncludingTxId );
            }
        }, Protocol.VOID_DESERIALIZER );
    }

    public static enum SlaveRequestType implements RequestType<Slave>
    {
        PULL_UPDATES( new TargetCaller<Slave, Void>()
        {
            @Override
            public Response<Void> call( Slave master, RequestContext context, ChannelBuffer input,
                                        ChannelBuffer target )
            {
                readString( input ); // And discard
                return master.pullUpdates( input.readLong() );
            }
        }, VOID_SERIALIZER );

        private final TargetCaller caller;
        private final ObjectSerializer serializer;

        private SlaveRequestType( TargetCaller caller, ObjectSerializer serializer )
        {
            this.caller = caller;
            this.serializer = serializer;
        }

        @Override
        public TargetCaller getTargetCaller()
        {
            return caller;
        }

        @Override
        public ObjectSerializer getObjectSerializer()
        {
            return serializer;
        }

        @Override
        public byte id()
        {
            return (byte) ordinal();
        }

        @Override
        public boolean responseShouldBeUnpacked()
        {
            return false;
        }
    }

    @Override
    public int getServerId()
    {
        return machineId.toIntegerIndex();
    }
}
