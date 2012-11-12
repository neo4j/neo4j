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
package org.neo4j.kernel.ha;

import static org.neo4j.com.Protocol.VOID_SERIALIZER;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.kernel.configuration.ConfigurationDefaults.getDefault;
import static org.neo4j.kernel.ha.HaSettings.read_timeout;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.Client;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.TargetCaller;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

public class SlaveClient extends Client<Slave> implements Slave
{
    private final int machineId;
    
    public SlaveClient( int machineId, String hostNameOrIp, int port, StringLogger logger, StoreId storeId,
            int maxConcurrentChannels, int chunkSize )
    {
        super( hostNameOrIp, port, logger, storeId, Protocol.DEFAULT_FRAME_LENGTH, SlaveServer.APPLICATION_PROTOCOL_VERSION,
                HaSettings.read_timeout.valueOf( getDefault( read_timeout, HaSettings.class ), null ),
                maxConcurrentChannels, maxConcurrentChannels, chunkSize );
        this.machineId = machineId;
    }

    @Override
    public Response<Void> pullUpdates( final String resource, final long upToAndIncludingTxId )
    {
        return sendRequest( SlaveRequestType.PULL_UPDATES, RequestContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                writeString( buffer, resource );
                buffer.writeLong( upToAndIncludingTxId );
            }
        }, Protocol.VOID_DESERIALIZER );
    }

    static enum SlaveRequestType implements RequestType<Slave>
    {
        PULL_UPDATES( new TargetCaller<Slave, Void>()
        {
            @Override
            public Response<Void> call( Slave master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.pullUpdates( readString( input ), input.readLong() );
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
    }

    @Override
    public int getServerId()
    {
        return machineId;
    }
}
