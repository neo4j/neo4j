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
package org.neo4j.backup;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import org.neo4j.backup.BackupClient.BackupRequestType;
import org.neo4j.com.ChunkingChannelBuffer;
import org.neo4j.com.Client;
import org.neo4j.com.Protocol;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;

class BackupServer extends Server<TheBackupInterface,Object>
{
    private static final long DEFAULT_OLD_CHANNEL_THRESHOLD = Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS * 1000;
    private static final int DEFAULT_MAX_CONCURRENT_TX = 3;

    private static final BackupRequestType[] contexts = BackupRequestType.values();

    static final byte PROTOCOL_VERSION = 1;
    static final int DEFAULT_PORT = 6362;
    static final int FRAME_LENGTH = Protocol.MEGA * 4;

    public BackupServer( TheBackupInterface requestTarget, final HostnamePort server,
                         LogProvider logProvider, ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( requestTarget, newBackupConfig( FRAME_LENGTH, server ), logProvider, FRAME_LENGTH,
                new ProtocolVersion( PROTOCOL_VERSION, ProtocolVersion.INTERNAL_PROTOCOL_VERSION ),
                TxChecksumVerifier.ALWAYS_MATCH, SYSTEM_CLOCK, byteCounterMonitor, requestMonitor );
    }

    @Override
    protected ChunkingChannelBuffer newChunkingBuffer( ChannelBuffer bufferToWriteTo, Channel channel, int capacity,
            byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        return new BufferReusingChunkingChannelBuffer( bufferToWriteTo, channel, capacity, internalProtocolVersion,
                applicationProtocolVersion );
    }

    private static Configuration newBackupConfig( final int chunkSize, final HostnamePort server )
    {
        return new Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return DEFAULT_OLD_CHANNEL_THRESHOLD;
            }

            @Override
            public int getMaxConcurrentTransactions()
            {
                return DEFAULT_MAX_CONCURRENT_TX;
            }

            @Override
            public int getChunkSize()
            {
                return chunkSize;
            }

            @Override
            public HostnamePort getServerAddress()
            {
                return server;
            }
        };
    }

    @Override
    protected void responseWritten( RequestType<TheBackupInterface> type, Channel channel,
                                    RequestContext context )
    {
    }

    @Override
    protected RequestType<TheBackupInterface> getRequestContext( byte id )
    {
        return contexts[id];
    }

    @Override
    protected void stopConversation( RequestContext context )
    {
    }
}
