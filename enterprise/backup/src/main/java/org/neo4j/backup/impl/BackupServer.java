/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.backup.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import org.neo4j.backup.TheBackupInterface;
import org.neo4j.backup.impl.BackupClient.BackupRequestType;
import org.neo4j.com.ChunkingChannelBuffer;
import org.neo4j.com.Client;
import org.neo4j.com.Protocol;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;
import static org.neo4j.com.TxChecksumVerifier.ALWAYS_MATCH;

public class BackupServer extends Server<TheBackupInterface,Object>
{
    private static final long DEFAULT_OLD_CHANNEL_THRESHOLD = Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS * 1000;
    private static final int DEFAULT_MAX_CONCURRENT_TX = 3;

    private static final BackupRequestType[] contexts = BackupRequestType.values();

    /**
     * Protocol Version : Product Version
     *                1 : * to 3.0.x
     *                2 : 3.1.x
     */
    public static final ProtocolVersion BACKUP_PROTOCOL_VERSION =
            new ProtocolVersion( (byte) 2, INTERNAL_PROTOCOL_VERSION );

    public static final int DEFAULT_PORT = 6362;
    static final int FRAME_LENGTH = Protocol.MEGA * 4;

    public BackupServer( TheBackupInterface requestTarget, final HostnamePort server, LogProvider logProvider,
                         ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( requestTarget, newBackupConfig( FRAME_LENGTH, server ), logProvider, FRAME_LENGTH,
                BACKUP_PROTOCOL_VERSION, ALWAYS_MATCH, Clocks.systemClock(), byteCounterMonitor, requestMonitor );
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
    protected void responseWritten( RequestType type, Channel channel,
                                    RequestContext context )
    {
    }

    @Override
    protected RequestType getRequestContext( byte id )
    {
        return contexts[id];
    }

    @Override
    protected void stopConversation( RequestContext context )
    {
    }
}
