/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.net;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.ThreadFactory;

public interface BootstrapConfiguration<TYPE extends Channel>
{
    static BootstrapConfiguration<? extends ServerSocketChannel> preferNativeServerConfig()
    {
        if ( Epoll.isAvailable() )
        {
            return EpollBootstrapConfig.epollServerConfig();
        }
        else if ( KQueue.isAvailable() )
        {
            return KQueueBootsrapConfig.kQueueServerConfig();
        }
        else
        {
            return NioBootstrapConfig.nioServerConfig();
        }
    }

    static BootstrapConfiguration<? extends SocketChannel> preferNativeClientConfig()
    {
        if ( Epoll.isAvailable() )
        {
            return EpollBootstrapConfig.epollClientConfig();
        }
        else if ( KQueue.isAvailable() )
        {
            return KQueueBootsrapConfig.kQueueClientConfig();
        }
        else
        {
            return NioBootstrapConfig.nioClientConfig();
        }
    }

    EventLoopGroup eventLoopGroup( ThreadFactory threadFactory );

    Class<TYPE> channelClass();
}
