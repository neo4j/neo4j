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
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;

import java.util.concurrent.ThreadFactory;

public abstract class KQueueBootstrapConfig<CHANNEL extends Channel> implements BootstrapConfiguration<CHANNEL>
{
    public static KQueueBootstrapConfig<KQueueServerSocketChannel> kQueueServerConfig()
    {
        return new KQueueBootstrapConfig<KQueueServerSocketChannel>()
        {
            @Override
            public Class<KQueueServerSocketChannel> channelClass()
            {
                return KQueueServerSocketChannel.class;
            }
        };
    }

    public static KQueueBootstrapConfig<KQueueSocketChannel> kQueueClientConfig()
    {
        return new KQueueBootstrapConfig<KQueueSocketChannel>()
        {
            @Override
            public Class<KQueueSocketChannel> channelClass()
            {
                return KQueueSocketChannel.class;
            }
        };
    }

    @Override
    public EventLoopGroup eventLoopGroup( ThreadFactory threadFactory )
    {
        return new KQueueEventLoopGroup( 0, threadFactory );
    }
}
