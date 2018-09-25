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

import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BootstrapConfigurationTest
{
    @Test
    @AssumeEpoll
    void shouldChooseEpollIfAvailable()
    {
        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( true );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( true );

        assertEquals( EpollSocketChannel.class, cConfig.channelClass() );
        assertEquals( EpollServerSocketChannel.class, sConfig.channelClass() );
    }

    @Test
    @AssumeKQueue
    void shouldChooseKqueueIfAvailable()
    {
        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( true );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( true );

        assertEquals( KQueueSocketChannel.class, cConfig.channelClass() );
        assertEquals( KQueueServerSocketChannel.class, sConfig.channelClass() );
    }

    @Test
    @AssumeNoEpollOrKQueue
    void shouldChooseNioIfNoNativeAvailable()
    {
        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( true );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( true );

        assertEquals( NioSocketChannel.class, cConfig.channelClass() );
        assertEquals( NioServerSocketChannel.class, sConfig.channelClass() );
    }

    @Test
    void shouldChooseNioIfNativeIsNotPrefered()
    {
        BootstrapConfiguration<? extends SocketChannel> cConfig = BootstrapConfiguration.clientConfig( false );
        BootstrapConfiguration<? extends ServerSocketChannel> sConfig = BootstrapConfiguration.serverConfig( false );

        assertEquals( NioSocketChannel.class, cConfig.channelClass() );
        assertEquals( NioServerSocketChannel.class, sConfig.channelClass() );
    }

    private static class KQueueExecutionCondition implements ExecutionCondition
    {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition( ExtensionContext context )
        {
            return KQueue.isAvailable() ? ConditionEvaluationResult.enabled( "KQueue is available" )
                                        : ConditionEvaluationResult.disabled( "KQueue is not available" );
        }
    }

    private static class EpollExecutionCondition implements ExecutionCondition
    {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition( ExtensionContext context )
        {
            return Epoll.isAvailable() ? ConditionEvaluationResult.enabled( "Epoll is available" )
                                       : ConditionEvaluationResult.disabled( "Epoll is not available" );
        }
    }

    private static class NoEpollOrKqueueCondition implements ExecutionCondition
    {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition( ExtensionContext context )
        {
            return Epoll.isAvailable() || KQueue.isAvailable() ? ConditionEvaluationResult.disabled( "Epoll or Kqueue is available" )
                                                               : ConditionEvaluationResult.enabled( "Epoll and KQueue is not available" );
        }
    }

    @Retention( RetentionPolicy.RUNTIME )
    @ExtendWith( KQueueExecutionCondition.class )
    private @interface AssumeKQueue
    { }

    @Retention( RetentionPolicy.RUNTIME )
    @ExtendWith( EpollExecutionCondition.class )
    private @interface AssumeEpoll
    { }

    @Retention( RetentionPolicy.RUNTIME )
    @ExtendWith( NoEpollOrKqueueCondition.class )
    private @interface AssumeNoEpollOrKQueue
    { }
}
