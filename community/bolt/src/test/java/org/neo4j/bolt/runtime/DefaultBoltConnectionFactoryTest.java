/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.runtime;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.runtime.scheduling.BoltSchedulerProvider;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.bolt.transport.pipeline.KeepAliveHandler;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

class DefaultBoltConnectionFactoryTest {

    @Test
    void shouldInstallKeepAliveHandler() {
        var schedulerProvider = mock(BoltSchedulerProvider.class);
        var config = Config.newBuilder()
                .set(BoltConnector.connection_keep_alive_type, BoltConnector.KeepAliveRequestType.ALL)
                .build();
        var logService = mock(LogService.class);

        var pipeline = mock(ChannelPipeline.class);
        var channel = mock(Channel.class, RETURNS_MOCKS);
        var protector = mock(ChannelProtector.class);
        var boltChannel = new BoltChannel("bolt123", "joffrey", channel, protector);
        var stateMachine = mock(BoltStateMachine.class);
        var messageWriter = mock(BoltResponseMessageWriter.class);

        when(channel.pipeline()).thenReturn(pipeline);

        var connectionFactory = new DefaultBoltConnectionFactory(
                schedulerProvider, config, logService, Clock.systemUTC(), new Monitors());
        connectionFactory.newConnection(boltChannel, stateMachine, messageWriter);

        verify(channel).pipeline();
        verify(pipeline).addLast(Mockito.any(KeepAliveHandler.class));
        verifyNoMoreInteractions(pipeline);
    }
}
