/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connection;

class DefaultBoltConnectionFactoryTest {

    //    @Test
    //    void shouldInstallKeepAliveHandler() {
    //        var schedulerProvider = mock(BoltSchedulerProvider.class);
    //        var config = Config.newBuilder()
    //                .set(BoltConnector.connection_keep_alive_type, BoltConnector.KeepAliveRequestType.ALL)
    //                .build();
    //        var logService = mock(LogService.class);
    //
    //        var pipeline = mock(ChannelPipeline.class);
    //        var channel = mock(Channel.class, RETURNS_MOCKS);
    //        var protector = mock(ConnectionListener.class);
    //        var authentication = mock(Authentication.class);
    //        var memoryTracker = mock(MemoryTracker.class);
    //        var stateMachine = mock(StateMachine.class);
    //
    //        var boltChannel = new BoltChannel(
    //                "bolt123", "joffrey", channel, authentication, protector, ConnectionHintProvider.noop(),
    // memoryTracker);
    //
    //        when(channel.pipeline()).thenReturn(pipeline);
    //
    //        var connectionFactory = new DefaultBoltConnectionFactory(
    //                schedulerProvider, config, logService, Clock.systemUTC(), new Monitors());
    //        connectionFactory.newConnection(boltChannel, stateMachine);
    //
    //        verify(channel).pipeline();
    //        verify(pipeline).addLast(eq("keepAliveHandler"), any(KeepAliveHandler.class));
    //        verifyNoMoreInteractions(pipeline);
    //    }
}
