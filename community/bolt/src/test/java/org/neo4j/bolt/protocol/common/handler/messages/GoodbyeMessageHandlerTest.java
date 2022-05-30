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
package org.neo4j.bolt.protocol.common.handler.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connection.BoltConnection;
import org.neo4j.bolt.protocol.v40.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.ResetMessage;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.logging.Log;

class GoodbyeMessageHandlerTest {

    @Test
    void shouldStopConnection() {
        var connection = mock(BoltConnection.class);
        var log = new AssertableLogProvider(true);

        var channel =
                new EmbeddedChannel(new GoodbyeMessageHandler(connection, log.getLog(GoodbyeMessageHandler.class)));

        channel.writeInbound(GoodbyeMessage.INSTANCE);

        assertThat(channel.<Object>readInbound()).isNull();

        verify(connection).stop();

        assertThat(log)
                .forLevel(Level.DEBUG)
                .forClass(GoodbyeMessageHandler.class)
                .containsMessageWithArguments("Stopping connection %s due to client request", channel.remoteAddress());
    }

    @Test
    void shouldIgnoreOtherMessageTypes() {
        var connection = mock(BoltConnection.class);
        var log = mock(Log.class);

        var channel = new EmbeddedChannel(new GoodbyeMessageHandler(connection, log));

        channel.writeInbound(ResetMessage.INSTANCE);

        assertThat(channel.<Object>readInbound()).isSameAs(ResetMessage.INSTANCE);

        verifyNoInteractions(connection, log);
    }
}
