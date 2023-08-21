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
package org.neo4j.bolt.protocol.common.handler.messages;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.request.connection.ResetMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.CommitMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;

class ResetMessageHandlerTest {

    @Test
    void shouldInterruptStateMachine() {
        var log = new AssertableLogProvider();

        var channel = new EmbeddedChannel();
        var connection = ConnectionMockFactory.newFactory().attachTo(channel, new ResetMessageHandler(log));

        channel.writeInbound(ResetMessage.getInstance());

        verify(connection).interrupt();
        verifyNoMoreInteractions(connection);

        assertThat(log).forLevel(Level.DEBUG).containsMessages("Interrupted state machine");
    }

    @Test
    void shouldIgnoreUnrelatedMessages() {
        var log = new AssertableLogProvider();

        var channel = new EmbeddedChannel();
        var connection = ConnectionMockFactory.newFactory().attachTo(channel, new ResetMessageHandler(log));

        channel.writeInbound(CommitMessage.getInstance());

        verifyNoInteractions(connection);

        assertThat(log).doesNotHaveAnyLogs();
    }
}
