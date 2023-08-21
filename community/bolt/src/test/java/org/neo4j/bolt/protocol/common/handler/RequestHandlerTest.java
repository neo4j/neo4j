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
package org.neo4j.bolt.protocol.common.handler;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.connection.Job;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;

class RequestHandlerTest {

    @Test
    void shouldEnqueueRequests() throws BoltConnectionFatality {
        var msg = new HelloMessage(
                "SomeAgent/1.0", Collections.emptyList(), new RoutingContext(true, emptyMap()), emptyMap());

        var channel = new EmbeddedChannel();
        var connection =
                ConnectionMockFactory.newFactory().attachTo(channel, new RequestHandler(NullLogProvider.getInstance()));

        channel.writeInbound(msg);

        var messageCaptor = ArgumentCaptor.forClass(RequestMessage.class);
        verify(connection).submit(messageCaptor.capture());

        var actualMsg = messageCaptor.getValue();
        assertThat(actualMsg).isNotNull().isSameAs(msg);
    }

    @Test
    void shouldEnqueueErrors() throws BoltConnectionFatality {
        var responseHandler = mock(ResponseHandler.class);
        var fsm = mock(StateMachine.class);

        var channel = new EmbeddedChannel();

        ConnectionMockFactory.newFactory()
                .withAnswer(mock -> mock.submit(ArgumentMatchers.<Job>any()), invocation -> {
                    var job = invocation.<Job>getArgument(0);
                    job.perform(fsm, responseHandler);
                    return null;
                })
                .attachTo(channel, new RequestHandler(NullLogProvider.getInstance()));

        channel.pipeline().fireExceptionCaught(new IllegalStructArgumentException("foo", "Something went wrong! :("));

        // invoked through mock
        verify(responseHandler).onFailure(any(Error.class));
        verifyNoInteractions(fsm);
    }
}
