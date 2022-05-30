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
package org.neo4j.bolt.protocol.common.handler;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.protocol.common.connection.BoltConnection;
import org.neo4j.bolt.protocol.common.connection.Job;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.v41.message.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;

class RequestHandlerTest {

    @Test
    void shouldEnqueueRequests() throws BoltConnectionFatality {
        var msg = new HelloMessage(emptyMap(), new RoutingContext(true, emptyMap()), emptyMap());

        var connection = mock(BoltConnection.class);
        var responseHandler = mock(ResponseHandler.class);
        var fsm = mock(StateMachine.class);

        var captor = ArgumentCaptor.forClass(Job.class);

        doNothing().when(connection).enqueue(captor.capture());

        var channel = new EmbeddedChannel(new RequestHandler(connection, responseHandler));

        channel.writeInbound(msg);

        verify(connection).enqueue(any(Job.class));

        var job = captor.getValue();

        assertThat(job).isNotNull();

        job.perform(fsm);

        verify(fsm).process(msg, responseHandler);
    }

    @Test
    void shouldEnqueueErrors() throws BoltConnectionFatality {
        var msg = new HelloMessage(emptyMap(), new RoutingContext(true, emptyMap()), emptyMap());

        var connection = mock(BoltConnection.class);
        var responseHandler = mock(ResponseHandler.class);
        var fsm = mock(StateMachine.class);

        doAnswer(invocation -> {
                    var job = invocation.<Job>getArgument(0);
                    job.perform(fsm);
                    return null;
                })
                .when(connection)
                .enqueue(any(Job.class));

        var channel = new EmbeddedChannel(new RequestHandler(connection, responseHandler));

        channel.pipeline().fireExceptionCaught(new IllegalStructArgumentException("foo", "Something went wrong! :("));

        verify(connection).enqueue(any(Job.class));

        // invoked through mock
        verify(fsm).handleExternalFailure(any(Error.class), eq(responseHandler));
    }
}
