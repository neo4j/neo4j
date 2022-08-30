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
package org.neo4j.bolt.protocol.v40.fsm;

import static org.neo4j.util.Preconditions.checkState;

import java.util.Collections;
import java.util.Map;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.HelloMessage;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Following the socket connection and a small handshake exchange to establish protocol version, the machine begins in the CONNECTED state. The <em>only</em>
 * valid transition from here is through a correctly authorised HELLO into the READY state. Any other action results in disconnection.
 */
public class ConnectedState implements State {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ConnectedState.class);

    private static final String CONNECTION_ID_KEY = "connection_id";

    private State readyState;

    @Override
    public State process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        checkState(readyState != null, "Ready state not set");

        if (message instanceof HelloMessage helloMessage) {
            var userAgent = helloMessage.userAgent();
            var authToken = helloMessage.authToken();
            var routingContext = extractRoutingContext(helloMessage);

            if (processAuthentication(userAgent, authToken, context)) {
                context.initStatementProcessorProvider(routingContext);

                context.connectionState().onMetadata(CONNECTION_ID_KEY, Values.utf8Value(context.connectionId()));

                var connectionHints = new MapValueBuilder();
                context.connection().connector().connectionHintProvider().append(connectionHints);
                context.connectionState().onMetadata("hints", connectionHints.build());

                return readyState;
            } else {
                return null;
            }
        }
        return null;
    }

    protected RoutingContext extractRoutingContext(HelloMessage message) {
        // routing context is unsupported in 4.0
        return new RoutingContext(false, Collections.emptyMap());
    }

    protected boolean processAuthentication(
            String userAgent, Map<String, Object> authToken, StateMachineContext context)
            throws BoltConnectionFatality {
        try {
            var boltSpi = context.boltSpi();

            var connectionState = context.connectionState();
            connectionState.onMetadata("server", Values.utf8Value(boltSpi.version()));

            var flags = context.connection().authenticate(authToken, userAgent);
            if (flags != null) {
                connectionState.onMetadata(flags.name().toLowerCase(), Values.TRUE);
            }

            return true;
        } catch (Throwable t) {
            context.handleFailure(t, true);
            return false;
        }
    }

    @Override
    public String name() {
        return "CONNECTED";
    }

    public void setReadyState(State readyState) {
        this.readyState = readyState;
    }
}
