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

package org.neo4j.bolt.protocol.v51.fsm;

import static org.neo4j.util.Preconditions.checkState;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.fsm.ConnectedState;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.protocol.v51.message.request.HelloMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Following the socket connection and a small handshake exchange to establish protocol version, the machine begins in the CONNECTED state. The <em>only</em>
 * valid transition from here is through a correctly authorised HELLO into the READY state. Any other action results in disconnection.
 */
public class NegotiationState implements State {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(NegotiationState.class);
    private static final String CONNECTION_ID_KEY = ConnectedState.CONNECTION_ID_KEY;

    protected State authenticationState;

    @Override
    public String name() {
        return "NEGOTIATION";
    }

    /**
     * WARNING: This is now Pre-auth as of v51 so be carefully what is returned from here.
     * @param message an arbitrary request.
     * @param context the context in which this request is handled.
     * @return The only valid state from Connected is Authentication.
     * @throws BoltConnectionFatality
     */
    @Override
    public State process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        checkState(authenticationState != null, "Authentication state not set");

        if (message instanceof HelloMessage helloMessage) {
            var routingContext = extractRoutingContext(helloMessage);

            var enabledFeatures =
                    context.connection().negotiate(helloMessage.features(), helloMessage.userAgent(), routingContext);

            var connectionState = context.connectionState();
            var boltSpi = context.boltSpi();

            connectionState.onMetadata(CONNECTION_ID_KEY, Values.utf8Value(context.connectionId()));
            connectionState.onMetadata("server", Values.utf8Value(boltSpi.version()));

            var connectionHints = new MapValueBuilder();
            context.connection().connector().connectionHintProvider().append(connectionHints);
            connectionState.onMetadata("hints", connectionHints.build());

            if (!enabledFeatures.isEmpty()) {
                var builder = ListValueBuilder.newListBuilder(enabledFeatures.size());
                enabledFeatures.forEach(feature -> builder.add(Values.stringValue(feature.getId())));

                context.connectionState().onMetadata("patch_bolt", builder.build());
            }

            return authenticationState;
        }
        return null;
    }

    protected RoutingContext extractRoutingContext(HelloMessage message) {
        return message.routingContext();
    }

    public void setAuthenticationState(State authenticationState) {
        this.authenticationState = authenticationState;
    }
}
