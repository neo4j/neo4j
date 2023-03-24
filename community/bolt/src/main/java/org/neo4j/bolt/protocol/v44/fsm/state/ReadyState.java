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
package org.neo4j.bolt.protocol.v44.fsm.state;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

public class ReadyState extends org.neo4j.bolt.protocol.v43.fsm.state.ReadyState {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ReadyState.class);

    @Override
    public State processUnsafe(RequestMessage message, StateMachineContext context) throws Exception {
        if (message instanceof RouteMessage || message instanceof RunMessage || message instanceof BeginMessage) {
            return super.processUnsafe(message, context);
        }

        return null;
    }

    @Override
    protected State processRouteMessage(RouteMessage message, StateMachineContext context) throws Exception {
        var routeMessage = (RouteMessage) message;
        context.connection().impersonate(routeMessage.impersonatedUser());

        try {
            return super.processRouteMessage(message, context);
        } finally {
            context.connection().impersonate(null);
        }
    }

    @Override
    protected void onRoutingTableReceived(StateMachineContext context, RouteMessage message, MapValue routingTable) {
        var databaseName = message.getDatabaseName();
        if (databaseName == null) {
            // TODO: we need to resolve default database here to handle the case where it has changed during connection
            //       lifetime. Ideally we are returned this as part of the routing table lookup.
            context.connection().resolveDefaultDatabase();
            databaseName = context.connection().selectedDefaultDatabase();
        }

        super.onRoutingTableReceived(
                context, message, routingTable.updatedWith("db", Values.stringValue(databaseName)));
    }

    @Override
    protected State processRunMessage(RunMessage message, StateMachineContext context) throws Exception {
        this.authenticateImpersonation(context, message.impersonatedUser());

        try {
            return super.processRunMessage(message, context);
        } finally {
            context.connection().impersonate(null);
        }
    }

    @Override
    protected State processBeginMessage(BeginMessage message, StateMachineContext context) throws Exception {
        this.authenticateImpersonation(context, message.impersonatedUser());

        return super.processBeginMessage(message, context);
    }

    /**
     * Authenticates the impersonation of a given target user and returns the associated login context which is used as a substitute for following operations.
     *
     * @param context  the state machine context.
     * @param username the desired target user.
     */
    private void authenticateImpersonation(StateMachineContext context, String username)
            throws AuthenticationException {
        context.connection().impersonate(username);
    }
}
