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
package org.neo4j.bolt.protocol.common.fsm.transition.ready;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.error.state.InternalStateTransitionException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.SimpleImpersonationStateTransition;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.dbms.routing.RoutingException;
import org.neo4j.dbms.routing.result.RoutingResultFormat;
import org.neo4j.values.virtual.MapValue;

/**
 * Handles the generation of routing tables.
 * <p />
 * Remains within the current state when executed successfully.
 */
public final class RouteStateTransition extends SimpleImpersonationStateTransition<RouteMessage> {
    private static final RouteStateTransition INSTANCE = new RouteStateTransition();

    private RouteStateTransition() {
        super(RouteMessage.class);
    }

    public static RouteStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    public StateReference doProcess(Context ctx, RouteMessage message, ResponseHandler handler)
            throws StateMachineException {
        var databaseName = message.getDatabaseName();
        if (databaseName == null) {
            // TODO: Since the home database may change throughout the lifetime of the
            //       connection, we will need to re-resolve the target database. Ideally we
            //       should always be aware of the target database.
            ctx.connection().resolveDefaultDatabase();
            databaseName = ctx.connection().selectedDefaultDatabase();
        }

        MapValue routingTable;
        try {
            var user = ctx.connection().username();
            var result = ctx.connection()
                    .connector()
                    .routingService()
                    .route(databaseName, user, message.getRequestContext());

            routingTable = RoutingResultFormat.buildMap(result);
        } catch (RoutingException ex) {
            throw new InternalStateTransitionException("Failed to retrieve routing table", ex);
        }

        handler.onRoutingTable(databaseName, routingTable);
        return ctx.state();
    }
}
