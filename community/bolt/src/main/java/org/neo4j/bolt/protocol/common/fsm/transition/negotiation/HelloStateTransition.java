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
package org.neo4j.bolt.protocol.common.fsm.transition.negotiation;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.AbstractStateTransition;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.AuthenticationStateTransition;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.kernel.internal.Version;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Handles negotiation of optional protocol capabilities as well as client functionality.
 * <p />
 * This implementation should be used together with {@link AuthenticationStateTransition} in order
 * to facilitate backwards compatibility where necessary.
 * <p />
 * Transitions to {@link States#AUTHENTICATION} when successfully executed.
 */
public final class HelloStateTransition extends AbstractStateTransition<HelloMessage> {
    private static final HelloStateTransition INSTANCE = new HelloStateTransition();

    private HelloStateTransition() {
        super(HelloMessage.class);
    }

    public static HelloStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    public StateReference process(Context ctx, HelloMessage message, ResponseHandler handler)
            throws StateMachineException {
        var features = message.features();
        var userAgent = message.userAgent();
        var routingContext = message.routingContext();
        var notificationsConfig = message.notificationsConfig();
        var boltAgent = message.boltAgent();

        var enabledFeatures =
                ctx.connection().negotiate(features, userAgent, routingContext, notificationsConfig, boltAgent);
        if (!enabledFeatures.isEmpty()) {
            var builder = ListValueBuilder.newListBuilder(enabledFeatures.size());
            enabledFeatures.forEach(feature -> builder.add(Values.stringValue(feature.getId())));

            handler.onMetadata("patch_bolt", builder.build());
        }

        // TODO: Introduce dedicated handler methods?
        handler.onMetadata("connection_id", Values.stringValue(ctx.connection().id()));
        handler.onMetadata("server", Values.stringValue("Neo4j/" + Version.getNeo4jVersion()));

        var connectionHints = new MapValueBuilder();
        ctx.connection().connector().connectionHintProvider().append(connectionHints);
        handler.onMetadata("hints", connectionHints.build());

        // advance the default state to authentication so that negotiation does not happen again
        // if the state machine is reset while in this state (the default state will advance once
        // again when authentication is completed)
        ctx.defaultState(States.AUTHENTICATION);
        return States.AUTHENTICATION;
    }
}
