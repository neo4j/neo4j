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
package org.neo4j.bolt.protocol.common.fsm.transition.authentication;

import java.util.Locale;
import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.AbstractStateTransition;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.negotiation.HelloStateTransition;
import org.neo4j.bolt.protocol.common.message.request.authentication.AuthenticationMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.values.storable.Values;

/**
 * Handles the authentication of a connection via an instance of {@link AuthenticationMessage}.
 * <p />
 * This implementation may be used with the different authentication scheme present throughout the
 * supported set of Bolt versions. It should be combined with {@link HelloStateTransition} in
 * order to facilitate backwards compatibility where necessary.
 * <p />
 * Transitions to {@link States#READY} when successfully executed.
 */
public final class AuthenticationStateTransition extends AbstractStateTransition<AuthenticationMessage> {
    private static final AuthenticationStateTransition INSTANCE = new AuthenticationStateTransition();

    private AuthenticationStateTransition() {
        super(AuthenticationMessage.class);
    }

    public static AuthenticationStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    public StateReference process(Context ctx, AuthenticationMessage message, ResponseHandler handler)
            throws StateMachineException {
        AuthenticationFlag flags;
        try {
            flags = ctx.connection().logon(message.authToken());
        } catch (AuthenticationException ex) {
            throw new AuthenticationStateTransitionException(ex);
        }

        if (flags != null) {
            // TODO: Introduce a dedicated handler method?
            handler.onMetadata(flags.name().toLowerCase(Locale.ROOT), Values.TRUE);
        }

        // once authenticated the default state advances to READY so that a state machine reset
        // won't return it to the authentication phase (this effect is negated during logoff)
        ctx.defaultState(States.READY);
        return States.READY;
    }
}
