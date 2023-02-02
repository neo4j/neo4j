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
import org.neo4j.bolt.protocol.v40.fsm.FailSafeState;
import org.neo4j.bolt.protocol.v51.message.request.LogonMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;

public class AuthenticationState extends FailSafeState {
    public static long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(AuthenticationState.class);

    private State readyState;

    @Override
    public String name() {
        return "AUTHENTICATION";
    }

    @Override
    public State processUnsafe(RequestMessage message, StateMachineContext context) throws AuthenticationException {
        checkState(readyState != null, "Ready state not set");
        if (message instanceof LogonMessage logonMessage) {
            processLogonMessage(context, logonMessage);

            // if it succeeds go to ready state.
            return readyState;
        }

        return null;
    }

    protected void processLogonMessage(StateMachineContext context, LogonMessage message)
            throws AuthenticationException {
        var authToken = message.authToken();
        var flags = context.connection().logon(authToken);
        if (flags != null) {
            context.connectionState().onMetadata(flags.name().toLowerCase(), Values.TRUE);
        }
    }

    public void setReadyState(State readyState) {
        this.readyState = readyState;
    }
}
