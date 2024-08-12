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
package org.neo4j.bolt.fsm.error.state;

import org.neo4j.bolt.fsm.error.ConnectionTerminating;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;
import org.neo4j.kernel.api.exceptions.Status.Request;

/**
 * Represents an error case in which a state machine does not define a transition for the desired
 * request and is thus unable to handle it.
 */
public class IllegalTransitionException extends IllegalRequestException implements HasStatus, ConnectionTerminating {
    private final State state;
    private final RequestMessage request;

    protected IllegalTransitionException(State state, RequestMessage request, String message, Throwable cause) {
        super(message, cause);
        this.state = state;
        this.request = request;
    }

    protected IllegalTransitionException(
            ErrorGqlStatusObject gqlStatusObject,
            State state,
            RequestMessage request,
            String message,
            Throwable cause) {
        super(gqlStatusObject, message, cause);

        this.state = state;
        this.request = request;
    }

    public IllegalTransitionException(State state, RequestMessage request, Throwable cause) {
        this(
                state,
                request,
                "Message of type " + request.getClass().getSimpleName() + " cannot be handled by a session in the "
                        + state.name() + " state.",
                cause);
    }

    public IllegalTransitionException(
            ErrorGqlStatusObject gqlStatusObject, State state, RequestMessage request, Throwable cause) {
        this(
                gqlStatusObject,
                state,
                request,
                "Message of type " + request.getClass().getSimpleName() + " cannot be handled by a session in the "
                        + state.name() + " state.",
                cause);
    }

    public IllegalTransitionException(State state, RequestMessage request) {
        this(state, request, null);
    }

    public IllegalTransitionException(ErrorGqlStatusObject gqlStatusObject, State state, RequestMessage request) {
        this(gqlStatusObject, state, request, null);
    }

    @Override
    public Status status() {
        return Request.Invalid;
    }

    public State getState() {
        return this.state;
    }

    public RequestMessage getRequest() {
        return this.request;
    }
}
