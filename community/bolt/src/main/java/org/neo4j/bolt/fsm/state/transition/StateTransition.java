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
package org.neo4j.bolt.fsm.state.transition;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

/**
 * Encapsulates the necessary logic for transitioning a state machine from one of its states to
 * another along with all the associated side effects.
 *
 * @param <R> the request type which triggers this transition.
 */
public interface StateTransition<R extends RequestMessage> {

    /**
     * Retrieves the request message type which is accepted by this state transition.
     *
     * @return a request type.
     */
    Class<R> requestType();

    /**
     * Handles the state transition and associated side effects of a given request.
     *
     * @param message a request message.
     * @param handler a response handler via which information is passed back to the
     *                requesting party.
     * @return a follow-up state.
     * @throws StateMachineException when the transition fails to execute with the given request.
     */
    StateReference process(Context ctx, R message, ResponseHandler handler) throws StateMachineException;

    /**
     * Chains another transition at the end of this transition.
     *
     * @param transitions one or more transitions to chain.
     * @return a composed transition.
     * @see SequentialStateTransition
     */
    @SuppressWarnings("unchecked")
    default StateTransition<R> andThen(StateTransition<? super R>... transitions) {
        if (transitions.length == 0) {
            return this;
        }
        if (transitions.length == 1) {
            return new SequentialStateTransition<R>(this.requestType(), true, this, transitions[0]);
        }

        @SuppressWarnings("unchecked")
        StateTransition<? super R>[] chain = new StateTransition[transitions.length + 1];
        chain[0] = this;
        System.arraycopy(transitions, 0, chain, 1, transitions.length);

        return new SequentialStateTransition<R>(this.requestType(), true, chain);
    }

    /**
     * Chains another transition at the end of this transition.
     *
     * @param transitions one or more transitions to chain.
     * @return a composed transition.
     * @see SequentialStateTransition
     */
    @SuppressWarnings("unchecked")
    default StateTransition<R> also(StateTransition<? super R>... transitions) {
        if (transitions.length == 0) {
            return this;
        }
        if (transitions.length == 1) {
            return new SequentialStateTransition<R>(this.requestType(), false, this, transitions[0]);
        }

        @SuppressWarnings("unchecked")
        StateTransition<? super R>[] chain = new StateTransition[transitions.length + 1];
        chain[0] = this;
        System.arraycopy(transitions, 0, chain, 1, transitions.length);

        return new SequentialStateTransition<R>(this.requestType(), false, chain);
    }
}
