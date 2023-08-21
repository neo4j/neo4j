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
package org.neo4j.bolt.fsm.state;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.transition.StateTransition;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

public interface State {

    /**
     * Creates a new state factory for a given reference.
     *
     * @param reference a unique state reference.
     * @return a new empty state factory.
     */
    static Factory builder(StateReference reference) {
        return new StateFactoryImpl(reference);
    }

    /**
     * Creates a state factory which mimics the set of transitions registered within this state.
     *
     * @return a new state factory.
     */
    Factory builderOf();

    /**
     * Retrieves a dedicated reference object via which a given state may be referenced for the
     * purposes of transitioning.
     *
     * @return a state reference.
     */
    StateReference reference();

    /**
     * Retrieves a human-readable name via which this state is identified.
     * <p />
     * This value is primarily provided for debugging, logging or restore operations in which a
     * state machine immediately transitions to a designated state instead of its initial state.
     *
     * @return a human-readable state identification.
     */
    default String name() {
        return this.reference().name();
    }

    /**
     * Handles a given message.
     * @param ctx a state machine instance context.
     * @param message a request.
     * @param handler a response handler.
     * @return a state to transition the state machine to as a result of handling the given request.
     * @throws StateMachineException when the state cannot or fails to transition.
     */
    StateReference process(Context ctx, RequestMessage message, ResponseHandler handler) throws StateMachineException;

    interface Factory {

        /**
         * Retrieves the state reference via which this state shall be identified throughout the
         * duration of its life.
         *
         * @return a state reference.
         */
        StateReference reference();

        /**
         * Constructs a new state using the transitions present within this factory instance.
         *
         * @return a state.
         */
        State build();

        /**
         * Registers a transition for its respective request type.
         * <p />
         * When a transition for the desired request type already exists, it will be replaced with
         * the given transition.
         *
         * @param transition a transition.
         * @return a reference to this factory.
         */
        Factory withTransition(StateTransition<?> transition);

        /**
         * Removes a transition based on its respective request type.
         *
         * @param requestType a request type.
         * @return a reference to this factory.
         */
        Factory withoutTransition(Class<? extends RequestMessage> requestType);
    }
}
