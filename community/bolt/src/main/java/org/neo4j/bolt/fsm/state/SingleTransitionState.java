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
import org.neo4j.bolt.fsm.error.state.IllegalTransitionException;
import org.neo4j.bolt.fsm.state.transition.StateTransition;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

final class SingleTransitionState extends AbstractState {
    private final StateTransition<?> transition;

    SingleTransitionState(StateReference reference, StateTransition<?> transition) {
        super(reference);
        this.transition = transition;
    }

    @Override
    public Factory builderOf() {
        return State.builder(this.reference()).withTransition(this.transition);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public StateReference process(Context ctx, RequestMessage message, ResponseHandler handler)
            throws StateMachineException {
        var transition = (StateTransition) this.transition;
        if (!transition.requestType().isInstance(message)) {
            throw new IllegalTransitionException(this, message);
        }

        return transition.process(ctx, message, handler);
    }
}
