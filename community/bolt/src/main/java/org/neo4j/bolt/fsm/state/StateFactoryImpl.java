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

import java.util.HashMap;
import java.util.Map;
import org.neo4j.bolt.fsm.state.State.Factory;
import org.neo4j.bolt.fsm.state.transition.StateTransition;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

final class StateFactoryImpl implements State.Factory {
    private final StateReference reference;
    private final Map<Class<? extends RequestMessage>, StateTransition<?>> transitions = new HashMap<>();

    StateFactoryImpl(StateReference reference) {
        this.reference = reference;
    }

    @Override
    public StateReference reference() {
        return this.reference;
    }

    @Override
    public State build() {
        if (this.transitions.isEmpty()) {
            return new NoopState(this.reference);
        }

        if (this.transitions.size() == 1) {
            var transition = this.transitions.values().iterator().next();
            return new SingleTransitionState(this.reference, transition);
        }

        var factory = HandlerRegistry.<RequestMessage, StateTransition<?>>builder();
        this.transitions.forEach(factory::register);

        return new MultiTransitionState(this.reference, factory.build());
    }

    @Override
    public Factory withTransition(StateTransition<?> transition) {
        this.transitions.put(transition.requestType(), transition);
        return this;
    }

    @Override
    public Factory withoutTransition(Class<? extends RequestMessage> requestType) {
        this.transitions.remove(requestType);
        return this;
    }
}
