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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

/**
 * Provides a state transition implementation which permits the sequential chaining of multiple
 * transitions into a single composed transition.
 * <p />
 * Child transitions passed to this implementation will be invoked in the order of their occurrence
 * within the input list. As such, the last transition has authority over the resulting state.
 * <p />
 * When one transition fails to process a request, the overall transition fails immediately.
 *
 * @param <R> a transition type.
 */
final class SequentialStateTransition<R extends RequestMessage> extends AbstractStateTransition<R> {
    private final boolean passLastResult;
    private final List<StateTransition<? super R>> transitions;

    // TODO: Find common ancestor via reflection for simplified API?
    public SequentialStateTransition(
            Class<R> requestType, boolean passLastResult, List<StateTransition<? super R>> transitions) {
        super(requestType);

        if (transitions.size() < 2) {
            throw new IllegalArgumentException("Must provide at least two transitions");
        }

        this.passLastResult = passLastResult;
        this.transitions = new ArrayList<>(transitions);
    }

    public SequentialStateTransition(
            Class<R> requestType, boolean passLastResult, StateTransition<? super R>... transitions) {
        super(requestType);

        if (transitions.length < 2) {
            throw new IllegalArgumentException("Must provide at least two transitions");
        }

        this.passLastResult = passLastResult;
        this.transitions = Arrays.asList(transitions);
    }

    @Override
    public StateReference process(Context ctx, R message, ResponseHandler handler) throws StateMachineException {
        StateReference result = null;
        var it = this.transitions.iterator();
        do {
            var transition = it.next();
            var newResult = transition.process(ctx, message, handler);

            if (result == null || this.passLastResult) {
                result = newResult;
            }
        } while (it.hasNext());

        return result;
    }
}
