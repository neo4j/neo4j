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
package org.neo4j.bolt.protocol.v40.fsm;

import static org.neo4j.util.Preconditions.checkState;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.result.ResultConsumer;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.v40.messaging.request.DiscardMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.PullMessage;
import org.neo4j.bolt.protocol.v40.messaging.result.DiscardResultConsumer;
import org.neo4j.bolt.protocol.v40.messaging.result.PullResultConsumer;

/**
 * When STREAMING, a result is available as a stream of records. These must be PULLed or DISCARDed before any further statements can be executed.
 */
public abstract class AbstractStreamingState extends FailSafeState {
    protected State readyState;

    @Override
    @SuppressWarnings("removal")
    protected State processUnsafe(RequestMessage message, StateMachineContext context) throws Throwable {
        context.connectionState().ensureNoPendingTerminationNotice();

        State nextState = null;
        if (message instanceof PullMessage pullMessage) {
            nextState = processStreamPullResultMessage(
                    pullMessage.statementId(),
                    new PullResultConsumer(context, pullMessage.n()),
                    context,
                    pullMessage.n());
        } else if (message instanceof DiscardMessage discardMessage) {
            nextState = processStreamDiscardResultMessage(
                    discardMessage.statementId(),
                    new DiscardResultConsumer(context, discardMessage.n()),
                    context,
                    discardMessage.n());
        }

        // TODO: Remove along with EXIT_STREAMING
        if (nextState != this) {
            context.channel().rawChannel().write(StateSignal.EXIT_STREAMING);
        }

        return nextState;
    }

    public void setReadyState(State readyState) {
        this.readyState = readyState;
    }

    protected abstract State processStreamPullResultMessage(
            int statementId, ResultConsumer resultConsumer, StateMachineContext context, long numberToPull)
            throws Throwable;

    protected abstract State processStreamDiscardResultMessage(
            int statementId, ResultConsumer resultConsumer, StateMachineContext context, long noToDiscard)
            throws Throwable;

    @Override
    protected void assertInitialized() {
        checkState(readyState != null, "Ready state not set");
        super.assertInitialized();
    }
}
