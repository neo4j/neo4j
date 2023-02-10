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
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.v40.messaging.request.BeginMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.memory.HeapEstimator;

/**
 * The READY state indicates that the connection is ready to accept a new RUN request. This is the "normal" state for a connection and becomes available after
 * successful authorisation and when not executing another statement. It is this that ensures that statements must be executed in series and each must wait for
 * the previous statement to complete.
 */
public class ReadyState extends FailSafeState {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ReadyState.class);

    protected State streamingState;
    protected State txReadyState;

    public void setStreamingState(State streamingState) {
        this.streamingState = streamingState;
    }

    public void setTransactionReadyState(State txReadyState) {
        this.txReadyState = txReadyState;
    }

    @Override
    public String name() {
        return "READY";
    }

    @Override
    protected void assertInitialized() {
        checkState(streamingState != null, "Streaming state not set");
        checkState(txReadyState != null, "TransactionReady state not set");
        super.assertInitialized();
    }

    @Override
    public State processUnsafe(RequestMessage message, StateMachineContext context) throws Exception {
        assertInitialized();

        if (message instanceof RunMessage runMessage) {
            return processRunMessage(runMessage, context);
        }
        if (message instanceof BeginMessage beginMessage) {
            return processBeginMessage(beginMessage, context);
        }

        return null;
    }

    @SuppressWarnings("removal")
    protected State processRunMessage(RunMessage message, StateMachineContext context) throws Exception {
        long start = context.clock().millis();

        var tx = context.connection()
                .beginTransaction(
                        TransactionType.IMPLICIT,
                        message.databaseName(),
                        message.getAccessMode(),
                        message.bookmarks(),
                        message.transactionTimeout(),
                        message.transactionMetadata());
        var statement = tx.run(message.statement(), message.params());

        long end = context.clock().millis();

        context.connectionState()
                .getResponseHandler()
                .onStatementPrepared(TransactionType.IMPLICIT, statement.id(), end - start, statement.fieldNames());

        // TODO: Remove along with ENTER_STREAMING
        context.connection().write(StateSignal.ENTER_STREAMING);

        return streamingState;
    }

    @SuppressWarnings("removal")
    protected State processBeginMessage(BeginMessage message, StateMachineContext context) throws Exception {
        var type = message.type();
        if (type == null) {
            type = TransactionType.EXPLICIT;
        }

        context.connection()
                .beginTransaction(
                        type,
                        message.databaseName(),
                        message.getAccessMode(),
                        message.bookmarks(),
                        message.transactionTimeout(),
                        message.transactionMetadata());

        // TODO: Remove along with ENTER_STREAMING
        context.connection().write(StateSignal.ENTER_STREAMING);

        return txReadyState;
    }

    protected State processLogoffMessage(StateMachineContext context) {
        return null;
    }
}
