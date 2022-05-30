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
import static org.neo4j.values.storable.Values.stringArray;

import java.util.UUID;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.v40.messaging.request.BeginMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;

/**
 * The READY state indicates that the connection is ready to accept a new RUN request. This is the "normal" state for a connection and becomes available after
 * successful authorisation and when not executing another statement. It is this that ensures that statements must be executed in series and each must wait for
 * the previous statement to complete.
 */
public class ReadyState extends FailSafeState {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(ReadyState.class);

    public static final String FIELDS_KEY = "fields";
    public static final String FIRST_RECORD_AVAILABLE_KEY = "t_first";

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
        var programId = UUID.randomUUID().toString();
        context.connectionState().setCurrentTransactionId(programId);
        var runResult = context.transactionManager()
                .runProgram(
                        programId,
                        context.getLoginContext(),
                        message.databaseName(),
                        message.statement(),
                        message.params(),
                        message.bookmarks(),
                        message.getAccessMode().equals(AccessMode.READ),
                        message.transactionMetadata(),
                        message.transactionTimeout(),
                        context.connectionId());
        long end = context.clock().millis();

        context.connectionState()
                .onMetadata(
                        FIELDS_KEY, stringArray(runResult.statementMetadata().fieldNames()));
        context.connectionState().onMetadata(FIRST_RECORD_AVAILABLE_KEY, Values.longValue(end - start));

        // TODO: Remove along with ENTER_STREAMING
        context.channel().rawChannel().write(StateSignal.ENTER_STREAMING);

        return streamingState;
    }

    @SuppressWarnings("removal")
    protected State processBeginMessage(BeginMessage message, StateMachineContext context) throws Exception {
        var transactionId = context.transactionManager()
                .begin(
                        context.getLoginContext(),
                        message.databaseName(),
                        message.bookmarks(),
                        message.getAccessMode().equals(AccessMode.READ),
                        message.transactionMetadata(),
                        message.transactionTimeout(),
                        context.connectionId());
        context.connectionState().setCurrentTransactionId(transactionId);

        // TODO: Remove along with ENTER_STREAMING
        context.channel().rawChannel().write(StateSignal.ENTER_STREAMING);

        return txReadyState;
    }
}
