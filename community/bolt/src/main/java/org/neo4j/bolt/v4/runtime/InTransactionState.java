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
package org.neo4j.bolt.v4.runtime;

import static org.neo4j.bolt.v3.runtime.ReadyState.FIELDS_KEY;
import static org.neo4j.bolt.v3.runtime.ReadyState.FIRST_RECORD_AVAILABLE_KEY;
import static org.neo4j.values.storable.Values.stringArray;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.transaction.TransactionNotFoundException;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.exceptions.KernelException;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;

public class InTransactionState extends AbstractStreamingState {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(InTransactionState.class);

    public static final String QUERY_ID_KEY = "qid";

    @Override
    protected BoltStateMachineState processUnsafe(RequestMessage message, StateMachineContext context)
            throws Throwable {
        context.connectionState().ensureNoPendingTerminationNotice();

        if (message instanceof RunMessage) {
            return processRunMessage((RunMessage) message, context);
        }
        if (message instanceof CommitMessage) {
            return processCommitMessage(context);
        }
        if (message instanceof RollbackMessage) {
            return processRollbackMessage(context);
        } else {
            return super.processUnsafe(message, context);
        }
    }

    @Override
    public String name() {
        return "IN_TRANSACTION";
    }

    @Override
    protected BoltStateMachineState processStreamPullResultMessage(
            int statementId, ResultConsumer resultConsumer, StateMachineContext context, long noToPull)
            throws Throwable {
        context.getTransactionManager()
                .pullData(context.connectionState().getCurrentTransactionId(), statementId, noToPull, resultConsumer);
        return this;
    }

    @Override
    protected BoltStateMachineState processStreamDiscardResultMessage(
            int statementId, ResultConsumer resultConsumer, StateMachineContext context, long noToDiscard)
            throws Throwable {
        context.getTransactionManager()
                .discardData(
                        context.connectionState().getCurrentTransactionId(), statementId, noToDiscard, resultConsumer);
        return this;
    }

    private BoltStateMachineState processRunMessage(RunMessage message, StateMachineContext context)
            throws KernelException, TransactionNotFoundException {
        context.connectionState().ensureNoPendingTerminationNotice();
        long start = context.clock().millis();
        var metadata = context.getTransactionManager()
                .runQuery(context.connectionState().getCurrentTransactionId(), message.statement(), message.params());
        long end = context.clock().millis();

        context.connectionState().onMetadata(FIELDS_KEY, stringArray(metadata.fieldNames()));
        context.connectionState().onMetadata(FIRST_RECORD_AVAILABLE_KEY, Values.longValue(end - start));
        context.connectionState().onMetadata(QUERY_ID_KEY, Values.longValue(metadata.queryId()));

        return this;
    }

    protected BoltStateMachineState processCommitMessage(StateMachineContext context)
            throws KernelException, TransactionNotFoundException {
        Bookmark bookmark =
                context.getTransactionManager().commit(context.connectionState().getCurrentTransactionId());
        context.connectionState().clearCurrentTransactionId();
        bookmark.attachTo(context.connectionState());
        return readyState;
    }

    protected BoltStateMachineState processRollbackMessage(StateMachineContext context)
            throws KernelException, TransactionNotFoundException {
        context.getTransactionManager().rollback(context.connectionState().getCurrentTransactionId());
        context.connectionState().clearCurrentTransactionId();
        return readyState;
    }
}
