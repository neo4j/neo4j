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
package org.neo4j.bolt.protocol.common.fsm.transition.transaction.streaming;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.streaming.DiscardMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.statement.Statement;

public final class DiscardResultsStreamingStateTransition extends StreamingStateTransition<DiscardMessage> {
    private static final DiscardResultsStreamingStateTransition INSTANCE = new DiscardResultsStreamingStateTransition();

    private DiscardResultsStreamingStateTransition() {
        super(DiscardMessage.class);
    }

    public static DiscardResultsStreamingStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    protected void process(Context ctx, Transaction tx, Statement statement, long noToProcess, ResponseHandler handler)
            throws StateMachineException, TransactionException {
        statement.discard(handler, noToProcess);
    }
}
