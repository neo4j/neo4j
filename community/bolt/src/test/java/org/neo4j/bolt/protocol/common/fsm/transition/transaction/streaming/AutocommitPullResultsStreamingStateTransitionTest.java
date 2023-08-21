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

import org.mockito.InOrder;
import org.neo4j.bolt.protocol.common.message.request.streaming.PullMessage;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementException;

class AutocommitPullResultsStreamingStateTransitionTest
        extends AbstractStreamingStateTransitionTest<PullMessage, AutocommitPullStreamingStateTransition> {

    @Override
    protected AutocommitPullStreamingStateTransition getTransition() {
        return AutocommitPullStreamingStateTransition.getInstance();
    }

    @Override
    protected PullMessage createMessage(long statementId, long n) {
        return new PullMessage(n, statementId);
    }

    @Override
    protected void verifyInteractions(TestParameters parameters, InOrder inOrder) throws StatementException {
        inOrder.verify(this.statement).consume(this.responseHandler, parameters.n());
    }

    @Override
    protected void onStatementCompleted(InOrder inOrder, TestParameters parameters) throws TransactionException {
        if (parameters.completed()) {
            inOrder.verify(this.transaction).commit();
        }
    }
}
