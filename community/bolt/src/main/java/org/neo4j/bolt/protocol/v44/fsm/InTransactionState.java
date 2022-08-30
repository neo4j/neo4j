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
package org.neo4j.bolt.protocol.v44.fsm;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.transaction.TransactionNotFoundException;
import org.neo4j.exceptions.KernelException;

public class InTransactionState extends org.neo4j.bolt.protocol.v40.fsm.InTransactionState {

    @Override
    protected State processCommitMessage(StateMachineContext context)
            throws KernelException, TransactionNotFoundException, AuthenticationException {
        try {
            return super.processCommitMessage(context);
        } finally {
            context.connection().impersonate(null);
        }
    }

    @Override
    protected State processRollbackMessage(StateMachineContext context)
            throws KernelException, TransactionNotFoundException, AuthenticationException {
        try {
            return super.processRollbackMessage(context);
        } finally {
            context.connection().impersonate(null);
        }
    }
}
