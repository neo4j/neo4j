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
package org.neo4j.bolt.protocol.common.fsm.error;

import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;

public final class StatusTransactionStateTransitionException extends TransactionStateTransitionException
        implements HasStatus, ErrorGqlStatusObject {
    private final Status status;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    protected StatusTransactionStateTransitionException(TransactionException cause, Status status) {
        super(cause);
        this.status = status;
        this.gqlStatusObject = null;
        this.oldMessage = ErrorMessageHolder.getOldCauseMessage(cause);
    }

    protected StatusTransactionStateTransitionException(
            ErrorGqlStatusObject gqlStatusObject, TransactionException cause, Status status) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, ErrorMessageHolder.getOldCauseMessage(cause)), cause);
        this.status = status;
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = ErrorMessageHolder.getOldCauseMessage(cause);
    }

    @Override
    public String legacyMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return this.status;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
