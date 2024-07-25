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
package org.neo4j.kernel.impl.util;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

public class ReadAndDeleteTransactionConflictException extends RuntimeException
        implements Status.HasStatus, HasGqlStatusInfo {
    private static final String CONCURRENT_DELETE_MESSAGE =
            "Database elements (nodes, relationships, properties) were observed during query execution, "
                    + "but got deleted by an overlapping committed transaction before the query results could be serialised. "
                    + "The transaction might succeed if it is retried.";
    private static final String DELETED_IN_TRANSACTION_MESSAGE =
            "Database elements (nodes, relationships, properties) were deleted in this transaction, "
                    + "but were also included in the result set.";

    private final boolean deletedInThisTransaction;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    @Deprecated
    public ReadAndDeleteTransactionConflictException(boolean deletedInThisTransaction) {
        super(getMessageHelper(deletedInThisTransaction));
        this.deletedInThisTransaction = deletedInThisTransaction;

        this.gqlStatusObject = null;
        this.oldMessage = getMessageHelper(deletedInThisTransaction);
    }

    public ReadAndDeleteTransactionConflictException(
            ErrorGqlStatusObject gqlStatusObject, boolean deletedInThisTransaction) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, getMessageHelper(deletedInThisTransaction)));
        this.gqlStatusObject = gqlStatusObject;

        this.deletedInThisTransaction = deletedInThisTransaction;
        this.oldMessage = getMessageHelper(deletedInThisTransaction);
    }

    @Deprecated
    public ReadAndDeleteTransactionConflictException(boolean deletedInThisTransaction, Throwable cause) {
        super(getMessageHelper(deletedInThisTransaction), cause);
        this.deletedInThisTransaction = deletedInThisTransaction;

        this.gqlStatusObject = null;
        this.oldMessage = getMessageHelper(deletedInThisTransaction);
    }

    public ReadAndDeleteTransactionConflictException(
            ErrorGqlStatusObject gqlStatusObject, boolean deletedInThisTransaction, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, getMessageHelper(deletedInThisTransaction)), cause);
        this.gqlStatusObject = gqlStatusObject;

        this.deletedInThisTransaction = deletedInThisTransaction;
        this.oldMessage = getMessageHelper(deletedInThisTransaction);
    }

    public boolean wasDeletedInThisTransaction() {
        return deletedInThisTransaction;
    }

    private static String getMessageHelper(boolean deletedInThisTransaction) {
        return deletedInThisTransaction ? DELETED_IN_TRANSACTION_MESSAGE : CONCURRENT_DELETE_MESSAGE;
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return deletedInThisTransaction ? Status.Statement.EntityNotFound : Status.Transaction.Outdated;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
