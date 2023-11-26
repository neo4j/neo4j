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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionStatusFailureException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;

public class DefaultTransactionExceptionMapper implements TransactionExceptionMapper {
    private static final String UNABLE_TO_COMPLETE_TRANSACTION = "Unable to complete transaction.";

    public static final DefaultTransactionExceptionMapper INSTANCE = new DefaultTransactionExceptionMapper();

    private DefaultTransactionExceptionMapper() {}

    @Override
    public RuntimeException mapException(Exception e) {
        if (e instanceof TransientFailureException tfe) {
            // We let transient exceptions pass through unchanged since they aren't really transaction failures
            // in the same sense as unexpected failures are. Such exception signals that the transaction
            // can be retried and might be successful the next time.
            return tfe;
        } else if (e instanceof ConstraintViolationTransactionFailureException) {
            return new ConstraintViolationException(e.getMessage(), e);
        } else if (e instanceof Status.HasStatus) {
            Status status = ((Status.HasStatus) e).status();
            Status.Code statusCode = status.code();
            String statusExceptionMessage = UNABLE_TO_COMPLETE_TRANSACTION + ": " + statusCode.description();
            if (statusCode.classification() == Status.Classification.TransientError) {
                return new TransientTransactionFailureException(status, statusExceptionMessage, e);
            }
            return new TransactionStatusFailureException(status, statusExceptionMessage, e);
        } else {
            return new TransactionFailureException(UNABLE_TO_COMPLETE_TRANSACTION, e, Status.Database.Unknown);
        }
    }
}
