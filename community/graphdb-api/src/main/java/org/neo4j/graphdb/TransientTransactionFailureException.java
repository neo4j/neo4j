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
package org.neo4j.graphdb;

import static java.lang.String.format;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Indicates that a transaction couldn't complete successfully due to an intermediate failure.
 * <p>
 * A proper response to a caught exception of this type is to cancel the unit of work that produced this exception and retry the unit of work again, as a
 * whole.
 */
public class TransientTransactionFailureException extends TransientFailureException {
    private final Status status;

    @Deprecated
    public TransientTransactionFailureException(Status status, String message) {
        super(message);
        this.status = status;
    }

    public TransientTransactionFailureException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        super(gqlStatusObject, message);

        this.status = status;
    }

    @Deprecated
    public TransientTransactionFailureException(Status status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    protected TransientTransactionFailureException(
            ErrorGqlStatusObject gqlStatusObject, Status status, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);

        this.status = status;
    }

    public static TransientTransactionFailureException leaseExpired(
            String tokenRequest, int currentLeaseId, int requestLeaseId) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N08)
                .build();

        var errorMsg = format(
                "The lease used for %s has expired: [current lease id:%d, token request lease id:%d]",
                tokenRequest, currentLeaseId, requestLeaseId);
        return new TransientTransactionFailureException(gql, Status.Transaction.LeaseExpired, errorMsg);
    }

    public static TransientTransactionFailureException procedureRegistryIsBusy() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N21)
                .build();

        return new TransientTransactionFailureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "The procedure registry is busy. You may retry this operation.");
    }

    public static TransientTransactionFailureException procedureRegistryWasModified() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N21)
                .build();

        return new TransientTransactionFailureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "The procedure registry was modified by another transaction. You may retry this operation.");
    }

    public static TransientTransactionFailureException outdatedRead() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N27)
                .build();

        return new TransientTransactionFailureException(
                gql,
                Status.Transaction.Outdated,
                "The transaction read outdated data and cannot be recovered due to concurrent data modification. Retry the transaction.");
    }

    @Override
    public Status status() {
        return status;
    }
}
