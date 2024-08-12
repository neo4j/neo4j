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

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Signals that the transaction within which the failed operations ran
 * has been terminated with {@link Transaction#terminate()}.
 */
@PublicApi
public class TransactionTerminatedException extends TransactionFailureException implements Status.HasStatus {
    private final Status status;

    public TransactionTerminatedException(Status status) {
        this(status, "");
    }

    public TransactionTerminatedException(ErrorGqlStatusObject gqlStatusObject, Status status) {
        this(gqlStatusObject, status, "");
    }

    protected TransactionTerminatedException(Status status, String additionalInfo) {
        super(
                "The transaction has been terminated. Retry your operation in a new transaction, "
                        + "and you should see a successful result. "
                        + status.code().description() + " " + additionalInfo,
                status);
        this.status = status;
    }

    protected TransactionTerminatedException(
            ErrorGqlStatusObject gqlStatusObject, Status status, String additionalInfo) {
        super(
                gqlStatusObject,
                "The transaction has been terminated. Retry your operation in a new transaction, "
                        + "and you should see a successful result. "
                        + status.code().description() + " " + additionalInfo,
                status);

        this.status = status;
    }

    @Override
    public Status status() {
        return status;
    }
}
