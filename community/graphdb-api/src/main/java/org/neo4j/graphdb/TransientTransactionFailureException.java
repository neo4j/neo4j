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

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
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

    public TransientTransactionFailureException(
            ErrorGqlStatusObject gqlStatusObject, Status status, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);

        this.status = status;
    }

    @Override
    public Status status() {
        return status;
    }
}
