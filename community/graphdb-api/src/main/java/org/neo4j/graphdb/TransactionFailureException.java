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
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Signals that a transaction failed and has been rolled back.
 */
@PublicApi
public class TransactionFailureException extends RuntimeException implements Status.HasStatus, HasGqlStatusInfo {
    public final Status status;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    @Deprecated
    public TransactionFailureException(String message) {
        super(message);
        this.status = Status.Database.Unknown;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public TransactionFailureException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message));
        this.gqlStatusObject = gqlStatusObject;

        this.status = Status.Database.Unknown;
        this.oldMessage = message;
    }

    @Deprecated
    public TransactionFailureException(String message, Throwable cause) {
        super(message, cause);
        this.status = (cause instanceof Status.HasStatus se) ? se.status() : Status.Database.Unknown;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public TransactionFailureException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;

        this.status = (cause instanceof Status.HasStatus se) ? se.status() : Status.Database.Unknown;
        this.oldMessage = message;
    }

    public TransactionFailureException(String message, Status status) {
        super(message);
        this.status = status;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public TransactionFailureException(ErrorGqlStatusObject gqlStatusObject, String message, Status status) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message));
        this.gqlStatusObject = gqlStatusObject;

        this.status = status;
        this.oldMessage = message;
    }

    public TransactionFailureException(String message, Throwable cause, Status status) {
        super(message, cause);
        this.status = status;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public TransactionFailureException(
            ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause, Status status) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;

        this.status = status;
        this.oldMessage = message;
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
