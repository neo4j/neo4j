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
package org.neo4j.bolt.protocol.common.message;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.neo4j.bolt.fsm.error.BoltException;
import org.neo4j.bolt.fsm.error.ConnectionTerminating;
import org.neo4j.bolt.protocol.common.message.response.FailureMessage;
import org.neo4j.bolt.protocol.common.message.response.FailureMetadata;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.api.exceptions.HasQuery;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * An error object, represents something having gone wrong that is to be signaled to the user. This is, by design, not using the java exception system.
 *
 * This object is responsible for centralise the order related to errors like default values from fields expected fields
 * on the {@link org.neo4j.bolt.protocol.common.message.response.FailureMessage}, detecting fatal errors, and so on.
 *
 */
public class Error {
    private static final Map<String, Object> DEFAULT_DIAGNOSTIC_RECORD =
            DiagnosticRecord.from().build().asMap();

    private final Status status;
    private final String message;
    private final Throwable cause;
    private final Throwable wrappedThrowable;
    private final UUID reference;
    private final boolean fatal;
    private final Long queryId;

    private Error(
            Status status, String message, Throwable cause, boolean fatal, Long queryId, Throwable wrappedThrowable) {
        this.status = status;
        this.message = message;
        this.cause = cause;
        this.fatal = fatal;
        this.reference = UUID.randomUUID();
        this.queryId = queryId;
        this.wrappedThrowable = wrappedThrowable;
    }

    private Error(Status status, String message, boolean fatal) {
        this(status, message, null, fatal, null, null);
    }

    private Error(Status status, Throwable cause, boolean fatal, Long queryId) {
        this(status, status.code().description(), cause, fatal, queryId, cause);
    }

    public Status status() {
        return status;
    }

    public String message() {
        return message;
    }

    public Throwable cause() {
        return cause;
    }

    public UUID reference() {
        return reference;
    }

    public Long queryId() {
        return queryId;
    }

    public Throwable wrappedThrowable() {
        return wrappedThrowable;
    }

    /**
     * Generates the Protocol with all fields expected.
     *
     * The serializer is responsible for selecting the data of
     * interest.
     *
     * @return
     */
    public FailureMessage asBoltMessage() {
        if (wrappedThrowable instanceof ErrorGqlStatusObject wrapped) {
            return new FailureMessage(
                    new FailureMetadata(
                            this.status(),
                            this.message(),
                            wrapped.statusDescription(),
                            wrapped.gqlStatus(),
                            wrapped.diagnosticRecord(),
                            wrapped.cause().map(Error::causeAsFailureMetadata).orElse(null)),
                    this.isFatal());
        }
        return new FailureMessage(
                new FailureMetadata(
                        this.status(),
                        this.message(),
                        ErrorGqlStatusObject.DEFAULT_STATUS_DESCRIPTION,
                        ErrorGqlStatusObject.DEFAULT_STATUS_CODE,
                        DEFAULT_DIAGNOSTIC_RECORD,
                        null),
                this.isFatal());
    }

    private static FailureMetadata causeAsFailureMetadata(ErrorGqlStatusObject error) {

        Status status = Status.General.UnknownError;
        if (error instanceof Status.HasStatus errorWithStatus) {
            status = errorWithStatus.status();
        }
        return new FailureMetadata(
                status,
                error.getMessage(),
                error.statusDescription(),
                error.gqlStatus(),
                error.diagnosticRecord(),
                error.cause().map(Error::causeAsFailureMetadata).orElse(null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Error error = (Error) o;
        return Objects.equals(status, error.status)
                && Objects.equals(message, error.message)
                && Objects.equals(cause, error.cause)
                && Objects.equals(wrappedThrowable, error.wrappedThrowable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, message, cause, wrappedThrowable);
    }

    @Override
    public String toString() {
        return "Neo4jError{" + "status="
                + status + ", message='"
                + message + '\'' + ", cause="
                + cause + ", wrappedThrowable="
                + wrappedThrowable + '}';
    }

    /**
     * This function is deprecated because it does not include any GQL status information.
     * Use the method that takes a Throwable instead.
     */
    @Deprecated
    public static Error from(Status status, String message) {
        return new Error(status, message, false);
    }

    public static Error from(Throwable any) {
        return from(any, false);
    }

    public static Error from(Throwable any, boolean fatal) {

        for (Throwable cause = any; cause != null; cause = cause.getCause()) {
            Long queryId = null;
            if (cause instanceof ConnectionTerminating) {
                fatal = true;
            }
            if (cause instanceof HasQuery) {
                queryId = ((HasQuery) cause).query();
            }
            if (cause instanceof DatabaseShutdownException) {
                return new Error(
                        Status.General.DatabaseUnavailable,
                        // Change
                        //   Status.General.DatabaseUnavailable.code().description()
                        // to
                        //   cause.getMessage()
                        // once the GQL constructors of DatabaseShutdownException are used
                        Status.General.DatabaseUnavailable.code().description(),
                        any,
                        fatal,
                        queryId,
                        cause);
            }
            if (cause instanceof Status.HasStatus) {
                return new Error(((Status.HasStatus) cause).status(), cause.getMessage(), any, fatal, queryId, cause);
            }
            if (cause instanceof OutOfMemoryError) {
                return new Error(
                        Status.General.OutOfMemoryError,
                        cause.getMessage(),
                        any,
                        fatal,
                        queryId,
                        BoltException.outOfMemory(any));
            }
            if (cause instanceof StackOverflowError) {
                return new Error(
                        Status.General.StackOverFlowError,
                        cause.getMessage(),
                        any,
                        fatal,
                        queryId,
                        BoltException.stackOverflow(any));
            }
        }

        // In this case, an error has "slipped out", and we don't have a good way to handle it. This indicates
        // a buggy code path, and we need to try to convince whoever ends up here to tell us about it.
        return new Error(
                Status.General.UnknownError,
                any != null ? any.getMessage() : null,
                any,
                fatal,
                null,
                BoltException.unknownError(any));
    }

    /**
     * This function is deprecated because it does not include any GQL status information.
     * Use the method that takes a Throwable instead.
     */
    @Deprecated
    private static Error fatalFrom(Status status, String message) {
        return new Error(status, message, true);
    }

    public static Error fatalFrom(Throwable any) {
        return from(any, true);
    }

    public boolean isFatal() {
        return fatal;
    }
}
