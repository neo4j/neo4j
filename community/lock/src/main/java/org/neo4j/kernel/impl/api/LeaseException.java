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
package org.neo4j.kernel.impl.api;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.NotALeader;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class LeaseException extends GqlRuntimeException implements Status.HasStatus {
    private final Status status;

    private static final String NOT_ON_LEADER_ERROR_MESSAGE = "Should only attempt to acquire lease when leader.";

    @Deprecated
    public LeaseException(String message, Status status) {
        this(message, null, status);
    }

    private LeaseException(ErrorGqlStatusObject gqlStatusObject, String message, Status status) {
        this(gqlStatusObject, message, null, status);
    }

    @Deprecated
    public LeaseException(String message, Throwable cause, Status status) {
        super(message, cause);
        this.status = status;
    }

    private LeaseException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause, Status status) {
        super(gqlStatusObject, message, cause);
        this.status = status;
    }

    public static LeaseException failedToAcquireLease(Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N33)
                .build();
        return new LeaseException(gql, "Failed to acquire lease", cause, ReplicationFailure);
    }

    public static LeaseException localInstanceLostLease() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N07)
                .build();
        return new LeaseException(gql, "Local instance lost lease.", NotALeader);
    }

    public static LeaseException takenByAnotherCandidate() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N07)
                .build();
        return new LeaseException(gql, "Failed to acquire lease since it was taken by another candidate", NotALeader);
    }

    public static LeaseException notOnALeader() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N07)
                .build();
        return new LeaseException(gql, NOT_ON_LEADER_ERROR_MESSAGE, NotALeader);
    }

    public static LeaseException unexpectedException(Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N09)
                .build();
        return new LeaseException(gql, "Unexpected exception", cause, Status.General.UnknownError);
    }

    public static LeaseException interruptedOrTimeout() {
        return StacklessLeaseException.INTERRUPTED_EXCEPTION;
    }

    @Override
    public Status status() {
        return status;
    }

    static class StacklessLeaseException extends LeaseException {
        private static final StacklessLeaseException INTERRUPTED_EXCEPTION = new StacklessLeaseException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N33)
                        .build(),
                "Process was interrupted while attempting to acquire lease.",
                ReplicationFailure);

        private StacklessLeaseException(ErrorGqlStatusObject gqlStatusObject, String message, Status status) {
            super(gqlStatusObject, message, null, status);
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }
}
