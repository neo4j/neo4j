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
package org.neo4j.bolt.fsm.error;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class BoltException extends GqlException implements Status.HasStatus {
    private final Status status;

    private BoltException(ErrorGqlStatusObject gql, Status status, String message) {
        super(gql, message);
        this.status = status;
    }

    private BoltException(ErrorGqlStatusObject gql, Status status, String message, Throwable cause) {
        super(gql, message, cause);
        this.status = status;
    }

    public static BoltException invalidServerState(String message, String serverState) {
        // DRI-035
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N06)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N10)
                        .withParam(GqlParams.StringParam.msg, message)
                        .withParam(GqlParams.StringParam.boltServerState, serverState)
                        .build())
                .build();
        return new BoltException(
                gql,
                Status.Request.Invalid,
                "Message '" + message + "' cannot be handled by session in the " + serverState + " state");
    }

    public static BoltException failedToAcquireExecutionThread() {
        // DRI-055
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N59)
                .withDiagnosticRecordProperty(BoltDiagnosticRecordProperty.IDEMPOTENT, true)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N38)
                        .build())
                .build();
        return new BoltException(
                gql,
                Status.Request.NoThreadsAvailable,
                Status.Request.NoThreadsAvailable.code().description());
    }

    public static BoltException outOfMemory(Throwable cause) {
        // DRI-053
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N59)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N36)
                        .build())
                .build();
        return new BoltException(
                gql, Status.General.OutOfMemoryError, ErrorMessageHolder.getOldCauseMessage(cause), cause);
    }

    public static BoltException stackOverflow(Throwable cause) {
        // DRI-054
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N59)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N37)
                        .build())
                .build();
        return new BoltException(
                gql, Status.General.StackOverFlowError, ErrorMessageHolder.getOldCauseMessage(cause), cause);
    }

    public static BoltException unknownError(Throwable cause) {
        // DRI-057
        if (cause == null) {
            // Default GQL error code
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                    .build();
            return new BoltException(
                    gql,
                    Status.General.UnknownError,
                    Status.General.UnknownError.code().description(),
                    cause);
        } else {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                    .withParam(GqlParams.StringParam.msgTitle, cause.getClass().getSimpleName())
                    .withParam(GqlParams.StringParam.msg, cause.getMessage() != null ? cause.getMessage() : "")
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N09)
                            .withParam(GqlParams.StringParam.boltServerState, "uncaught error")
                            .build())
                    .build();
            return new BoltException(
                    gql, Status.General.UnknownError, ErrorMessageHolder.getOldCauseMessage(cause), cause);
        }
    }

    @Override
    public Status status() {
        return status;
    }
}
