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
package org.neo4j.exceptions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class CypherExecutionException extends Neo4jException {

    @Deprecated
    public CypherExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public CypherExecutionException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    @Deprecated
    public CypherExecutionException(String message) {
        super(message);
    }

    public CypherExecutionException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static CypherExecutionException wrapKernelException(String msg, KernelException e) {
        if (e.gqlStatusObject() != null) {
            return new CypherExecutionException(e, msg, e);
        }
        return new CypherExecutionException(msg, e);
    }

    public static CypherExecutionException csvBufferSizeOverflow(Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N49)
                        .build())
                .build();
        return new CypherExecutionException(
                gql,
                """
                Tried to read a field larger than the current buffer size.
                 Make sure that the field doesn't have an unterminated quote,
                 if it doesn't you can try increasing the buffer size via `dbms.import.csv.buffer_size`.""",
                cause);
    }

    public static CypherExecutionException internalError(String msgTitle, String msg, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .withParam(GqlParams.StringParam.msgTitle, msgTitle)
                .withParam(GqlParams.StringParam.msg, msg)
                .build();
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException unexpectedError(Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .withParam(GqlParams.StringParam.msgTitle, "Unexpected error")
                .withParam(GqlParams.StringParam.msg, cause.getMessage())
                .build();
        return new CypherExecutionException(gql, cause.getMessage(), cause);
    }

    public static CypherExecutionException unrecognisedExecutionMode(String procedure, String mode) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                .withParam(GqlParams.StringParam.proc, procedure)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N03)
                        .withParam(GqlParams.StringParam.proc, procedure)
                        .withParam(GqlParams.StringParam.procExeMode, mode)
                        .build())
                .build();
        return new CypherExecutionException(
                gql, "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode, null);
    }

    public static CypherExecutionException failedCopyPrivileges(String to, String from, Throwable cause) {
        var msg = String.format("Failed to create role '%s' as copy of '%s': Failed to copy privileges.", to, from);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException failedToAlterDb(String dbName, Throwable cause) {
        var msg = String.format("Failed to alter the specified database '%s'.", dbName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException createEntity(String entity, String name) {
        var msg = String.format("Failed to create the specified %s '%s'", entity, name);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg);
    }

    public static CypherExecutionException createEntityCause(String entity, String name, Throwable cause) {
        var msg = String.format("Failed to create the specified %s '%s'", entity, name);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException deleteEntityCause(String entity, String name, Throwable cause) {
        var msg = String.format("Failed to delete the specified %s '%s'", entity, name);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException alterEntityCause(String entity, String name, Throwable cause) {
        var msg = String.format("Failed to alter the specified %s '%s'", entity, name);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException renameEntityCauseNoTargetName(
            String entity, String oldName, Throwable cause) {
        var msg = String.format("Failed to rename the specified %s '%s'.", entity, oldName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException renameEntityCause(
            String entity, String oldName, String newName, Throwable cause) {
        var msg = String.format("Failed to rename the specified %s '%s' to '%s'.", entity, oldName, newName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException createRoleCopyCause(String role, Throwable cause) {
        var msg = String.format("Failed to create a role as copy of '%s'", role);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException dbStart(String dbName, Throwable cause) {
        var msg = String.format("Failed to start the specified database '%s'.", dbName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException dbStop(String dbName, Throwable cause) {
        var msg = String.format("Failed to stop the specified database '%s'.", dbName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException enableServer(String server, Throwable cause) {
        var msg = String.format("Failed to enable the specified server '%s'.", server);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException reallocateDbs(Throwable cause) {
        var msg = String.format("Failed to reallocate databases: %s", cause.getMessage());
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException deallocateServers(String names, Throwable cause) {
        var msg = String.format("Failed to deallocate the specified server(s) '%s'.", names);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException grantRole(String role, String userName, Throwable cause) {
        var msg = String.format("Failed to grant role '%s' to user '%s'.", role, userName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException revokeRole(String role, String userName, Throwable cause) {
        var msg = String.format("Failed to revoke role '%s' from user '%s'.", role, userName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException grantOrDenyExecutionPlan(String msg, Throwable cause) {
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException makeOrRevoke(String msg, Throwable cause) {
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    public static CypherExecutionException alterOwnPassword(String userName, Throwable cause) {
        var msg = String.format("User '%s' failed to alter their own password.", userName);
        var gql = GqlHelper.get50N00(CypherExecutionException.class.getSimpleName(), msg);
        return new CypherExecutionException(gql, msg, cause);
    }

    @Override
    public Status status() {
        Throwable cause = getCause();
        if (cause instanceof Status.HasStatus) {
            return ((Status.HasStatus) cause).status();
        } else {
            return Status.Statement.ExecutionFailed;
        }
    }
}
