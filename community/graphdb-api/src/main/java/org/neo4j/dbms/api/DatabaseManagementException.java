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
package org.neo4j.dbms.api;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * General exception, thrown in the event of errors when managing individual databases.
 * @see DatabaseManagementService
 */
@PublicApi
public class DatabaseManagementException extends GqlRuntimeException implements Status.HasStatus {

    public DatabaseManagementException() {
        super("");
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject, "");
    }

    public DatabaseManagementException(String message) {
        super(message);
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public DatabaseManagementException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public DatabaseManagementException(Throwable cause) {
        super(ErrorMessageHolder.getOldCauseMessage(cause), cause);
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(gqlStatusObject, ErrorMessageHolder.getOldCauseMessage(cause), cause);
    }

    @Override
    public Status status() {
        return Status.Database.Unknown;
    }

    public static DatabaseManagementException wrap(Throwable toWrap) {
        if (toWrap instanceof DatabaseManagementException) {
            return (DatabaseManagementException) toWrap;
        }
        return new DatabaseManagementException(toWrap.getMessage(), toWrap);
    }
}
