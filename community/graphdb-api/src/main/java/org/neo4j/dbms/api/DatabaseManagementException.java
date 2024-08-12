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
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * General exception, thrown in the event of errors when managing individual databases.
 * @see DatabaseManagementService
 */
@PublicApi
public class DatabaseManagementException extends RuntimeException implements Status.HasStatus, HasGqlStatusInfo {
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public DatabaseManagementException() {
        super();
        this.gqlStatusObject = null;
        this.oldMessage = "";
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject) {
        super();
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = "";
    }

    public DatabaseManagementException(String message) {
        super(message);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message));
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = message;
    }

    public DatabaseManagementException(String message, Throwable cause) {
        super(message, cause);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = message;
    }

    public DatabaseManagementException(Throwable cause) {
        super(cause);

        this.gqlStatusObject = null;
        this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
    }

    public DatabaseManagementException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, HasGqlStatusInfo.getOldCauseMessage(cause)), cause);
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return Status.Database.Unknown;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }

    public static DatabaseManagementException wrap(Throwable toWrap) {
        if (toWrap instanceof DatabaseManagementException) {
            return (DatabaseManagementException) toWrap;
        }
        return new DatabaseManagementException(toWrap.getMessage(), toWrap);
    }
}
