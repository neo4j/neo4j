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
package org.neo4j.fabric.executor;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.HasQuery;
import org.neo4j.kernel.api.exceptions.Status;

public class FabricException extends RuntimeException implements Status.HasStatus, HasQuery, HasGqlStatusInfo {
    private final Status statusCode;
    private Long queryId;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public FabricException(Status statusCode, Throwable cause) {
        super(cause);
        this.statusCode = statusCode;
        this.queryId = null;

        this.gqlStatusObject = null;
        this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
    }

    public FabricException(ErrorGqlStatusObject gqlStatusObject, Status statusCode, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, HasGqlStatusInfo.getOldCauseMessage(cause)), cause);
        this.statusCode = statusCode;
        this.queryId = null;

        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
    }

    public FabricException(Status statusCode, String message, Object... parameters) {
        super(String.format(message, parameters));
        this.statusCode = statusCode;
        this.queryId = null;

        this.gqlStatusObject = null;
        this.oldMessage = String.format(message, parameters);
    }

    public FabricException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Object... parameters) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, String.format(message, parameters)));
        this.gqlStatusObject = gqlStatusObject;

        this.statusCode = statusCode;
        this.queryId = null;
        this.oldMessage = String.format(message, parameters);
    }

    public FabricException(Status statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = null;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public FabricException(ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;

        this.statusCode = statusCode;
        this.queryId = null;
        this.oldMessage = message;
    }

    public FabricException(Status statusCode, String message, Throwable cause, Long queryId) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = queryId;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public FabricException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Throwable cause, Long queryId) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;

        this.statusCode = statusCode;
        this.queryId = queryId;
        this.oldMessage = message;
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return statusCode;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }

    @Override
    public Long query() {
        return queryId;
    }

    @Override
    public void setQuery(Long queryId) {
        this.queryId = queryId;
    }
}
