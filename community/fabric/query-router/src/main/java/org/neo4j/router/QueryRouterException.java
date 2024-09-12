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
package org.neo4j.router;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.kernel.api.exceptions.HasQuery;
import org.neo4j.kernel.api.exceptions.Status;

public class QueryRouterException extends GqlRuntimeException implements Status.HasStatus, HasQuery {
    private final Status statusCode;
    private Long queryId;

    @Deprecated
    public QueryRouterException(Status statusCode, Throwable cause) {
        super(ErrorMessageHolder.getOldCauseMessage(cause), cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public QueryRouterException(ErrorGqlStatusObject gqlStatusObject, Status statusCode, Throwable cause) {
        super(gqlStatusObject, ErrorMessageHolder.getOldCauseMessage(cause), cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    @Deprecated
    public QueryRouterException(Status statusCode, String message, Object... parameters) {
        super(String.format(message, parameters));
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public QueryRouterException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Object... parameters) {
        super(gqlStatusObject, String.format(message, parameters));
        this.statusCode = statusCode;
        this.queryId = null;
    }

    @Deprecated
    public QueryRouterException(Status statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public QueryRouterException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    @Override
    public Status status() {
        return statusCode;
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
