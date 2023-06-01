/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.router;

import org.neo4j.kernel.api.exceptions.HasQuery;
import org.neo4j.kernel.api.exceptions.Status;

public class QueryRouterException extends RuntimeException implements Status.HasStatus, HasQuery {
    private final Status statusCode;
    private Long queryId;

    public QueryRouterException(Status statusCode, Throwable cause) {
        super(cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public QueryRouterException(Status statusCode, String message, Object... parameters) {
        super(String.format(message, parameters));
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public QueryRouterException(Status statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public QueryRouterException(Status statusCode, String message, Throwable cause, Long queryId) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = queryId;
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
