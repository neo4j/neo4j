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
package org.neo4j.server.queryapi.request.common;

import org.neo4j.server.queryapi.request.QueryRequest;

/**
 * A generic message body reader for {@link QueryRequest}
 * <p/>
 * This is specific for requests that don't expect the body has transaction information such
 * as bookmarks, max execution time, etc.
 */
public abstract class AbstractQueryRequestMessageBodyReader
        extends AbstractQueryRequestGenericMessageBodyReader<QueryRequest> {

    protected AbstractQueryRequestMessageBodyReader(QueryRequestModule queryRequestModule) {
        super(QueryRequest.class, queryRequestModule);
    }

    protected QueryRequest defaultQueryRequest() {
        return new QueryRequest();
    }
}
