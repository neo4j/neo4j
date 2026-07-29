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
package org.neo4j.queryapi;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.function.Function;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.queryapi.testclient.QueryResponse;

public enum TransactionType {
    IMPLICIT("Implicit Transaction", Function.identity(), QueryAPITestClient::autoCommit),
    EXPLICIT("Explicit Transaction", (queryEndpoint) -> queryEndpoint + "/tx", QueryAPITestClient::beginTx);

    private final String name;
    private final Function<String, String> transformer;
    private final TransactionBeginMethodInterface beginMethod;

    TransactionType(
            String name, Function<String, String> transform, TransactionBeginMethodInterface beginMethodInterface) {
        this.name = name;
        this.transformer = transform;
        this.beginMethod = beginMethodInterface;
    }

    public String endpoint(String queryEndpoint) {
        return transformer.apply(queryEndpoint);
    }

    public HttpResponse<QueryResponse> begin(QueryAPITestClient queryAPITestClient, QueryRequest queryRequest)
            throws IOException, InterruptedException {
        return beginMethod.begin(queryAPITestClient, queryRequest);
    }

    @Override
    public String toString() {
        return name;
    }

    interface TransactionBeginMethodInterface {
        HttpResponse<QueryResponse> begin(QueryAPITestClient client, QueryRequest request)
                throws IOException, InterruptedException;
    }
}
