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
package org.neo4j.queryapi.jsonl;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;

@QueryAPITestExtension(
        contentType = QueryContentType.TYPED_V1_0,
        acceptedContentTypes = {
            QueryContentType.TYPED_L_V1_0,
            QueryContentType.TYPED_V1_0,
            QueryContentType.TYPED,
            QueryContentType.UNTYPED
        },
        enabledFeatureFlagForUUID = true)
class QueryResourceTypedJsonlParametersIT extends AbstractQueryResourceTypedJsonlParametersIT {

    QueryResourceTypedJsonlParametersIT(QueryAPITestClient testClient) {
        super(testClient);
    }

    @Override
    protected QueryContentType expectedContentType() {
        return QueryContentType.TYPED_L_V1_0;
    }

    @Test
    void uuid() throws IOException, InterruptedException {
        var response =
                testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\"," + "\"parameters\": {\"parameter\": "
                        + "{\"$type\": \"UUID\", \"_value\": \"ca3d9a43-09e3-4b66-9384-87ea25e27d01\"}}}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(expectedContentType())
                .hasStatus(400)
                .receivesError(Status.Request.Invalid, "Type UUID is not supported.")
                .hasNoRemainingEvents();
    }
}
