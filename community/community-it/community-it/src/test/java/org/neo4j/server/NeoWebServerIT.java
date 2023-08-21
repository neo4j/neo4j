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
package org.neo4j.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.withHeaders;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.server.HTTP;

public class NeoWebServerIT extends AbstractRestFunctionalTestBase {
    @Test
    public void shouldErrorForUnknownDatabaseViaTransactionalEndpoint() {
        HTTP.Response response =
                POST(txCommitUri("foo"), quotedJson("{ 'statements': [ { 'statement': 'RETURN 1' } ] }"));

        assertThat(response.status()).isEqualTo(404);
        assertThat(response).satisfies(hasErrors(Status.Database.DatabaseNotFound));
    }

    @Test
    public void shouldBeAbleToRunQueryAgainstSystemDatabaseViaTransactionalEndpoint() {
        HTTP.Response response = POST(
                txCommitUri("system"), quotedJson("{ 'statements': [ { 'statement': 'SHOW DEFAULT DATABASE' } ] }"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response).satisfies(containsNoErrors());
    }

    @Test
    public void shouldRespondToUnknownContentTypeWithNotAcceptableStatus() {
        var response = withHeaders("Accept", "application/badger").POST(txCommitUri("foo"));

        assertThat(response.status()).isEqualTo(406);
        assertThat(response.rawContent()).contains("application/json", "application/vnd.neo4j.jolt");
    }

    @Test
    public void shouldRedirectRootToBrowser() {
        assertFalse(container().getBaseUri().toString().contains("browser"));

        HTTP.Response res = HTTP.withHeaders(HttpHeaders.ACCEPT, MediaType.TEXT_HTML)
                .GET(container().getBaseUri().toString());
        assertThat(res.header("Location")).contains("browser");
    }
}
