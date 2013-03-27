/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest;

import org.junit.Test;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;

public class TransactionFunctionalTest extends AbstractRestFunctionalTestBase
{

    @Test
    public void shouldBeAbleToCreateASession() throws Exception
    {
        // When, Then
        assertThat(gen().expectedStatus(201).post(getDataUri() + "session").deserialize(), not(nullValue()));
    }

    @Test
    public void shouldGetDifferentSessionIds() throws Exception
    {
        // When
        Integer firstSession = gen().expectedStatus(201).post(getDataUri() + "session").deserialize();
        Integer secondSession = gen().expectedStatus(201).post(getDataUri() + "session").deserialize();

        // Then
        assertThat(firstSession, not(equalTo(secondSession)));
    }

    @Test
    public void shouldBeAbleToCommitChanges() throws Exception
    {
        // Given
        String sessionId = POST(getDataUri() + "session").content().toString();

        // When
        String node1 = HTTP.withHeaders("X-Session", sessionId).POST(getDataUri() + "node").location();

        // And When
        String node2 = HTTP.withHeaders("X-Session", sessionId,
                                        "X-Tx-Action", "COMMIT").POST(getDataUri() + "node").location();

        // Then
        assertThat(GET(node1).status(), is(200));
        assertThat(GET(node2).status(), is(200));
    }

    @Test
    public void shouldBeAbleToRollbackChanges() throws Exception
    {
        // Given
        String sessionId = POST(getDataUri() + "session").content().toString();

        // When
        String node = HTTP.withHeaders("X-Session", sessionId).POST(getDataUri() + "node").location();
        HTTP.withHeaders("X-Session", sessionId,
                         "X-Tx-Action", "ROLLBACK").GET(getDataUri() + "session");

        // Then
        assertThat(GET(node).status(), is(404));
    }
}
