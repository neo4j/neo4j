/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest;

import org.junit.Test;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;

public class CypherQueriesIT extends EnterpriseVersionIT {

    @Test
    public void runningInCompiledRuntime() throws Exception
    {
        // Given
        String uri = functionalTestHelper.dataUri() + "transaction/commit";
        String payload = "{ 'statements': [ { 'statement': 'CYPHER runtime=compiled MATCH (n) RETURN n' } ] }";

        // When
        HTTP.Response res = HTTP.POST(uri, payload.replaceAll("'", "\""));

        // Then
        assertEquals( 200, res.status() );
    }
}
