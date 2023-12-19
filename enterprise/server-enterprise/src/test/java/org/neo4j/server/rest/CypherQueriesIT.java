/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.rest;

import org.junit.Test;

import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;

public class CypherQueriesIT extends EnterpriseVersionIT
{

    @Test
    public void runningInCompiledRuntime()
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
