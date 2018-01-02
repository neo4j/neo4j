/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.web.integration;

import org.junit.Test;

import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class CypherOldEndpointIT extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( "http://localhost:7474" );

    @Test
    public void periodicCommitTest() throws Exception
    {
        ServerTestUtils.withCSVFile(2, new ServerTestUtils.BlockWithCSVFileURL() {
            @Override
            public void execute( String url )
            {
                // begin
                HTTP.Response begin = http.POST(
                        "/db/data/cypher",
                        quotedJson( "{ 'query': 'USING PERIODIC COMMIT 100 LOAD CSV FROM \\\"" + url + "\\\" AS line CREATE ();' }" )
                );
                assertThat( begin.status(), equalTo( 200 ) );
            }
        });
    }
}

