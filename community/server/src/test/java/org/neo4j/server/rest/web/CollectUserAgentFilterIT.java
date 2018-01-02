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
package org.neo4j.server.rest.web;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class CollectUserAgentFilterIT extends AbstractRestFunctionalTestBase
{
    public static final String USER_AGENT = "test/1.0";
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Test
    public void shouldRecordUserAgent() throws Exception
    {
        // When
        sendRequest( "test/1.0" );

        // Then
        assertThat( resolveDependency( UsageData.class ).get( UsageDataKeys.clientNames ), hasItem( USER_AGENT ) );
    }

    private void sendRequest(String userAgent)
    {
        String url = functionalTestHelper.baseUri().toString();
        JaxRsResponse resp = RestRequest.req().header( "User-Agent", userAgent ).get(url);
        String json = resp.getEntity();
        resp.close();
        assertEquals(json, 200, resp.getStatus());
    }
}
