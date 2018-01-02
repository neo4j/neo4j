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
package org.neo4j.server.rest;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.server.helpers.FunctionalTestHelper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class JmxServiceDocIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Test
    public void shouldRespondWithTheWebAdminClientSettings() throws Exception {
        String url = functionalTestHelper.managementUri() + "/server/jmx";
        JaxRsResponse resp = RestRequest.req().get(url);
        String json = resp.getEntity();

        assertEquals(json, 200, resp.getStatus());
        assertThat(json, containsString("resources"));
        assertThat(json, containsString("jmx/domain/{domain}/{objectName}"));
        resp.close();
    }
}
