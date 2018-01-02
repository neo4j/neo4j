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
package org.neo4j.server.plugins;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.server.SharedServerTestBase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.*;

public class ExtensionListingFunctionalTest extends SharedServerTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( SharedServerTestBase.server() );
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( SharedServerTestBase.server() );
    }

    @Test
    public void datarootContainsReferenceToExtensions() throws Exception {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.dataUri());
        assertThat(response.getStatus(), equalTo( 200 ));
        Map<String, Object> json = JsonHelper.jsonToMap( response.getEntity() );
        String extInfo = (String) json.get("extensions_info");
        assertNotNull(new URI(extInfo));
        response.close();
    }

    @Test
    public void canListAllAvailableServerExtensions() throws Exception {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.extensionUri());
        assertThat(response.getStatus(), equalTo( 200 ));
        Map<String, Object> json = JsonHelper.jsonToMap( response.getEntity() );
        assertFalse(json.isEmpty());
        response.close();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void canListExtensionMethodsForServerExtension() throws Exception {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.extensionUri());
        assertThat(response.getStatus(), equalTo( 200 ));

        Map<String, Object> json = JsonHelper.jsonToMap( response.getEntity() );
        String refNodeService = (String) json.get(FunctionalTestPlugin.class.getSimpleName());
        response.close();

        response = RestRequest.req().get(refNodeService);
        String result = response.getEntity();

        assertThat(response.getStatus(), equalTo( 200 ));

        json = JsonHelper.jsonToMap(result);
        json = (Map<String, Object>) json.get("graphdb");
        assertThat(json, hasKey( FunctionalTestPlugin.CREATE_NODE ));
        response.close();
    }
}
