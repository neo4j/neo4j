/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.webadmin.rest.MasterInfoService.BASE_PATH;
import static org.neo4j.server.webadmin.rest.MasterInfoService.IS_MASTER_PATH;
import static org.neo4j.server.webadmin.rest.MasterInfoService.IS_SLAVE_PATH;
import static org.neo4j.test.server.ha.EnterpriseServerHelper.createNonPersistentServer;

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.TargetDirectory;

public class StandaloneHaInfoFunctionalTest
{
    private static EnterpriseNeoServer server;

    @Rule
    public TargetDirectory.TestDirectory target = TargetDirectory.testDirForTest( getClass() );

    @Before
    public void before() throws IOException
    {
        server = createNonPersistentServer(target.directory());
    }

    @After
    public void after()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void testHaDiscoveryOnStandaloneReturns403() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    private String getBasePath( FunctionalTestHelper helper )
    {
        return helper.managementUri() + "/" + BASE_PATH;
    }

    @Test
    public void testIsMasterOnStandaloneReturns403() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) + IS_MASTER_PATH );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    public void testIsSlaveOnStandaloneReturns403() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) + IS_SLAVE_PATH );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    public void testDiscoveryListingOnStandaloneDoesNotContainHA() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( helper.managementUri() );

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity() );

        assertEquals( 3, ((Map) map.get( "services" )).size() );
    }
}
