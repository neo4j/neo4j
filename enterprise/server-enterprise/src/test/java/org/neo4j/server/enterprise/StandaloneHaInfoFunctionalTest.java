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
package org.neo4j.server.enterprise;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.MasterInfoService.BASE_PATH;
import static org.neo4j.server.rest.MasterInfoService.IS_MASTER_PATH;
import static org.neo4j.server.rest.MasterInfoService.IS_SLAVE_PATH;
import static org.neo4j.test.server.ha.EnterpriseServerHelper.createNonPersistentServer;

public class StandaloneHaInfoFunctionalTest
{
    private static OpenEnterpriseNeoServer server;

    @Rule
    public final TestDirectory target = TestDirectory.testDirectory();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

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
    public void testHaDiscoveryOnStandaloneReturns403()
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
    public void testIsMasterOnStandaloneReturns403()
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) + IS_MASTER_PATH );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    public void testIsSlaveOnStandaloneReturns403()
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

       assertEquals( 2, ((Map) map.get( "services" )).size() );
    }
}
