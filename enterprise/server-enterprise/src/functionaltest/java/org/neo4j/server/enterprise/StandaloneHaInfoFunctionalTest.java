/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;
import org.mortbay.jetty.Response;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.webadmin.rest.MasterInfoService;
import org.neo4j.test.server.ha.AbstractEnterpriseRestFunctionalTestBase;

public class StandaloneHaInfoFunctionalTest extends AbstractEnterpriseRestFunctionalTestBase
{
    @Test
    public void testHaDiscoveryOnStandaloneReturns403() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server() );

        JaxRsResponse response = RestRequest.req().get( helper.managementUri() + MasterInfoService.BASE_PATH );
        assertEquals( Response.SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    public void testIsMasterOnStandaloneReturns403() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server() );

        JaxRsResponse response = RestRequest.req().get(helper.managementUri() +
                MasterInfoService.BASE_PATH + MasterInfoService.ISMASTER_PATH);
        assertEquals( Response.SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    public void testGetMasterOnStandaloneReturns403() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server() );

        JaxRsResponse response = RestRequest.req().get(helper.managementUri() +
                MasterInfoService.BASE_PATH + MasterInfoService.GETMASTER_PATH );
        assertEquals( Response.SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    public void testDiscoveryListingOnStandaloneDoesNotContainHA() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server() );

        JaxRsResponse response = RestRequest.req().get( helper.managementUri() );

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity( String.class ) );

        assertEquals( 3, ((Map) map.get( "services" )).size());
    }
}
