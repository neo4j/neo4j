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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.Triplet;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.webadmin.rest.MasterInfoService;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.ha.ServerCluster;

public class ClusterHaInfoFunctionalTest
{
    @SuppressWarnings( "unchecked" )
    private static final Pair<Integer/*ha port*/, Integer/*web port*/>[] SERVER_PORTS = new Pair[] {
            Pair.of( 6001, 7474 ), Pair.of( 6002, 7475 ), Pair.of( 6003, 7476 ) };
    private static final TargetDirectory dir = TargetDirectory.forTest( ClusterHaInfoFunctionalTest.class );
    private static final String MANAGEMENT_BASE_PATH = "db/manage";
    private static final String MASTER_INFO_BASE_PATH = MANAGEMENT_BASE_PATH + MasterInfoService.BASE_PATH;

    @Rule
    public TestName testName = new TestName();

    private ServerCluster cluster;

    @After
    public void stopServer()
    {
        if ( cluster != null ) cluster.shutdown();
        cluster = null;
    }

    @Before
    public void before() throws Exception
    {
        assumeFalse( Settings.osIsWindows() );

        cluster = new ServerCluster( testName.getMethodName(), dir, SERVER_PORTS );
    }

    @Test
    public void shouldAdvertiseManagementEndpoints() throws Exception
    {
        for ( Triplet<ServerCluster.ServerManager, URI, File> baseServerURI : cluster.getServers() )
        {
            URI managementUri = URI.create( baseServerURI.second() + MASTER_INFO_BASE_PATH );
            JaxRsResponse response = RestRequest.req().get( managementUri.toString() );
            Map<String, Object> services = JsonHelper.jsonToMap( response.getEntity() );
            assertEquals( 2, services.size() );
            assertEquals( managementUri + "/master", services.get( "isMaster" ) );
            assertEquals( managementUri + "/slave", services.get( "isSlave" ) );
        }
    }

    @Test
    public void shouldFindOneMaster() throws Exception
    {
        int masterCount = 0;

        for ( Triplet<ServerCluster.ServerManager, URI, File> serverTriplet : cluster.getServers() )
        {
            URI managementUri = URI.create( serverTriplet.second() + MASTER_INFO_BASE_PATH);
            JaxRsResponse response = RestRequest.req().get( managementUri + MasterInfoService.IS_MASTER_PATH );

            if (response.getStatus() == 200) {
                assertThat( response.getType(), is( MediaType.TEXT_PLAIN_TYPE ) );
                assertThat( response.getEntity(), is( "true" ) );
                masterCount++;
            }
        }

        assertEquals( 1, masterCount );
    }

    @Test
    public void shouldFindTwoSlaves() throws Exception
    {
        int slaveCount = 0;

        for ( Triplet<ServerCluster.ServerManager, URI, File> serverTriplet : cluster.getServers() )
        {
            URI managementUri = URI.create( serverTriplet.second() + MASTER_INFO_BASE_PATH );
            JaxRsResponse response = RestRequest.req().get( managementUri + MasterInfoService.IS_SLAVE_PATH );

            if (response.getStatus() == 200) {
                assertEquals( MediaType.TEXT_PLAIN_TYPE, response.getType() );
                assertEquals( "true", response.getEntity() );
                slaveCount++;
            }
        }

        assertEquals( 2, slaveCount);
    }
}
