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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mortbay.jetty.Response;
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
    public @Rule
    TestName testName = new TestName()
    {
        @Override
        public String getMethodName()
        {
            return ClusterHaInfoFunctionalTest.class.getSimpleName() + "." + super.getMethodName();
        }
    };

    private ServerCluster cluster;

    @After
    public void stopServer()
    {
        if ( cluster != null ) cluster.shutdown();
        cluster = null;
    }

    @Test
    public void allInstancesInClusterReportAvailabilityOfHaEndpoint() throws Exception
    {
        if ( Settings.osIsWindows() ) return;

        cluster = new ServerCluster( testName.getMethodName(), dir, SERVER_PORTS );

        for ( Triplet<ServerCluster.ServerManager, URI, File> baseServerURI : cluster.getServers() )
        {
            URI managementUri = URI.create( baseServerURI.second().toString() + "db/manage" );
            JaxRsResponse response = RestRequest.req().get( managementUri.toString() );
            Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity( String.class ) );
            assertEquals( 4, ((Map) map.get( "services" ) ).size());
        }
    }

    @Test
    public void allInstancesInClusterReturnProperIsMasterResponse() throws Exception
    {
        if ( Settings.osIsWindows() ) return;

        cluster = new ServerCluster( testName.getMethodName(), dir, SERVER_PORTS );

        String theMaster = null;
        for ( Triplet<ServerCluster.ServerManager, URI, File> baseServerURI : cluster.getServers() )
        {
            URI managementUri = URI.create( baseServerURI.second().toString() + "db/manage" );
            System.out.println("Requesting :::: "+managementUri + MasterInfoService.BASE_PATH +
                    MasterInfoService.ISMASTER_PATH);
            JaxRsResponse response = RestRequest.req().get( managementUri + MasterInfoService.BASE_PATH +
                    MasterInfoService.ISMASTER_PATH );
            if ( Response.SC_OK == response.getStatus() )
            {
                assertEquals( "200 means it is the master", Boolean.toString( Boolean.TRUE ), response.getEntity() );
                if ( theMaster == null )
                {
                    theMaster = baseServerURI.first().getHaURI();
                }
                else
                {
                    assertEquals( "all should report the same master", theMaster, baseServerURI );
                }
            }
            else if ( Response.SC_SEE_OTHER == response.getStatus() )
            {
                String reportedMaster = response.getHeaders().get( HttpHeaders.LOCATION ).get( 0 );
                assertNotNull( reportedMaster );

                if ( theMaster == null )
                {
                    theMaster = reportedMaster;
                }
                else
                {
                    assertEquals( "all should report the same master", theMaster, reportedMaster);
                }
            }
            else
            {
                fail( response.getStatus() + " is not an expected return code from " + baseServerURI.second() );
            }
        }
        // someone must be reported as the master
        assertNotNull( theMaster );
    }
}
