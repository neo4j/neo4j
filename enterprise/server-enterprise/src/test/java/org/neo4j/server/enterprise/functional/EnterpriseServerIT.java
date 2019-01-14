/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise.functional;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.server.NeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.test.rule.SuppressOutput;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.mode;
import static org.neo4j.cluster.ClusterSettings.server_id;

public class EnterpriseServerIT
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldBeAbleToStartInHAMode() throws Throwable
    {
        // Given
        int clusterPort = PortAuthority.allocatePort();
        NeoServer server = EnterpriseServerBuilder.serverOnRandomPorts()
                .usingDataDir( folder.getRoot().getAbsolutePath() )
                .withProperty( mode.name(), "HA" )
                .withProperty( server_id.name(), "1" )
                .withProperty( cluster_server.name(), ":" + clusterPort )
                .withProperty( initial_hosts.name(), ":" + clusterPort )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase().getGraph(), is( instanceOf(HighlyAvailableGraphDatabase.class) ) );

            Client client = Client.create();
            ClientResponse r = client.resource( getHaEndpoint( server ) )
                    .accept( APPLICATION_JSON ).get( ClientResponse.class );
            assertEquals( 200, r.getStatus() );
            assertThat( r.getEntity( String.class ), containsString( "master" ) );
        }
        finally
        {
            server.stop();
        }
    }

    @Test
    public void shouldRequireAuthorizationForHAStatusEndpoints() throws Exception
    {
        // Given
        int clusterPort = PortAuthority.allocatePort();
        NeoServer server = EnterpriseServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), "true" )
                .usingDataDir( folder.getRoot().getAbsolutePath() )
                .withProperty( mode.name(), "HA" )
                .withProperty( server_id.name(), "1" )
                .withProperty( cluster_server.name(), ":" + clusterPort )
                .withProperty( initial_hosts.name(), ":" + clusterPort )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase().getGraph(), is( instanceOf(HighlyAvailableGraphDatabase.class) ) );

            Client client = Client.create();
            ClientResponse r = client.resource( getHaEndpoint( server ) )
                    .accept( APPLICATION_JSON ).get( ClientResponse.class );
            assertEquals( 401, r.getStatus() );
        }
        finally
        {
            server.stop();
        }
    }

    @Test
    public void shouldAllowDisablingAuthorizationOnHAStatusEndpoints() throws Exception
    {
        // Given
        int clusterPort = PortAuthority.allocatePort();
        NeoServer server = EnterpriseServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), "true" )
                .withProperty( HaSettings.ha_status_auth_enabled.name(), "false" )
                .usingDataDir( folder.getRoot().getAbsolutePath() )
                .withProperty( mode.name(), "HA" )
                .withProperty( server_id.name(), "1" )
                .withProperty( cluster_server.name(), ":" + clusterPort )
                .withProperty( initial_hosts.name(), ":" + clusterPort )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase().getGraph(), is( instanceOf(HighlyAvailableGraphDatabase.class) ) );

            Client client = Client.create();
            ClientResponse r = client.resource( getHaEndpoint( server ) )
                    .accept( APPLICATION_JSON ).get( ClientResponse.class );
            assertEquals( 200, r.getStatus() );
            assertThat( r.getEntity( String.class ), containsString( "master" ) );
        }
        finally
        {
            server.stop();
        }
    }

    private String getHaEndpoint( NeoServer server )
    {
        return server.baseUri().toString() + "db/manage/server/ha";
    }
}
