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
package org.neo4j.server;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.jmx.Primitives;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TestData;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WrappingNeoServerBootstrapperDocIT extends ExclusiveServerTestBase
{
    public
    @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    static GraphDatabaseAPI myDb;

    @BeforeClass
    public static void setup() throws IOException
    {
        myDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterClass
    public static void teardown()
    {
        myDb.shutdown();
    }

    @Test
    public void usingWrappingNeoServerBootstrapper()
    {
        // START SNIPPET: usingWrappingNeoServerBootstrapper
        // You provide the database, which must implement GraphDatabaseAPI.
        // Both EmbeddedGraphDatabase and HighlyAvailableGraphDatabase do this.
        GraphDatabaseAPI graphdb = myDb;

        WrappingNeoServerBootstrapper srv;
        srv = new WrappingNeoServerBootstrapper( graphdb );
        srv.start();
        // The server is now running
        // until we stop it:
        srv.stop();
        // END SNIPPET: usingWrappingNeoServerBootstrapper
    }

    @Test
    public void shouldAllowModifyingProperties()
    {
        // START SNIPPET: customConfiguredWrappingNeoServerBootstrapper
        // let the database accept remote neo4j-shell connections
        GraphDatabaseAPI graphdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE )
                .newGraphDatabase();
        ServerConfigurator config;
        config = new ServerConfigurator( graphdb );
        // let the server endpoint be on a custom port
        config.configuration().setProperty(
                Configurator.WEBSERVER_PORT_PROPERTY_KEY, "7575" );
        config.configuration().setProperty( ServerSettings.auth_enabled.name(), "false" );

        WrappingNeoServerBootstrapper srv;
        srv = new WrappingNeoServerBootstrapper( graphdb, config );
        srv.start();
        // END SNIPPET: customConfiguredWrappingNeoServerBootstrapper

        assertEquals( srv.getServer().baseUri().getPort(), 7575 );
        String response = gen.get().payload(
                "{\"command\" : \"ls\",\"engine\":\"shell\"}" ).expectedStatus(
                Status.OK.getStatusCode() ).post(
                "http://127.0.0.1:7575/db/manage/server/console/" ).entity();
        assertTrue( response.contains( "neo4j-sh (?)$" ) );
        srv.stop();
    }

    @Test
    public void shouldAllowShellConsoleWithoutCustomConfig()
    {
        ServerConfigurator config = new ServerConfigurator( myDb );
        config.configuration().setProperty( ServerSettings.auth_enabled.name(), "false" );
        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper( myDb, config );
        srv.start();
        String response = gen.get().payload(
                "{\"command\" : \"ls\",\"engine\":\"shell\"}" ).expectedStatus(
                Status.OK.getStatusCode() ).post(
                "http://127.0.0.1:7474/db/manage/server/console/" ).entity();
        assertTrue( response.contains( "neo4j-sh (?)$" ) );
        srv.stop();
    }

    @Test
    public void shouldAllowModifyingListenPorts() throws UnknownHostException
    {
        ServerConfigurator config = new ServerConfigurator( myDb );
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        config.configuration().setProperty(
                Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, hostAddress.toString() );
        config.configuration().setProperty( ServerSettings.auth_enabled.name(), "false" );
        config.configuration().setProperty(
                Configurator.WEBSERVER_PORT_PROPERTY_KEY, "8484" );


        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper( myDb, config );

        srv.start();
        try
        {
            gen.get().expectedStatus( Status.OK.getStatusCode() ).get(
                    format( "http://%s:7474/db/data/", hostAddress ) );
            fail();
        }
        catch ( ClientHandlerException cee )
        {
            // ok
        }

        gen.get().expectedStatus( Status.OK.getStatusCode() ).get(
                "http://" + hostAddress + ":8484/db/data/" );

        srv.stop();
    }

    @Test
    public void shouldRespondAndBeAbleToModifyDb()
    {
        Configurator configurator = new ServerConfigurator(myDb);

        configurator.configuration().setProperty( ServerSettings.auth_enabled.name(), "false" );
        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper( myDb, configurator );
        srv.start();

        long originalNodeNumber = myDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Primitives.class ).getNumberOfNodeIdsInUse();

        FunctionalTestHelper helper = new FunctionalTestHelper( srv.getServer() );
        JaxRsResponse response = new RestRequest().get( helper.dataUri() );
        assertEquals( 200, response.getStatus() );

        String nodeData = "{\"age\":12}";
        response = new RestRequest().post( helper.dataUri() + "node", nodeData );
        assertEquals( 201, response.getStatus() );

        long newNodeNumber = myDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Primitives.class ).getNumberOfNodeIdsInUse();

        assertEquals( originalNodeNumber + 1, newNodeNumber );

        srv.stop();

        // Should be able to still talk to the db
        try ( Transaction tx = myDb.beginTx() )
        {
            assertTrue( myDb.createNode() != null );
        }
    }
}
