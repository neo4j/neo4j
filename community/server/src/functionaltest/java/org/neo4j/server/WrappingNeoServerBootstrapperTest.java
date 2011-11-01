/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.jmx.Primitives;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.EmbeddedServerConfigurator;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse.Status;

public class WrappingNeoServerBootstrapperTest
{
    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    AbstractGraphDatabase myDb;
    FunctionalTestHelper helper;

    @Before
    public void setup() throws IOException
    {
        myDb = new ImpermanentGraphDatabase();
    }

    @After
    public void teardown()
    {
        myDb.shutdown();
    }

    private AbstractGraphDatabase getGraphDb()
    {
        return myDb;
    }

    @Test
    public void usingWrappingNeoServerBootstrapper()
    {
        // START SNIPPET: usingWrappingNeoServerBootstrapper
        AbstractGraphDatabase graphdb = getGraphDb();
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
        AbstractGraphDatabase graphdb = getGraphDb();
        EmbeddedServerConfigurator config;
        config = new EmbeddedServerConfigurator( graphdb );
        config.configuration().setProperty(
                Configurator.WEBSERVER_PORT_PROPERTY_KEY, 7575 );

        WrappingNeoServerBootstrapper srv;
        srv = new WrappingNeoServerBootstrapper( graphdb, config );
        srv.start();
        // END SNIPPET: customConfiguredWrappingNeoServerBootstrapper

        assertEquals( srv.getServer().baseUri().getPort(), 7575 );

        srv.stop();
    }

    @Test
    public void shouldAllowModifyingListenPorts() throws UnknownHostException
    {

        EmbeddedServerConfigurator config = new EmbeddedServerConfigurator(
                myDb );
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        config.configuration().setProperty(
                Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY,
                hostAddress );

        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper(
                myDb, config );

        srv.start();
        try
        {
            gen.get().expectedStatus( Status.OK.getStatusCode() ).get(
                    "http://127.0.0.1:7474/db/data/" );
            fail();
        }
        catch ( ClientHandlerException cee )
        {
            //ok
        }

        gen.get().expectedStatus( Status.OK.getStatusCode() ).get(
                "http://"+hostAddress+":7474/db/data/" );

        srv.stop();
    }

    @Test
    public void serverShouldRespondProperly()
    {

        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper(
                myDb );

        srv.start();
        helper = new FunctionalTestHelper( srv.getServer() );

        JaxRsResponse response = new RestRequest().get( helper.dataUri() );
        assertEquals( 200, response.getStatus() );

        srv.stop();
    }

    @Test
    public void shouldModifyInjectedDatabase()
    {

        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper(
                myDb );

        srv.start();

        long originalNodeNumber = myDb.getManagementBean( Primitives.class ).getNumberOfNodeIdsInUse();

        helper = new FunctionalTestHelper( srv.getServer() );
        String nodeData = "{\"age\":12}";

        JaxRsResponse response = new RestRequest().post( helper.dataUri()
                                                         + "node", nodeData );
        assertEquals( 201, response.getStatus() );

        long newNodeNumber = myDb.getManagementBean( Primitives.class ).getNumberOfNodeIdsInUse();

        assertEquals( originalNodeNumber + 1, newNodeNumber );

        srv.stop();
    }

    @Test
    public void shouldNotStopDatabaseOnShutdown()
    {

        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper(
                myDb );

        srv.start();
        helper = new FunctionalTestHelper( srv.getServer() );
        srv.stop();

        // Should be able to still talk to the db
        assertTrue( myDb.getReferenceNode() != null );
    }

}
