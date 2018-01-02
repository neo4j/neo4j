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

import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.UnitOfWork;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.helpers.FunctionalTestHelper.CLIENT;

public class NeoServerJAXRSDocIT extends ExclusiveServerTestBase
{
    private NeoServer server;

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void shouldMakeJAXRSClassesAvailableViaHTTP() throws Exception
    {
        CommunityServerBuilder builder = CommunityServerBuilder.server();
        server = ServerHelper.createNonPersistentServer( builder );
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server );

        JaxRsResponse response = new RestRequest().get( functionalTestHelper.webAdminUri() );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldLoadThirdPartyJaxRsClasses() throws Exception
    {
        server = CommunityServerBuilder.server()
                .withThirdPartyJaxRsPackage( "org.dummy.web.service",
                        DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        URI thirdPartyServiceUri = new URI( server.baseUri()
                .toString() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT ).normalize();
        String response = CLIENT.resource( thirdPartyServiceUri.toString() )
                .get( String.class );
        assertEquals( "hello", response );

        // Assert that extensions gets initialized
        int nodesCreated = createSimpleDatabase( server.getDatabase().getGraph() );
        thirdPartyServiceUri = new URI( server.baseUri()
                .toString() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT + "/inject-test" ).normalize();
        response = CLIENT.resource( thirdPartyServiceUri.toString() )
                .get( String.class );
        assertEquals( String.valueOf( nodesCreated ), response );
    }

    private int createSimpleDatabase( final GraphDatabaseAPI graph )
    {
        final int numberOfNodes = 10;
        new Transactor( graph, new UnitOfWork()
        {

            @Override
            public void doWork()
            {
                for ( int i = 0; i < numberOfNodes; i++ )
                {
                    graph.createNode();
                }

                for ( Node n1 : GlobalGraphOperations.at(graph).getAllNodes() )
                {
                    for ( Node n2 : GlobalGraphOperations.at(graph).getAllNodes() )
                    {
                        if ( n1.equals( n2 ) )
                        {
                            continue;
                        }

                        n1.createRelationshipTo( n2, DynamicRelationshipType.withName( "REL" ) );
                    }
                }
            }
        } ).execute();

        return numberOfNodes;
    }
}
