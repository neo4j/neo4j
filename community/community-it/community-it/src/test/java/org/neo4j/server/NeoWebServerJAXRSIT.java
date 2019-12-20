/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.WebContainerHelper;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static org.junit.Assert.assertEquals;

public class NeoWebServerJAXRSIT extends ExclusiveWebContainerTestBase
{
    private TestWebContainer testWebContainer;

    @After
    public void stopServer()
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }

    @Test
    public void shouldMakeJAXRSClassesAvailableViaHTTP() throws Exception
    {
        var serverBuilder = CommunityWebContainerBuilder.builder();
        testWebContainer = WebContainerHelper.createNonPersistentContainer( serverBuilder );
        var functionalTestHelper = new FunctionalTestHelper( testWebContainer );

        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() ).GET().build();
        var httpClient = HttpClient.newBuilder().followRedirects( NORMAL ).build();
        var response = httpClient.send( request, discarding() );
        assertEquals( 200, response.statusCode() );
    }

    @Test
    public void shouldLoadThirdPartyJaxRsClasses() throws Exception
    {
        testWebContainer = CommunityWebContainerBuilder.serverOnRandomPorts()
                .withThirdPartyJaxRsPackage( "org.dummy.web.service",
                        DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        var httpClient = newBuilder().followRedirects( NORMAL ).build();

        var thirdPartyServiceUri = new URI( testWebContainer.getBaseUri() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT ).normalize();

        var request = HttpRequest.newBuilder( thirdPartyServiceUri ).GET().build();
        var response = httpClient.send( request, ofString() ).body();
        assertEquals( "hello", response );

        // Assert that extensions gets initialized
        var nodesCreated = createSimpleDatabase( testWebContainer.getDefaultDatabase() );
        thirdPartyServiceUri =
                new URI( testWebContainer.getBaseUri() + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT + "/inject-test" ).normalize();
        request = HttpRequest.newBuilder( thirdPartyServiceUri ).GET().build();
        response = httpClient.send( request, ofString() ).body();
        assertEquals( response, String.valueOf( nodesCreated ), response );
    }

    private static int createSimpleDatabase( final GraphDatabaseAPI graph )
    {
        final var numberOfNodes = 10;
        new Transactor( graph, tx ->
        {
            for ( var i = 0; i < numberOfNodes; i++ )
            {
                tx.createNode();
            }

            for ( var node1 : tx.getAllNodes() )
            {
                for ( var node2 : tx.getAllNodes() )
                {
                    if ( node1.equals( node2 ) )
                    {
                        continue;
                    }

                    node1.createRelationshipTo( node2, RelationshipType.withName( "REL" ) );
                }
            }
        } ).execute();

        return numberOfNodes;
    }
}
