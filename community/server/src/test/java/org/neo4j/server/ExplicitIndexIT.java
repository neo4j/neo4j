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

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@RunWith( Theories.class )
public class ExplicitIndexIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @DataPoints
    @SuppressWarnings( "unused" ) // accessed by reflection
    public static String[] candidates = {"", "get_or_create", "create_or_fail"};

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Before
    public void startServer() throws IOException
    {
        server = serverOnRandomPorts().withHttpsEnabled()
                .withProperty( "dbms.shell.enabled", "false" )
                .withProperty( "dbms.security.auth_enabled", "false" )
                .withProperty( ServerSettings.maximum_response_header_size.name(), "5000" )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
    }

    @Theory
    public void shouldRejectIndexValueLargerThanConfiguredSize( String uniqueness )
    {
        //Given
        server.start();

        // When
        String nodeURI = HTTP.POST( server.baseUri().toString() + "db/data/node" ).header( "Location" );

        Random r = new Random();
        String value = "";
        for ( int i = 0; i < 6_000; i++ )
        {
            value += (char) (r.nextInt( 26 ) + 'a');
        }
        HTTP.Response response =
                HTTP.POST( server.baseUri().toString() + "db/data/index/node/favorites?uniqueness=" + uniqueness,
                        quotedJson( "{ 'value': '" + value + " ', 'uri':'" + nodeURI + "', 'key': 'some-key' }" ) );

        // Then
        assertThat( response.status(), is( 413 ) );
    }

    @Theory
    public void shouldNotRejectIndexValueThatIsJustSmallerThanConfiguredSize( String uniqueness )
    {
        //Given
        server.start();

        // When
        String nodeURI = HTTP.POST( server.baseUri().toString() + "db/data/node" ).header( "Location" );

        Random r = new Random();
        String value = "";
        for ( int i = 0; i < 4_000; i++ )
        {
            value += (char) (r.nextInt( 26 ) + 'a');
        }
        HTTP.Response response =
                HTTP.POST( server.baseUri().toString() + "db/data/index/node/favorites?uniqueness=" + uniqueness,
                        quotedJson( "{ 'value': '" + value + " ', 'uri':'" + nodeURI + "', 'key': 'some-key' }" ) );

        // Then
        assertThat( response.status(), is( 201 ) );
    }
}
