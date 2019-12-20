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
package org.neo4j.server.rest.security;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.exceptions.Status.Security.CredentialsExpired;
import static org.neo4j.kernel.api.exceptions.Status.Security.Forbidden;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class CommunityWebContainerTestBase extends ExclusiveWebContainerTestBase
{
    protected TestWebContainer testWebContainer;

    @After
    public void cleanup()
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }

    protected void startServer( boolean authEnabled ) throws IOException
    {
        testWebContainer = serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .build();
    }

    @SuppressWarnings( "SameParameterValue" )
    void startServer( boolean authEnabled, String accessControlAllowOrigin ) throws IOException
    {
        testWebContainer = serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .withProperty( ServerSettings.http_access_control_allow_origin.name(), accessControlAllowOrigin )
                .build();
    }

    String databaseURL()
    {
        return testWebContainer.getBaseUri().resolve( "db/neo4j/" ).toString();
    }

    protected String txCommitURL()
    {
        return txCommitURL( "neo4j" );
    }

    protected String txCommitURL( String database )
    {
        return testWebContainer.getBaseUri().resolve( txCommitEndpoint( database ) ).toString();
    }

    protected void assertPermissionErrorAtDataAccess( HTTP.Response response ) throws JsonParseException
    {
        assertPermissionError( response, Collections.singletonList( CredentialsExpired.code().serialize() ) );
    }

    void assertPermissionErrorAtSystemAccess( HTTP.Response response ) throws JsonParseException
    {
        List<String> possibleErrors = Arrays.asList( CredentialsExpired.code().serialize(), Forbidden.code().serialize() );
        assertPermissionError( response, possibleErrors );
    }

    private void assertPermissionError( HTTP.Response response, List<String> errors ) throws JsonParseException
    {
        assertThat( response.status(), equalTo( 200 ) );
        assertThat( response.get( "errors" ).size(), equalTo( 1 ) );

        JsonNode firstError = response.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), is(in( errors ) ) );

        assertThat( firstError.get( "message" ).asText(), startsWith( "Permission denied." ) );
    }

    protected static HTTP.RawPayload query( String statement )
    {
        return rawPayload( "{\"statements\":[{\"statement\":\"" + statement + "\"}]}" );
    }
}
