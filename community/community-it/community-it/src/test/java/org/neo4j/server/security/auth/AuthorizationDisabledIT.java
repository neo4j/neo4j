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
package org.neo4j.server.security.auth;

import org.junit.After;
import org.junit.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class AuthorizationDisabledIT extends ExclusiveWebContainerTestBase
{

    private TestWebContainer testWebContainer;

    @Test
    public void shouldAllowDisablingAuthorization() throws Exception
    {
        // Given
        testWebContainer = serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), FALSE ).build();

        // When

        // Then I should have write access
        HTTP.Response response = HTTP.POST( testWebContainer.getBaseUri().resolve( txCommitEndpoint() ).toString(),
                rawPayload( "{\"statements\": [{\"statement\": \"CREATE ({name:'My Node'})\"}]}" ) );
        assertThat( response.status(), equalTo( 200 ) );

        // Then I should have read access
        response = HTTP.POST( testWebContainer.getBaseUri().resolve( txCommitEndpoint() ).toString(),
                rawPayload( "{\"statements\": [{\"statement\": \"MATCH (n {name:'My Node'}) RETURN n\"}]}" ) );
        assertThat( response.status(), equalTo( 200 ) );
        String responseBody = response.rawContent();
        assertThat( responseBody, containsString( "My Node" ) );
    }

    @After
    public void cleanup()
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }
}
