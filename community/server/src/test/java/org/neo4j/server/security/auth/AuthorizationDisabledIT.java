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
package org.neo4j.server.security.auth;

import org.junit.After;
import org.junit.Test;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class AuthorizationDisabledIT extends ExclusiveServerTestBase
{

    private CommunityNeoServer server;

    @Test
    public void shouldAllowDisablingAuthorization() throws Exception
    {
        // Given
        server = CommunityServerBuilder.server().withProperty( ServerSettings.auth_enabled.name(), "false" ).build();

        // When
        server.start();

        // Then I should have write access
        HTTP.Response response = HTTP.POST( server.baseUri().resolve( "db/data/node" ).toString(), quotedJson( "{'name':'My Node'}" ) );
        assertThat(response.status(), equalTo(201));
        String node = response.location();

        // Then I should have read access
        assertThat( HTTP.GET( node ).status(), equalTo( 200 ) );
    }

    @After
    public void cleanup()
    {
        if(server != null) { server.stop(); }
    }
}
