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
import static org.junit.Assert.assertThat;

public class AuthorizationWhitelistIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @Test
    public void shouldWhitelistBrowser() throws Exception
    {
        // Given
        server = CommunityServerBuilder.server().withProperty( ServerSettings.auth_enabled.name(), "true" ).build();

        // When
        server.start();

        // Then I should be able to access the browser
        HTTP.Response response = HTTP.GET( server.baseUri().resolve( "browser/index.html" ).toString() );
        assertThat( response.status(), equalTo( 200 ) );
    }

    @Test
    public void shouldWhitelistWebadmin() throws Exception
    {
        // Given
        server = CommunityServerBuilder.server().withProperty( ServerSettings.auth_enabled.name(), "true" ).build();

        // When
        server.start();

        // Then I should be able to access the webadmin
        HTTP.Response response = HTTP.GET( server.baseUri().resolve( "webadmin/index.html" ).toString() );
        assertThat( response.status(), equalTo( 200 ) );
    }

    @Test
    public void shouldNotWhitelistDB() throws Exception
    {
        // Given
        server = CommunityServerBuilder.server().withProperty( ServerSettings.auth_enabled.name(), "true" ).build();

        // When
        server.start();

        // Then I should get a unauthorized response for access to the DB
        HTTP.Response response = HTTP.GET( server.baseUri().resolve( "db/data" ).toString() );
        assertThat( response.status(), equalTo( 401 ) );
    }

    @After
    public void cleanup()
    {
        if ( server != null ) { server.stop(); }
    }
}
