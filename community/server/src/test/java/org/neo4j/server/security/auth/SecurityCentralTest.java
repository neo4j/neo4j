/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Test;
import org.neo4j.helpers.FakeClock;
import org.neo4j.server.security.auth.exception.IllegalTokenException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

public class SecurityCentralTest
{
    @Test
    public void shouldKeepPersistentToken() throws Exception
    {
        // Given
        SecurityCentral security = new SecurityCentral( new FakeClock(), new InMemoryUserRepository() );
        security.newUser("neo4j", Privileges.ADMIN);
        security.regenerateToken( "neo4j" );
        String token = security.userForName( "neo4j" ).token();

        // When
        User user = security.userForToken( token );

        // Then
        assertThat( user.name(), equalTo("neo4j") );
    }

    @Test
    public void shouldRegenerateToken() throws Exception
    {
        // Given
        SecurityCentral security = new SecurityCentral( new FakeClock(), new InMemoryUserRepository() );
        security.newUser("neo4j", Privileges.ADMIN);
        String oldToken = security.userForName( "neo4j" ).token();

        // And given the user has been loaded by token before
        security.userForToken( oldToken );

        // When
        String newToken = security.regenerateToken( "neo4j" );

        // Then
        assertThat( security.userForName( "neo4j" ).token(),  equalTo(newToken) );
        assertThat( security.userForToken( newToken ).name(), equalTo("neo4j"));
        assertThat( security.userForToken( oldToken ).name(), equalTo(SecurityCentral.UNAUTHENTICATED.name()));
    }

    @Test
    public void shouldNotAllowSettingDuplicateTokens() throws Exception
    {
        // Given
        SecurityCentral security = new SecurityCentral( new FakeClock(), new InMemoryUserRepository() );
        security.newUser("neo4j", Privileges.ADMIN);
        security.newUser("other", Privileges.ADMIN);
        security.regenerateToken( "neo4j" );
        String neo4jUserToken = security.userForName( "neo4j" ).token();

        // When
        try
        {
            security.setToken( "other", neo4jUserToken );
            fail("Should not have been allowed.");
        } catch(IllegalTokenException e)
        {
            assertThat(e.getMessage(), equalTo("Unable to set token, because the chosen token is already in use."));
        }
    }
}
