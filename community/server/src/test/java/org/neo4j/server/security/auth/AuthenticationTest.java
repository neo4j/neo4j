/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.neo4j.helpers.FakeClock;
import org.neo4j.server.security.auth.exception.TooManyAuthenticationAttemptsException;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class AuthenticationTest
{
    @Test
    public void shouldSetPassword() throws Exception
    {
        // Given
        InMemoryUserRepository users = new InMemoryUserRepository();
        Authentication auth = new Authentication( new FakeClock(), users, 1 );

        users.save( new User( "jake", Privileges.ADMIN ) );

        // When
        auth.setPassword( "jake", "hello, world!" );

        // Then
        assertTrue( auth.authenticate( "jake", "hello, world!" ) );
        assertFalse( auth.authenticate( "jake", "goodbye, world!" ) );
    }

    @Test
    public void shouldSlowRequestRateOnMultipleFailedAttempts() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        Authentication auth = new Authentication( clock, new InMemoryUserRepository(), 3 );

        // And given we've failed three times
        auth.authenticate( "wrong", "wrong" );
        auth.authenticate( "wrong", "wrong" );
        auth.authenticate( "wrong", "wrong" );

        // When we do another request within the cooldown timeframe
        try
        {
            auth.authenticate( "wrong", "wrong" );

            // Then
            fail("Shouldn't have been allowed");
        }
        catch(TooManyAuthenticationAttemptsException e)
        {
            assertThat(e.getMessage(), equalTo("Too many failed authentication requests. Please try again in 5 seconds."));
        }

        // But when time heals all wounds
        clock.forward( 6, TimeUnit.SECONDS );

        // Then things should be alright
        assertFalse( auth.authenticate( "wrong", "wrong" ));
    }

    @Test
    public void handlesMalformedAuthentication() throws Exception
    {
        // Given
        InMemoryUserRepository users = new InMemoryUserRepository();
        Authentication auth = new Authentication( new FakeClock(), users, 50 );
        users.save( new User( "jake", Privileges.ADMIN ) );
        auth.setPassword( "jake", "helo" );

        // When & then
        assertFalse(auth.authenticate( "jake", "hello, world!" ));
        assertFalse(auth.authenticate( null, "hello, world!" ));
        assertFalse(auth.authenticate( "jake", null ));
        assertFalse(auth.authenticate( null, null ));
        assertFalse(auth.authenticate( "no such user", null ));
    }
}
