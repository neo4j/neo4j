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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.neo4j.helpers.FakeClock;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class RateLimitedAuthenticationStrategyTest
{
    @Test
    public void shouldReturnSuccessForValidAttempt() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User( "user", Credential.forPassword( "right" ), false );

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldReturnFailureForInvalidAttempt() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User( "user", Credential.forPassword( "right" ), false );

        // Then
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldNotSlowRequestRateOnLessThanMaxFailedAttempts() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User( "user", Credential.forPassword( "right" ), false );

        // When we've failed two times
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.SUCCESS ));
    }

    @Test
    public void shouldSlowRequestRateOnMultipleFailedAttempts() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User( "user", Credential.forPassword( "right" ), false );

        // When we've failed three times
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );

        // Then
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ));

        // But when time heals all wounds
        clock.forward( 5, TimeUnit.SECONDS );

        // Then things should be alright
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User( "user", Credential.forPassword( "right" ), false );

        // When we've failed three times
        authStrategy.authenticate( user, "wrong" );
        authStrategy.authenticate( user, "wrong" );
        authStrategy.authenticate( user, "wrong" );

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ));

        // But when time heals all wounds
        clock.forward( 5, TimeUnit.SECONDS );

        // Then things should be alright
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.SUCCESS ) );
    }
}
