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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class RateLimitedAuthenticationStrategyTest
{
    @Test
    public void shouldReturnSuccessForValidAttempt()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldReturnFailureForInvalidAttempt()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // Then
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldNotSlowRequestRateOnLessThanMaxFailedAttempts()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // When we've failed two times
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.SUCCESS ));
    }

    @Test
    public void shouldSlowRequestRateOnMultipleFailedAttempts()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

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
    public void shouldSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

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

    @Test
    public void shouldAllowUnlimitedFailedAttemptsWhenMaxFailedAttemptsIsZero()
    {
        testUnlimitedFailedAuthAttempts( 0 );
    }

    @Test
    public void shouldAllowUnlimitedFailedAttemptsWhenMaxFailedAttemptsIsNegative()
    {
        testUnlimitedFailedAuthAttempts( -42 );
    }

    private void testUnlimitedFailedAuthAttempts( int maxFailedAttempts )
    {
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = new RateLimitedAuthenticationStrategy( clock, maxFailedAttempts );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        int attempts = ThreadLocalRandom.current().nextInt( 5, 100 );
        for ( int i = 0; i < attempts; i++ )
        {
            assertEquals( AuthenticationResult.FAILURE, authStrategy.authenticate( user, "wrong" ) );
        }
    }

    private FakeClock getFakeClock()
    {
        return Clocks.fakeClock();
    }

}
