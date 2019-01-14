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

import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_lock_time;

public class RateLimitedAuthenticationStrategyTest
{
    @Test
    public void shouldReturnSuccessForValidAttempt()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldReturnFailureForInvalidAttempt()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy( clock, 3 );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // Then
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldNotSlowRequestRateOnLessThanMaxFailedAttempts()
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy( clock, 3 );
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
        testSlowRequestRateOnMultipleFailedAttempts( 3, Duration.ofSeconds( 5 ) );
        testSlowRequestRateOnMultipleFailedAttempts( 1, Duration.ofSeconds( 10 ) );
        testSlowRequestRateOnMultipleFailedAttempts( 6, Duration.ofMinutes( 1 ) );
        testSlowRequestRateOnMultipleFailedAttempts( 42, Duration.ofMinutes( 2 ) );
    }

    @Test
    public void shouldSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid()
    {
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid( 3, Duration.ofSeconds( 5 ) );
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid( 1, Duration.ofSeconds( 11 ) );
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid( 22, Duration.ofMinutes( 2 ) );
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid( 42, Duration.ofDays( 4 ) );
    }

    private void testSlowRequestRateOnMultipleFailedAttempts( int maxFailedAttempts, Duration lockDuration )
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy( clock, maxFailedAttempts, lockDuration );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // When we've failed max number of times
        for ( int i = 0; i < maxFailedAttempts; i++ )
        {
            assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
        }

        // Then
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ) );

        // But when time heals all wounds
        clock.forward( lockDuration.plus( 1, SECONDS ) );

        // Then things should be alright
        assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
    }

    private void testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid( int maxFailedAttempts, Duration lockDuration )
    {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy( clock, maxFailedAttempts, lockDuration );
        User user = new User.Builder( "user", Credential.forPassword( "right" ) ).build();

        // When we've failed max number of times
        for ( int i = 0; i < maxFailedAttempts; i++ )
        {
            assertThat( authStrategy.authenticate( user, "wrong" ), equalTo( AuthenticationResult.FAILURE ) );
        }

        // Then
        assertThat( authStrategy.authenticate( user, "right" ), equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ));

        // But when time heals all wounds
        clock.forward( lockDuration.plus( 1, SECONDS ) );

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
        AuthenticationStrategy authStrategy = newAuthStrategy( clock, maxFailedAttempts );
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

    private static RateLimitedAuthenticationStrategy newAuthStrategy( Clock clock, int maxFailedAttempts )
    {
        Duration defaultLockDuration = Config.defaults().get( auth_lock_time );
        return newAuthStrategy( clock, maxFailedAttempts, defaultLockDuration );
    }

    private static RateLimitedAuthenticationStrategy newAuthStrategy( Clock clock, int maxFailedAttempts, Duration lockDuration )
    {
        return new RateLimitedAuthenticationStrategy( clock, lockDuration, maxFailedAttempts );
    }
}
