/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.auth;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_lock_time;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class RateLimitedAuthenticationStrategyTest {
    @Test
    void shouldReturnSuccessForValidAttempt() {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, 3);
        User user = new User("user", null, credentialFor("right"), false, false);

        // Then
        assertThat(authStrategy.authenticate(user, password("right"))).isEqualTo(AuthenticationResult.SUCCESS);
    }

    @Test
    void shouldReturnSuccessForValidAttemptWithUserId() {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, 3);
        User user = new User("user", "id", credentialFor("right"), false, false);

        // Then
        assertThat(authStrategy.authenticate(user, password("right"))).isEqualTo(AuthenticationResult.SUCCESS);
    }

    @Test
    void shouldReturnFailureForInvalidAttempt() {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, 3);
        User user = new User("user", null, credentialFor("right"), false, false);

        // Then
        assertThat(authStrategy.authenticate(user, password("wrong"))).isEqualTo(AuthenticationResult.FAILURE);
    }

    @Test
    void shouldNotSlowRequestRateOnLessThanMaxFailedAttempts() {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, 3);
        User user = new User("user", null, credentialFor("right"), false, false);

        // When we've failed two times
        assertThat(authStrategy.authenticate(user, password("wrong"))).isEqualTo(AuthenticationResult.FAILURE);
        assertThat(authStrategy.authenticate(user, password("wrong"))).isEqualTo(AuthenticationResult.FAILURE);

        // Then
        assertThat(authStrategy.authenticate(user, password("right"))).isEqualTo(AuthenticationResult.SUCCESS);
    }

    @Test
    void shouldSlowRequestRateOnMultipleFailedAttempts() {
        testSlowRequestRateOnMultipleFailedAttempts(3, Duration.ofSeconds(5));
        testSlowRequestRateOnMultipleFailedAttempts(1, Duration.ofSeconds(10));
        testSlowRequestRateOnMultipleFailedAttempts(6, Duration.ofMinutes(1));
        testSlowRequestRateOnMultipleFailedAttempts(42, Duration.ofMinutes(2));
    }

    @Test
    void shouldSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid() {
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid(3, Duration.ofSeconds(5));
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid(1, Duration.ofSeconds(11));
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid(22, Duration.ofMinutes(2));
        testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid(42, Duration.ofDays(4));
    }

    private static void testSlowRequestRateOnMultipleFailedAttempts(int maxFailedAttempts, Duration lockDuration) {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, maxFailedAttempts, lockDuration);
        User user = new User("user", null, credentialFor("right"), false, false);

        // When we've failed max number of times
        for (int i = 0; i < maxFailedAttempts; i++) {
            assertThat(authStrategy.authenticate(user, password("wrong"))).isEqualTo(AuthenticationResult.FAILURE);
        }

        // Then
        assertThat(authStrategy.authenticate(user, password("wrong")))
                .isEqualTo(AuthenticationResult.TOO_MANY_ATTEMPTS);

        // But when time heals all wounds
        clock.forward(lockDuration.plus(1, SECONDS));

        // Then things should be alright
        assertThat(authStrategy.authenticate(user, password("wrong"))).isEqualTo(AuthenticationResult.FAILURE);
    }

    private static void testSlowRequestRateOnMultipleFailedAttemptsWhereAttemptIsValid(
            int maxFailedAttempts, Duration lockDuration) {
        // Given
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, maxFailedAttempts, lockDuration);
        User user = new User("user", null, credentialFor("right"), false, false);

        // When we've failed max number of times
        for (int i = 0; i < maxFailedAttempts; i++) {
            assertThat(authStrategy.authenticate(user, password("wrong"))).isEqualTo(AuthenticationResult.FAILURE);
        }

        // Then
        assertThat(authStrategy.authenticate(user, password("right")))
                .isEqualTo(AuthenticationResult.TOO_MANY_ATTEMPTS);

        // But when time heals all wounds
        clock.forward(lockDuration.plus(1, SECONDS));

        // Then things should be alright
        assertThat(authStrategy.authenticate(user, password("right"))).isEqualTo(AuthenticationResult.SUCCESS);
    }

    @Test
    void shouldAllowUnlimitedFailedAttemptsWhenMaxFailedAttemptsIsZero() {
        testUnlimitedFailedAuthAttempts(0);
    }

    @Test
    void shouldAllowUnlimitedFailedAttemptsWhenMaxFailedAttemptsIsNegative() {
        testUnlimitedFailedAuthAttempts(-42);
    }

    private static void testUnlimitedFailedAuthAttempts(int maxFailedAttempts) {
        FakeClock clock = getFakeClock();
        AuthenticationStrategy authStrategy = newAuthStrategy(clock, maxFailedAttempts);
        User user = new User("user", null, credentialFor("right"), false, false);

        int attempts = ThreadLocalRandom.current().nextInt(5, 100);
        for (int i = 0; i < attempts; i++) {
            assertEquals(AuthenticationResult.FAILURE, authStrategy.authenticate(user, password("wrong")));
        }
    }

    private static FakeClock getFakeClock() {
        return Clocks.fakeClock();
    }

    private static RateLimitedAuthenticationStrategy newAuthStrategy(Clock clock, int maxFailedAttempts) {
        Duration defaultLockDuration = Config.defaults().get(auth_lock_time);
        return newAuthStrategy(clock, maxFailedAttempts, defaultLockDuration);
    }

    private static RateLimitedAuthenticationStrategy newAuthStrategy(
            Clock clock, int maxFailedAttempts, Duration lockDuration) {
        return new RateLimitedAuthenticationStrategy(clock, lockDuration, maxFailedAttempts);
    }
}
