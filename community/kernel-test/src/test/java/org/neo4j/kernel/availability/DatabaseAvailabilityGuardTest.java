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
package org.neo4j.kernel.availability;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.InternalLog;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.Clocks;

@Timeout(30)
@ExtendWith(LifeExtension.class)
class DatabaseAvailabilityGuardTest {
    private static final AvailabilityRequirement REQUIREMENT_1 =
            new DescriptiveAvailabilityRequirement("Requirement 1");
    private static final AvailabilityRequirement REQUIREMENT_2 =
            new DescriptiveAvailabilityRequirement("Requirement 2");

    private final Clock clock = Clocks.systemClock();
    private final InternalLog log = mock(InternalLog.class);

    @Inject
    private LifeSupport life;

    @Test
    void notStartedGuardIsNotAvailable() {
        DatabaseAvailabilityGuard availabilityGuard = createAvailabilityGuard(clock, log);
        assertFalse(availabilityGuard.isAvailable());
        assertFalse(availabilityGuard.isAvailable(0));
        assertTrue(availabilityGuard.isShutdown());
    }

    @Test
    void shutdownAvailabilityGuardIsNotAvailable() {
        DatabaseAvailabilityGuard availabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        assertTrue(availabilityGuard.isAvailable());
        assertFalse(availabilityGuard.isShutdown());

        availabilityGuard.stop();
        availabilityGuard.shutdown();

        assertFalse(availabilityGuard.isAvailable());
        assertTrue(availabilityGuard.isShutdown());
    }

    @Test
    void restartedAvailabilityGuardIsAvailable() {
        DatabaseAvailabilityGuard availabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        assertTrue(availabilityGuard.isAvailable());
        assertFalse(availabilityGuard.isShutdown());

        availabilityGuard.stop();
        availabilityGuard.shutdown();

        availabilityGuard.init();
        assertFalse(availabilityGuard.isShutdown());
        assertTrue(availabilityGuard.isAvailable());

        availabilityGuard.start();
        assertFalse(availabilityGuard.isShutdown());
        assertTrue(availabilityGuard.isAvailable());
    }

    @Test
    void logOnAvailabilityChange() {
        AvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);

        // When starting out
        verifyNoInteractions(log);

        // When requirement is added
        databaseAvailabilityGuard.require(REQUIREMENT_1);

        // Then log should have been called
        verifyLogging(log, atLeastOnce());

        // When requirement fulfilled
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);

        // Then log should have been called
        verifyLogging(log, times(4));

        // When requirement is added
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // Then log should have been called
        verifyLogging(log, times(6));

        // When requirement fulfilled
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);

        // Then log should not have been called
        verifyLogging(log, times(6));

        // When requirement fulfilled
        databaseAvailabilityGuard.fulfill(REQUIREMENT_2);

        // Then log should have been called
        verifyLogging(log, times(8));
    }

    @Test
    void givenAccessGuardWith2ConditionsWhenAwaitThenTimeoutAndReturnFalse() {
        // Given
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // When
        boolean result = databaseAvailabilityGuard.isAvailable(1000);

        // Then
        assertFalse(result);
    }

    @Test
    void givenAccessGuardWith2ConditionsWhenAwaitThenActuallyWaitGivenTimeout() {
        // Given
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // When
        long timeout = 1000;
        long start = clock.millis();
        boolean result = databaseAvailabilityGuard.isAvailable(timeout);
        long end = clock.millis();

        // Then
        long waitTime = end - start;
        assertFalse(result);
        assertThat(waitTime).isGreaterThanOrEqualTo(timeout);
    }

    @Test
    void givenAccessGuardWith2ConditionsWhenGrantOnceAndAwaitThenTimeoutAndReturnFalse() {
        // Given
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // When
        long start = clock.millis();
        long timeout = 1000;
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);
        boolean result = databaseAvailabilityGuard.isAvailable(timeout);
        long end = clock.millis();

        // Then
        long waitTime = end - start;
        assertFalse(result);
        assertThat(waitTime).isGreaterThanOrEqualTo(timeout);
    }

    @Test
    void givenAccessGuardWith2ConditionsWhenGrantEachAndAwaitThenTrue() {
        // Given
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // When
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);
        databaseAvailabilityGuard.fulfill(REQUIREMENT_2);

        assertTrue(databaseAvailabilityGuard.isAvailable(1000));
    }

    @Test
    void givenAccessGuardWith2ConditionsWhenGrantTwiceAndDenyOnceAndAwaitThenTimeoutAndReturnFalse() {
        // Given
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // When
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        long start = clock.millis();
        long timeout = 1000;
        boolean result = databaseAvailabilityGuard.isAvailable(timeout);
        long end = clock.millis();

        // Then
        long waitTime = end - start;
        assertFalse(result);
        assertThat(waitTime).isGreaterThanOrEqualTo(timeout);
    }

    @Test
    void givenAccessGuardWith2ConditionsWhenGrantOnceAndAwaitAndGrantAgainThenReturnTrue() {
        // Given
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        databaseAvailabilityGuard.fulfill(REQUIREMENT_2);
        assertFalse(databaseAvailabilityGuard.isAvailable(100));

        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);
        assertTrue(databaseAvailabilityGuard.isAvailable(100));
    }

    @Test
    void givenAccessGuardWithConditionWhenGrantThenNotifyListeners() {
        // Given
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);

        final AtomicBoolean notified = new AtomicBoolean();
        AvailabilityListener availabilityListener = new AvailabilityListener() {
            @Override
            public void available() {
                notified.set(true);
            }

            @Override
            public void unavailable() {}
        };

        databaseAvailabilityGuard.addListener(availabilityListener);

        // When
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);

        // Then
        assertTrue(notified.get());
    }

    @Test
    void givenAccessGuardWithConditionWhenGrantAndDenyThenNotifyListeners() {
        // Given
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);
        databaseAvailabilityGuard.require(REQUIREMENT_1);

        final AtomicBoolean notified = new AtomicBoolean();
        AvailabilityListener availabilityListener = new AvailabilityListener() {
            @Override
            public void available() {}

            @Override
            public void unavailable() {
                notified.set(true);
            }
        };

        databaseAvailabilityGuard.addListener(availabilityListener);

        // When
        databaseAvailabilityGuard.fulfill(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_1);

        // Then
        assertTrue(notified.get());
    }

    @Test
    void shouldExplainWhoIsBlockingAccess() {
        // Given
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard(clock, log);

        // When
        databaseAvailabilityGuard.require(REQUIREMENT_1);
        databaseAvailabilityGuard.require(REQUIREMENT_2);

        // Then
        assertThat(databaseAvailabilityGuard.describe())
                .isEqualTo("2 reasons for blocking: Requirement 1, Requirement 2.");
    }

    @Test
    void shouldWaitForAvailabilityWhenShutdown() throws Exception {
        // very long timeout to force blocking
        var waitMs = DAYS.toMillis(1);
        var availabilityGuard = createAvailabilityGuard(clock, log);

        availabilityGuard.init();
        availabilityGuard.start();

        assertFalse(availabilityGuard.isShutdown());
        assertTrue(availabilityGuard.isAvailable(waitMs));

        availabilityGuard.stop();
        availabilityGuard.shutdown();

        assertTrue(availabilityGuard.isShutdown());

        // check isAvailable in a separate thread with a very long timeout; this thread should keep polling and sleeping
        var isAvailableFuture = supplyAsync(() -> availabilityGuard.isAvailable(waitMs));
        SECONDS.sleep(1);
        assertFalse(isAvailableFuture.isDone());

        // start the guard, this should make the polling thread exit
        availabilityGuard.init();
        availabilityGuard.start();
        assertTrue(isAvailableFuture.get(5, SECONDS));
    }

    private static void verifyLogging(InternalLog log, VerificationMode mode) {
        verify(log, mode).info(anyString(), Mockito.any(Object[].class));
    }

    private DatabaseAvailabilityGuard getDatabaseAvailabilityGuard(Clock clock, InternalLog log) {
        DatabaseAvailabilityGuard availabilityGuard = createAvailabilityGuard(clock, log);
        life.add(availabilityGuard);
        return availabilityGuard;
    }

    private static DatabaseAvailabilityGuard createAvailabilityGuard(Clock clock, InternalLog log) {
        return new DatabaseAvailabilityGuard(
                from(DEFAULT_DATABASE_NAME, UUID.randomUUID()),
                clock,
                log,
                0,
                mock(CompositeDatabaseAvailabilityGuard.class));
    }
}
