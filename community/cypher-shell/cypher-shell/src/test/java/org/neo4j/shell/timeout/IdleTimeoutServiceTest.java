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
package org.neo4j.shell.timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.time.FakeClock;

class IdleTimeoutServiceTest {

    private static final Duration DEFAULT_DELAY = Duration.ofMillis(50);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofHours(3);
    private RecordingTimeout timeoutAction;
    private AwakenAction awakenAction;

    private FakeTicker ticker;
    private IdleTimeoutService service;

    @BeforeEach
    void setUp() throws Exception {
        startService(DEFAULT_TIMEOUT, DEFAULT_DELAY);
    }

    void startService(Duration timeout, Duration delay) throws Exception {
        if (service != null) service.close();
        ticker = new FakeTicker();
        timeoutAction = new RecordingTimeout(ticker);
        awakenAction = new AwakenAction();
        service = new IdleTimeoutServiceImpl(ticker, timeout, Duration.ZERO, delay, timeoutAction, awakenAction);
        service.resume();
    }

    @AfterEach
    void tearDown() throws Exception {
        service.close();
    }

    @Test
    void timeout() {
        assertThat(timeoutAction.hasTimedOut()).isEqualTo(false);
        ticker.forward(DEFAULT_TIMEOUT.plusMillis(1));
        assertTimedOut();
    }

    @Test
    void doNotTimeout() {
        ticker.forward(DEFAULT_TIMEOUT);
        assertNotTimedOut();
    }

    @Test
    void doNotTimeoutIfPaused() {
        ticker.forward(DEFAULT_TIMEOUT);
        service.pause();
        ticker.forward(DEFAULT_TIMEOUT);
        assertNotTimedOut();
    }

    @Test
    @Disabled
    void timeoutAfterResume() {
        for (int i = 0; i < 100; ++i) {
            ticker.forward(DEFAULT_TIMEOUT);
            service.pause();
            ticker.forward(i, TimeUnit.HOURS);
            service.resume();
        }
        assertNotTimedOut();
        ticker.forward(DEFAULT_TIMEOUT.plusMillis(1));
        assertTimedOut();
    }

    @Test
    void timeoutAfterAwake() {
        for (int i = 0; i < 100; ++i) {
            ticker.forward(DEFAULT_TIMEOUT);
            service.imAwake();
        }
        assertNotTimedOut();
        ticker.forward(DEFAULT_TIMEOUT.plusMillis(1));
        assertTimedOut();
    }

    @Test
    void awakenActionCalled() {
        for (int i = 0; i < 100; ++i) {
            ticker.forward(DEFAULT_TIMEOUT);
            service.imAwake();
        }
        assertNotTimedOut();
        assertTrue(awakenAction.actionCalled.get());
    }

    @Test
    void smallTimeout() throws Exception {
        final var smallTimeout = Duration.ofSeconds(5);
        startService(smallTimeout, Duration.ofMinutes(10));
        ticker.forward(smallTimeout);
        assertNotTimedOut();
        ticker.forward(Duration.ofMillis(1));
        assertTimedOut();
    }

    private void assertTimedOut() {
        await().timeout(Duration.ofSeconds(20)).until(() -> timeoutAction.hasTimedOut());
    }

    private void assertNotTimedOut() {
        await().pollDelay(DEFAULT_DELAY.multipliedBy(20)) // Give it some time before asserting
                .timeout(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(timeoutAction.timedOutAt.get()).isEqualTo(Long.MIN_VALUE));
    }

    private static class RecordingTimeout implements Runnable {
        private final IdleTimeoutServiceImpl.Ticker ticker;
        public final AtomicLong timedOutAt = new AtomicLong(Long.MIN_VALUE);

        private RecordingTimeout(IdleTimeoutServiceImpl.Ticker ticker) {
            this.ticker = ticker;
        }

        @Override
        public void run() {
            timedOutAt.set(ticker.get());
        }

        public boolean hasTimedOut() {
            return timedOutAt.get() != Long.MIN_VALUE;
        }
    }

    private static class AwakenAction implements Runnable {
        public final AtomicBoolean actionCalled = new AtomicBoolean(false);

        @Override
        public void run() {
            actionCalled.set(true);
        }
    }

    private static class FakeTicker extends FakeClock implements IdleTimeoutServiceImpl.Ticker {

        @Override
        public long get() {
            return nanos();
        }
    }
}
