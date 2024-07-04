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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.time.FakeClock;

class IdleTimeoutServiceTest {

    private final Duration delay = Duration.ofMillis(50);
    private final Duration timeout = Duration.ofHours(3);
    private RecordingTimeout timeoutAction;
    private FakeTicker ticker;
    private IdleTimeoutService service;

    @BeforeEach
    void setUp() {
        ticker = new FakeTicker();
        timeoutAction = new RecordingTimeout();
        service = new IdleTimeoutServiceImpl(ticker, timeout, Duration.ofMillis(0), delay, timeoutAction);
        service.resume();
    }

    @AfterEach
    void tearDown() throws Exception {
        service.close();
    }

    @Test
    void timeout() {
        assertThat(timeoutAction.timedOut.get()).isEqualTo(false);
        ticker.forward(timeout.plusMillis(1));
        assertTimedOut();
    }

    @Test
    void doNotTimeout() {
        ticker.forward(timeout);
        assertNotTimedOut();
    }

    @Test
    void doNotTimeoutIfPaused() {
        ticker.forward(timeout);
        service.pause();
        ticker.forward(timeout);
        assertNotTimedOut();
    }

    @Test
    void timeoutAfterResume() {
        for (int i = 0; i < 100; ++i) {
            ticker.forward(timeout);
            service.pause();
            ticker.forward(i, TimeUnit.HOURS);
            service.resume();
        }
        assertNotTimedOut();
        ticker.forward(timeout.plusMillis(1));
        assertTimedOut();
    }

    @Test
    void timeoutAfterAwake() {
        for (int i = 0; i < 100; ++i) {
            ticker.forward(timeout);
            service.imAwake();
        }
        assertNotTimedOut();
        ticker.forward(timeout.plusMillis(1));
        assertTimedOut();
    }

    private void assertTimedOut() {
        await().timeout(Duration.ofSeconds(20)).untilTrue(timeoutAction.timedOut);
    }

    private void assertNotTimedOut() {
        await().pollDelay(delay.multipliedBy(20)) // Give it some time before asserting
                .timeout(Duration.ofSeconds(20))
                .untilFalse(timeoutAction.timedOut);
    }

    private static class RecordingTimeout implements Runnable {
        public final AtomicBoolean timedOut = new AtomicBoolean(false);

        @Override
        public void run() {
            timedOut.set(true);
        }
    }

    private static class FakeTicker extends FakeClock implements IdleTimeoutServiceImpl.Ticker {

        @Override
        public long get() {
            return nanos();
        }
    }
}
