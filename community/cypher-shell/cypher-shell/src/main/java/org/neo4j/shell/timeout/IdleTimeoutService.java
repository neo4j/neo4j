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

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.util.VisibleForTesting;

/** Terminates the application on idle timeout. */
public interface IdleTimeoutService extends AutoCloseable {
    /** Signals that user is not idling. */
    void imAwake();

    /** Pause this service, after this is called we won't timeout until resumed.  */
    void pause();

    /** Enable timeouts after pause. */
    void resume();

    /**
     * Create a new idle timeout service in the paused state.
     * If timeout is zero or negative a noop service is returned.
     */
    static IdleTimeoutService create(Duration idleTimeout, Duration delay) {
        if (idleTimeout != null && !idleTimeout.isZero() && !idleTimeout.isNegative()) {
            return new IdleTimeoutServiceImpl(idleTimeout, delay);
        } else {
            return noTimeout();
        }
    }

    static IdleTimeoutService noTimeout() {
        return new IdleTimeoutService() {
            @Override
            public void imAwake() {}

            @Override
            public void pause() {}

            @Override
            public void resume() {}

            @Override
            public void close() {}
        };
    }
}

class IdleTimeoutServiceImpl implements IdleTimeoutService {
    private static final long DEFAULT_SIGNIFICANT_DURATION_NS = TimeUnit.SECONDS.toNanos(10);
    private final AtomicLong lastSeenNs;
    private final AtomicBoolean paused = new AtomicBoolean(true);
    private final long timeoutNs;
    private final Ticker ticker;
    private final ScheduledExecutorService timeoutExecutor;
    private final Runnable timeoutAction;
    private final long significantDurationNs;

    IdleTimeoutServiceImpl(Duration timeout, Duration delay) {
        this(System::nanoTime, timeout, timeout, delay, new ExitOnTimeout());
    }

    @VisibleForTesting
    IdleTimeoutServiceImpl(
            Ticker ticker, Duration timeout, Duration initialDelay, Duration delay, Runnable timeoutAction) {
        final var actualDelayMs = calculateDelayMs(timeout.toMillis(), delay.toMillis());
        this.ticker = ticker;
        this.timeoutNs = timeout.toNanos();
        this.timeoutAction = timeoutAction;
        this.lastSeenNs = new AtomicLong(ticker.get());
        // For testing purposes we use lower significantDurationNs with really small timeouts.
        this.significantDurationNs =
                timeoutNs > (10 * DEFAULT_SIGNIFICANT_DURATION_NS) ? DEFAULT_SIGNIFICANT_DURATION_NS : 0;
        timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        timeoutExecutor.scheduleWithFixedDelay(
                this::exitOnIdleTimeout, initialDelay.toMillis(), actualDelayMs, TimeUnit.MILLISECONDS);
    }

    // To not confuse users who just want to try out the feature with very low timeouts we adapt the delay here
    private static long calculateDelayMs(long timeoutMs, long delayMs) {
        final var timeoutDividedByFour = timeoutMs / 4;
        if (delayMs <= timeoutDividedByFour) {
            return delayMs;
        } else {
            final var minDelayMs = 1000;
            return Math.max(minDelayMs, timeoutDividedByFour);
        }
    }

    @Override
    public void pause() {
        paused.set(true);
    }

    @Override
    public void resume() {
        lastSeenNs.set(ticker.get());
        paused.set(false);
    }

    @Override
    public void imAwake() {
        if (!paused.get() && elapsed() > significantDurationNs) {
            this.lastSeenNs.set(ticker.get());
        }
    }

    private void exitOnIdleTimeout() {
        if (!paused.get() && elapsed() > timeoutNs) {
            timeoutAction.run();
        }
    }

    private long elapsed() {
        return ticker.get() - lastSeenNs.get();
    }

    @Override
    public void close() {
        timeoutExecutor.shutdownNow();
    }

    private static class ExitOnTimeout implements Runnable {

        @Override
        public void run() {
            System.err.println(
                    "Timeout after idling, avoid this by increasing --idle-timeout or omitting it completely.");
            System.exit(124);
        }
    }

    interface Ticker {
        long get();
    }
}
