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
package org.neo4j.bolt.protocol.common.connector.accounting.error;

import java.time.Clock;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public final class CircuitBreakerErrorAccountant implements ErrorAccountant {

    /**
     * Defines the amount of time which has to pass before a circuit breaker log line is repeated
     * to indicate continuation of a condition.
     */
    private static final long REPEAT_LOG_LINE_MILLIS = 5 * 60 * 1000;

    /**
     * Counts the number of network aborts (e.g. connection reset by peer, timeouts, etc) encountered
     * within a given accounting time period.
     */
    private final CircuitBreaker networkAbortCircuitBreaker;

    /**
     * Counts the number of thread starvation events (e.g. requests which could not be served as a
     * result of all worker threads being unavailable).
     */
    private final CircuitBreaker threadStarvationCircuitBreaker;

    private final Log userLog;

    public CircuitBreakerErrorAccountant(
            long networkAbortThreshold,
            long networkAbortSampleDurationMillis,
            long networkAbortResetDurationMillis,
            long threadStarvationThreshold,
            long threadStarvationSampleDurationMillis,
            long threadStarvationResetDurationMillis,
            Clock clock,
            LogService logging) {
        this.networkAbortCircuitBreaker = new CircuitBreaker(
                networkAbortThreshold,
                networkAbortSampleDurationMillis,
                networkAbortResetDurationMillis,
                new NetworkAbortCircuitBreakerListener(networkAbortThreshold, networkAbortSampleDurationMillis),
                clock);
        this.threadStarvationCircuitBreaker = new CircuitBreaker(
                threadStarvationThreshold,
                threadStarvationSampleDurationMillis,
                threadStarvationResetDurationMillis,
                new ThreadStarvationCircuitBreakerListener(
                        threadStarvationThreshold, threadStarvationSampleDurationMillis),
                clock);

        this.userLog = logging.getUserLog(ErrorAccountant.class);
    }

    public void notifyNetworkAbort(Connection connection, Throwable cause) {
        this.userLog.debug("[" + connection.id() + "] Terminating connection due to network error", cause);

        this.networkAbortCircuitBreaker.increment();
    }

    public void notifyThreadStarvation(Connection connection, Throwable cause) {
        this.userLog.debug(
                "[%s] Unable to schedule for execution since there are no available threads to serve it at the "
                        + "moment.",
                connection.id());

        this.threadStarvationCircuitBreaker.increment();
    }

    private abstract class ContinuallyLoggingCircuitBreakerListener implements CircuitBreaker.Listener {
        private volatile long lastLogLine;

        @Override
        public void onContinue() {
            var now = System.currentTimeMillis();
            if (this.lastLogLine == 0 || now - lastLogLine < REPEAT_LOG_LINE_MILLIS) {
                return;
            }

            this.lastLogLine = now;
            this.doOnContinue();
        }

        @Override
        public void onReset() {
            this.lastLogLine = 0;
        }

        protected abstract void doOnContinue();
    }

    private class NetworkAbortCircuitBreakerListener extends ContinuallyLoggingCircuitBreakerListener {
        private final long threshold;
        private final long window;

        public NetworkAbortCircuitBreakerListener(long threshold, long window) {
            this.threshold = threshold;
            this.window = window;
        }

        @Override
        public void onTripped() {
            userLog.error(
                    "Increase in network aborts detected (more than %d network related connection aborts over a period of %d ms) - This may indicate an issue with the network environment or an overload condition",
                    this.threshold, this.window);
        }

        @Override
        protected void doOnContinue() {
            userLog.error(
                    "Network abort rate remains increased (more than %d network related connection aborts over a period of %d ms) - This may indicate an issue with the network environment or an overload condition",
                    this.threshold, this.window);
        }

        @Override
        public void onReset() {
            super.onReset();

            userLog.info("Network abort rate has normalized");
        }
    }

    private class ThreadStarvationCircuitBreakerListener extends ContinuallyLoggingCircuitBreakerListener {
        private final long threshold;
        private final long window;

        public ThreadStarvationCircuitBreakerListener(long threshold, long window) {
            this.threshold = threshold;
            this.window = window;
        }

        @Override
        public void onTripped() {
            userLog.error(
                    "Increase in thread starvation events detected (%d events over a period of %d ms) - This may indicate an overload condition",
                    this.threshold, this.window);
        }

        @Override
        protected void doOnContinue() {
            userLog.error(
                    "Thread starvation event rate remains increased (more than %d events over a period of %d ms) - This may indicate an overload condition",
                    this.threshold, this.window);
        }

        @Override
        public void onReset() {
            super.onReset();

            userLog.info("Thread starvation event rate has normalized");
        }
    }
}
