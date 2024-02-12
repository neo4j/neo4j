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
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.time.FakeClock;

class CircuitBreakerTest {

    @TestFactory
    Stream<DynamicTest> shouldTripWhenThresholdExceeded() {
        return IntStream.rangeClosed(1, 8)
                .mapToObj(threshold -> DynamicTest.dynamicTest(String.format("%d invocations", threshold), () -> {
                    var listener = Mockito.mock(CircuitBreaker.Listener.class);
                    var order = Mockito.inOrder(listener);

                    var cb = new CircuitBreaker(threshold, 30000, 60000, listener, Clock.systemUTC());

                    // advance circuit breaker to its threshold
                    for (var i = 0; i < threshold; ++i) {
                        cb.increment();

                        order.verify(listener, Mockito.never()).onTripped();
                    }

                    // increment one more time and verify that it tripped now
                    cb.increment();

                    order.verify(listener).onTripped();

                    // further increments will not trip the breaker again
                    for (var i = 0; i < 3; ++i) {
                        cb.increment();

                        order.verify(listener, Mockito.never()).onTripped();
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldContinueWithinTimerResetWindow() {
        return IntStream.rangeClosed(1, 8)
                .mapToObj(threshold -> DynamicTest.dynamicTest(String.format("%d invocations", threshold), () -> {
                    var listener = Mockito.mock(CircuitBreaker.Listener.class);
                    var order = Mockito.inOrder(listener);

                    var clock = new FakeClock();

                    var cb = new CircuitBreaker(threshold, 500, 5000, listener, clock);

                    // exceed threshold first
                    for (var i = 0; i <= threshold; ++i) {
                        cb.increment();
                    }

                    order.verify(listener).onTripped();

                    for (var i = 0; i < 8; ++i) {
                        clock.forward(100, TimeUnit.MILLISECONDS);

                        cb.increment();

                        order.verify(listener).onContinue();
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldContinueWithinResetWindow() {
        return IntStream.rangeClosed(1, 8)
                .mapToObj(threshold -> DynamicTest.dynamicTest(String.format("%d invocations", threshold), () -> {
                    var listener = Mockito.mock(CircuitBreaker.Listener.class);
                    var order = Mockito.inOrder(listener);

                    var clock = new FakeClock();

                    var cb = new CircuitBreaker(threshold, 500, 5000, listener, clock);

                    // exceed threshold first
                    for (var i = 0; i <= threshold; ++i) {
                        cb.increment();
                    }

                    order.verify(listener).onTripped();

                    for (var i = 0; i < 5; ++i) {
                        clock.forward(500, TimeUnit.MILLISECONDS);

                        cb.increment();

                        order.verify(listener).onContinue();
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldResetAfterTimeout() {
        return IntStream.rangeClosed(1, 8)
                .mapToObj(threshold -> DynamicTest.dynamicTest(String.format("%d invocations", threshold), () -> {
                    var listener = Mockito.mock(CircuitBreaker.Listener.class);
                    var order = Mockito.inOrder(listener);

                    var clock = new FakeClock();

                    var cb = new CircuitBreaker(threshold, 500, 500, listener, clock);

                    // exceed threshold first
                    for (var i = 0; i <= threshold; ++i) {
                        cb.increment();
                    }

                    order.verify(listener).onTripped();

                    clock.forward(500, TimeUnit.MILLISECONDS);

                    cb.increment();

                    order.verify(listener).onReset();
                }));
    }
}
