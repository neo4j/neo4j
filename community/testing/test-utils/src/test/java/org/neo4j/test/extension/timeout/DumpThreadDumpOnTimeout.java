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
package org.neo4j.test.extension.timeout;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.opentest4j.AssertionFailedError;

class DumpThreadDumpOnTimeout {
    @Test
    void dumpOnTimeoutPreemptively() {
        assertTimeoutPreemptively(ofMillis(10), () -> {
            sleep(TimeUnit.MINUTES.toMillis(1));
        });
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MILLISECONDS)
    void dumpOnTimeoutAnnotation() throws InterruptedException {
        sleep(TimeUnit.MINUTES.toMillis(1));
    }

    @Test
    void dumpOnTimeoutException() throws TimeoutException {
        throw new TimeoutException();
    }

    @Test
    void dumpOnAssertEventually() {
        assertEventually(() -> false, equalityCondition(true), 100, TimeUnit.MILLISECONDS);
    }

    @Test
    void dumpOnAssertionFailedErrorWithMessage() {
        throw new AssertionFailedError(
                "foo() timed out after 20 minutes"); // mimic what is thrown by the default timeout
    }

    @Test
    void dumpOnCauseTimeout() {
        throw new RuntimeException(new TimeoutException());
    }

    @Test
    void dumpOnSuppressedTimeout() {
        RuntimeException exception = new RuntimeException();
        exception.addSuppressed(new TimeoutException());
        throw exception;
    }

    @Test
    void dumpOnDeepCauseTimeout() {
        RuntimeException exception = new RuntimeException(new TimeoutException());
        for (int i = 0; i < 10; i++) {
            exception = new RuntimeException(exception);
        }
        throw exception;
    }

    @Test
    void dumpOnDeepSuppressedTimeout() {
        RuntimeException exception = new RuntimeException();
        exception.addSuppressed(new TimeoutException());
        for (int i = 0; i < 10; i++) {
            exception = new RuntimeException(exception);
        }
        throw exception;
    }

    @Test
    void doNotDumpOnAssume() {
        assumeTrue(false);
    }

    @Test
    void doNotDumpOnAssert() {
        assertThat("foo").isEqualTo("bar");
    }

    @Test
    void doNotDumpOnException() {
        throw new RuntimeException("foo");
    }

    @Test
    void doNotDumpOnDeepException() {
        RuntimeException exception = new RuntimeException();
        for (int i = 0; i < 10; i++) {
            exception = new RuntimeException(exception);
        }
        throw exception;
    }

    @Nested
    class Before {
        @BeforeEach
        void setup() throws TimeoutException {
            throw new TimeoutException();
        }

        @Test
        void testWithoutTimeout() {}
    }

    @Nested
    class After {
        @AfterEach
        void tearDown() throws TimeoutException {
            throw new TimeoutException();
        }

        @Test
        void testWithoutTimeout() {}
    }

    @Nested
    class IncludeThreadsCleanedOnAfter {
        private final AtomicBoolean stop = new AtomicBoolean();
        private final AtomicBoolean started = new AtomicBoolean();
        private Thread thread;

        @AfterEach
        void cleanup() throws InterruptedException {
            stop.set(true);
            thread.join();
        }

        @Test
        void shouldContainHangingThread() throws TimeoutException {
            thread = new Thread(this::hangingMethod);
            thread.setName("HangingThread");
            thread.start();
            assertEventually(started::get, TRUE, 1, TimeUnit.MINUTES);
            throw new TimeoutException();
        }

        private void hangingMethod() {
            while (!stop.get()) {
                started.set(true);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    @Nested
    @ExtendWith(DisableThreadDump.class)
    class ThreadDumpingDisabled {
        @Test
        void testWithTimeout() throws TimeoutException {
            throw new TimeoutException();
        }
    }

    private static class DisableThreadDump implements TestInstancePostProcessor {
        @Override
        public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
            VerboseTimeoutExceptionExtension.disable(context);
        }
    }
}
