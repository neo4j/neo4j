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
package org.neo4j.bolt.test.extension.handler;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.neo4j.bolt.test.extension.error.TestRetryException;
import org.neo4j.bolt.test.extension.store.RetryInfo;

/**
 * Marks tests which failed for permissible reasons (such as temporary network conditions) as
 * aborted and signals to the Bolt test support extension to re-schedule test execution.
 */
public abstract class AbstractRetryingTestExecutionExceptionHandler implements TestExecutionExceptionHandler {

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        var info = RetryInfo.getOrCreate(context, () -> this.createRetryInfo(context, throwable));

        if (info.mayRetry() && this.isRetryable(context, info, throwable)) {
            info = info.increment();
            info.publish(context);

            throw new TestRetryException(info, this.getMessage(context, info, throwable), throwable);
        }

        throw throwable;
    }

    /**
     * Initializes a new retry information object within the context of the current test.
     * <p/>
     * This method effectively controls the maximum permissible number of retries for a given
     * permissible failure. If multiple handlers are executed for a given test, the initial handler's
     * maximum retry policy will take precedence.
     *
     * @param context   the context of the current test.
     * @param throwable the cause of the first failure.
     * @return a retry info object.
     */
    protected RetryInfo createRetryInfo(ExtensionContext context, Throwable throwable) {
        return new RetryInfo(0, 5);
    }

    /**
     * Retrieves a message explaining the reason for the retry and, optimally, why retries are
     * permissible for the current failure case.
     *
     * @param context   the context of the current test.
     * @param info      the test's retry information.
     * @param throwable the cause of this failure.
     * @return a message identifying the cause and criteria of this retry.
     */
    protected abstract String getMessage(ExtensionContext context, RetryInfo info, Throwable throwable);

    /**
     * Evaluates whether a given test failure is considered retryable by this handler.
     *
     * @param context the context of the current test.
     * @param info    the test's retry information.
     * @param cause   the cause of this failure.
     * @return true if a retry are permitted, false otherwise.
     */
    protected abstract boolean isRetryable(ExtensionContext context, RetryInfo info, Throwable cause);

    /**
     * Evaluates whether a test's failure is caused by an exception of the given base type.
     * <p/>
     * This method also considers {@link AssertionError} instances to be valid so long as their cause
     * is of the specified exception type.
     *
     * @param type a desired exception type.
     * @param ex   a failure cause.
     * @return true if the given failure cause is of the desired type or if an AssertionError was was
     * directly caused by the given type.
     */
    static boolean hasDirectCause(Class<? extends Throwable> type, Throwable ex) {
        if (type.isInstance(ex)) {
            return true;
        }

        var current = ex;
        do {
            if (current instanceof AssertionError) {
                var cause = current.getCause();
                if (type.isInstance(cause)) {
                    return true;
                }
            }

            current = current.getCause();
        } while (current != null);

        return false;
    }
}
