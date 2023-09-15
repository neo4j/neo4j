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

import static java.lang.String.format;
import static org.neo4j.internal.utils.DumpUtils.threadDump;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

public class VerboseTimeoutExceptionExtension
        implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable cause) throws Throwable {
        handleFailure(context.getDisplayName(), context.getRequiredTestMethod().getName(), cause);
        throw cause;
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        handleFailure(context.getDisplayName(), "BeforeAll", throwable);
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        handleFailure(context.getDisplayName(), "BeforeEach", throwable);
        throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        handleFailure(context.getDisplayName(), "AfterEach", throwable);
        throw throwable;
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleFailure(context.getDisplayName(), "AfterAll", throwable);
        throw throwable;
    }

    private static boolean isTimeout(Throwable throwable) {
        return throwable != null
                && (throwable instanceof TimeoutException
                        || throwable instanceof ConditionTimeoutException
                        || StringUtils.contains(throwable.getMessage(), "timed out")
                        || (!Objects.equals(throwable, throwable.getCause()) && isTimeout(throwable.getCause()))
                        || Arrays.stream(throwable.getSuppressed())
                                .anyMatch(VerboseTimeoutExceptionExtension::isTimeout));
    }

    private static void handleFailure(String displayName, String testMethod, Throwable cause) {
        if (isTimeout(cause)) {
            cause.addSuppressed(new ThreadDump(format("Test %s-%s timed out. ", displayName, testMethod)));
        }
    }

    static class ThreadDump extends RuntimeException {
        ThreadDump(String header) {
            super(header + threadDump());
            this.setStackTrace(new StackTraceElement[0]);
        }
    }
}
