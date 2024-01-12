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
    private static final ExtensionContext.Namespace THREAD_DUMPER_NAMESPACE =
            ExtensionContext.Namespace.create("threadDumper");
    private static final String DISABLED = "disabled";

    public static void disable(ExtensionContext context) {
        context.getRoot().getStore(THREAD_DUMPER_NAMESPACE).put(DISABLED, DISABLED);
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleFailure(context, context.getRequiredTestMethod().getName(), throwable);
        throw throwable;
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        handleFailure(context, "BeforeAll", throwable);
        throw throwable;
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        handleFailure(context, "BeforeEach", throwable);
        throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {
        handleFailure(context, "AfterEach", throwable);
        throw throwable;
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        handleFailure(context, "AfterAll", throwable);
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

    private static void handleFailure(ExtensionContext context, String testMethod, Throwable cause) {
        if (isEnabled(context) && isTimeout(cause)) {
            var displayName = context.getDisplayName();
            cause.addSuppressed(new ThreadDump(format("Test %s-%s timed out. ", displayName, testMethod)));
        }
    }

    private static boolean isEnabled(ExtensionContext context) {
        return context.getRoot().getStore(THREAD_DUMPER_NAMESPACE).get(DISABLED) == null;
    }

    static class ThreadDump extends RuntimeException {
        ThreadDump(String header) {
            super(header + threadDump());
            this.setStackTrace(new StackTraceElement[0]);
        }
    }
}
