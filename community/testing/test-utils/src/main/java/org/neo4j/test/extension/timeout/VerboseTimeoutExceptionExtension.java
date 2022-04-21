/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import org.junit.jupiter.api.extension.TestWatcher;

public class VerboseTimeoutExceptionExtension implements TestWatcher {
    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        if (isTimeout(cause)) {
            cause.addSuppressed(new ThreadDump(format(
                    "Test %s-%s timed out. ", context.getRequiredTestMethod().getName(), context.getDisplayName())));
        }
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

    static class ThreadDump extends RuntimeException {
        ThreadDump(String header) {
            super(header + threadDump());
            this.setStackTrace(new StackTraceElement[0]);
        }
    }
}
