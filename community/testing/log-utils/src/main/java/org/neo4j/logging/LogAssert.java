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
package org.neo4j.logging;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.Exceptions.stringify;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.util.Throwables;
import org.neo4j.logging.AssertableLogProvider.LogCall;
import org.neo4j.time.Stopwatch;

public class LogAssert extends AbstractAssert<LogAssert, AssertableLogProvider> {
    private Class<?> loggerClazz;
    private AssertableLogProvider.Level logLevel;

    public LogAssert(AssertableLogProvider logProvider) {
        super(logProvider, LogAssert.class);
    }

    public LogAssert forClass(Class<?> clazz) {
        loggerClazz = clazz;
        return this;
    }

    public LogAssert forLevel(AssertableLogProvider.Level level) {
        this.logLevel = level;
        return this;
    }

    public LogAssert containsMessages(String... messages) {
        isNotNull();
        for (String message : messages) {
            if (!haveMessage(message)) {
                failWithMessage(
                        "Expected log to contain messages: `%s` but no matches found in:%n%s",
                        Arrays.toString(messages), actual.serialize());
            }
        }
        return this;
    }

    public LogAssert containsMessagesEventually(long maxWaitTimoutMs, String... messages) throws InterruptedException {
        isNotNull();
        Stopwatch stopwatch = Stopwatch.start();
        for (String message : messages) {
            while (!haveMessage(message)) {
                if (stopwatch.hasTimedOut(maxWaitTimoutMs, TimeUnit.MILLISECONDS)) {
                    failWithMessage(
                            "Expected log to contain messages: `%s` but no matches found in:%n%s",
                            Arrays.toString(messages), actual.serialize());
                }
                Thread.sleep(10);
            }
        }
        return this;
    }

    public LogAssert containsMessages(Predicate<String> predicate) {
        isNotNull();
        if (!haveMessage(predicate)) {
            failWithMessage(
                    "Expected log to contain messages: `%s` but no matches found in:%n%s",
                    predicate, actual.serialize());
        }
        return this;
    }

    public final LogAssert containsMessages(Pattern pattern, Predicate<String>... valueTests) {
        isNotNull();
        var predicate = new Predicate<String>() {
            @Override
            public boolean test(String s) {
                var matcher = pattern.matcher(s);
                while (matcher.find()) {
                    if (matchesExpectedValues(matcher)) {
                        return true;
                    }
                }
                return false;
            }

            private boolean matchesExpectedValues(Matcher matcher) {
                var groupCount = matcher.groupCount();
                for (int i = 0; i < groupCount && i < valueTests.length; i++) {
                    if (!valueTests[i].test(matcher.group(1 + i))) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public String toString() {
                return String.format("Pattern:%s, valueTests:%s", pattern.pattern(), Arrays.toString(valueTests));
            }
        };

        return containsMessages(predicate);
    }

    public LogAssert containsMessagesInOrder(String... messages) {
        isNotNull();
        int prevIndex = -1;
        for (String message : messages) {
            int index = messageIndex(message, prevIndex);
            if (index < 0) {
                if (!haveMessage(message)) {
                    failWithMessage(
                            "Expected log to contain messages: `%s` but no matches found in:%n%s",
                            Arrays.toString(messages), actual.serialize());
                } else {
                    failWithMessage(
                            "Expected log to contain: `%s` in order `%s` but was not matching:%n%s",
                            message, Arrays.toString(messages), actual.serialize());
                }
            }
            prevIndex = index;
        }
        return this;
    }

    public LogAssert containsMessagesOnce(String... messages) {
        isNotNull();
        for (String message : messages) {
            long messageMatchCount = messageMatchCount(message);
            if (messageMatchCount != 1) {
                if (messageMatchCount == 0) {
                    failWithMessage(
                            "Expected log to contain messages: `%s` exactly once but no matches found in:%n%s",
                            Arrays.toString(messages), actual.serialize());
                } else {
                    failWithMessage(
                            "Expected log to contain messages: `%s` exactly once but %d matches found in:%n%s",
                            Arrays.toString(messages), messageMatchCount, actual.serialize());
                }
            }
        }
        return this;
    }

    public AbstractLongAssert messageCount(String... messages) {
        return assertThat(Stream.of(messages).mapToLong(this::messageMatchCount).sum());
    }

    public LogAssert doesNotHaveAnyLogs() {
        isNotNull();
        if (actual.getLogCalls().stream().anyMatch(call -> matchedLogger(call) && matchedLevel(call))) {
            failWithMessage("Expected log to be empty but following log calls were recorded:%n%s", actual.serialize());
        }
        return this;
    }

    public LogAssert doesNotContainMessage(String message) {
        isNotNull();
        if (haveMessage(message)) {
            failWithMessage("Unexpected log message: `%s` in:%n%s", message, actual.serialize());
        }
        return this;
    }

    public LogAssert onlyContainsMessages(String... messages) {
        isNotNull();
        if (!actual.getLogCalls().stream()
                .filter(call -> matchedLogger(call) && matchedLevel(call))
                .allMatch(call -> Stream.of(messages).anyMatch(message -> matchedMessage(message, call)))) {
            failWithMessage(
                    "Expected log to only contain messages:%n%s%nbut found:%n%s",
                    Arrays.toString(messages), actual.serialize());
        }
        return this;
    }

    public LogAssert containsMessageWithArguments(String message, Object... arguments) {
        isNotNull();
        if (!haveMessageWithArguments(message, arguments)) {
            failWithMessage(
                    "Expected log to contain messages: `%s` with arguments: `%s`. " + "But no matches found in:%n%s",
                    message, Arrays.toString(arguments), actual.serialize());
        }
        return this;
    }

    public LogAssert containsMessageWithArgumentsContaining(String message, Object... arguments) {
        isNotNull();
        if (!haveMessageWithArgumentsContaining(message, arguments)) {
            failWithMessage(
                    "Expected log to contain messages: `%s` with arguments containing: `%s`. "
                            + "But no matches found in:%n%s",
                    message, Arrays.toString(arguments), actual.serialize());
        }
        return this;
    }

    public LogAssert containsMessageWithArgumentsMatching(String message, Predicate<Object[]> argumentMatcher) {
        isNotNull();
        if (!haveMessageWithArgumentsMatching(message, argumentMatcher)) {
            failWithMessage(
                    "Expected log to contain messages: `%s` with arguments matching predicate. "
                            + "But no matches found in:%n%s",
                    message, actual.serialize());
        }
        return this;
    }

    public LogAssert doesNotContainMessageWithArguments(String message, Object... arguments) {
        isNotNull();
        if (haveMessageWithArguments(message, arguments)) {
            failWithMessage(
                    "Unexpected log message: `%s` with arguments: `%s` " + " in:%n%s",
                    message, Arrays.toString(arguments), actual.serialize());
        }
        return this;
    }

    /**
     * Checks that there is a message that contains all the supplies message snippets.
     */
    public LogAssert containsMessageWithAll(String... snippets) {
        isNotNull();

        var logCalls = actual.getLogCalls();
        boolean matched = logCalls.stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && Arrays.stream(snippets).allMatch(snippet -> matchedMessage(snippet, call)));

        if (!matched) {
            failWithMessage(
                    "Expected log to contain a message containing: `%s`. " + "But no matches found in:%s",
                    Arrays.toString(snippets), actual.serialize());
        }

        return this;
    }

    /**
     * Checks that there is only one message that contains all the supplies message snippets.
     */
    public LogAssert containsMessageWithAllOnce(String... snippets) {
        isNotNull();

        var logCalls = actual.getLogCalls();
        boolean matched = logCalls.stream()
                        .filter(call -> matchedLogger(call)
                                && matchedLevel(call)
                                && Arrays.stream(snippets).allMatch(snippet -> matchedMessage(snippet, call)))
                        .count()
                == 1;

        if (!matched) {
            failWithMessage(
                    "Expected log to contain a message containing: `%s`. " + "But more than one matches found in:%s",
                    Arrays.toString(snippets), actual.serialize());
        }

        return this;
    }

    public LogAssert eachMessageContains(String message) {
        isNotNull();
        for (LogCall logCall : actual.getLogCalls()) {
            if (!matchedMessage(message, logCall)) {
                failWithMessage(
                        "Expected each log message to contain '%s', but message '%s' doesn't",
                        message, logCall.toLogLikeString());
            }
        }
        return this;
    }

    public AbstractThrowableAssert<?, ? extends Throwable> assertExceptionForLogMessage(String message) {
        isNotNull();
        haveMessage(message);
        var logCall = actual.getLogCalls().stream()
                .filter(call -> matchedLogger(call) && matchedLevel(call) && matchedMessage(message, call))
                .findFirst();
        if (logCall.isEmpty()) {
            failWithMessage("Expected log call with message `%s` not found in:%n%s.", message, actual.serialize());
        }
        return assertThat(logCall.get().getThrowable());
    }

    public LogAssert containsMessageWithException(String message, Throwable t) {
        isNotNull();
        requireNonNull(t);
        if (!haveMessageWithException(message, t)) {
            failWithMessage(
                    "Expected log to contain message `%s` with exception: `%s`. But no matches found in:%n%s",
                    message, stringify(t), actual.serialize());
        }
        return this;
    }

    public LogAssert containsMessageWithoutException(String message) {
        isNotNull();
        if (!haveMessageWithoutException(message)) {
            failWithMessage(
                    "Expected log to contain message `%s` without an exception. But no matches found in:%n%s",
                    message, actual.serialize());
        }
        return this;
    }

    public LogAssert containsMessageWithExceptionMatching(String message, Predicate<Throwable> predicate) {
        isNotNull();
        requireNonNull(predicate);
        if (!haveMessageWithExceptionMatching(message, predicate)) {
            failWithMessage(
                    "Expected log to contain message `%s` with exception matching predicate. But no matches "
                            + "found in:%n%s",
                    message, actual.serialize());
        }
        return this;
    }

    public LogAssert containsMessageWithExceptionWithCause(String message, Throwable t) {
        isNotNull();
        requireNonNull(t);
        if (!haveMessageWithExceptionWithCause(message, t)) {
            failWithMessage(
                    "Expected log to contain message `%s` with exception with cause `%s`. But no matches found "
                            + "in:%n%s",
                    message, stringify(t), actual.serialize());
        }
        return this;
    }

    public LogAssert containsException(Throwable t) {
        requireNonNull(t);
        isNotNull();
        if (actual.getLogCalls().stream()
                .noneMatch(call -> matchedLogger(call) && matchedLevel(call) && t.equals(call.getThrowable()))) {
            failWithMessage(
                    "Expected log to contain exception: `%s`. But no matches found in:%n%s",
                    stringify(t), actual.serialize());
        }
        return this;
    }

    private boolean haveMessageWithException(String message, Throwable t) {
        return actual.getLogCalls().stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && t.equals(call.getThrowable())
                        && matchedMessage(message, call));
    }

    private boolean haveMessageWithoutException(String message) {
        return actual.getLogCalls().stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && call.getThrowable() == null
                        && matchedMessage(message, call));
    }

    private boolean haveMessageWithExceptionMatching(String message, Predicate<Throwable> predicate) {
        return actual.getLogCalls().stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && predicate.test(call.getThrowable())
                        && matchedMessage(message, call));
    }

    private boolean haveMessageWithExceptionWithCause(String message, Throwable cause) {
        return actual.getLogCalls().stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && cause.equals(Throwables.getRootCause(call.getThrowable()))
                        && matchedMessage(message, call));
    }

    private boolean haveMessageWithArguments(String message, Object... arguments) {
        var logCalls = actual.getLogCalls();
        return logCalls.stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && matchedArguments(call, arguments)
                        && matchedMessage(message, call));
    }

    private boolean haveMessageWithArgumentsContaining(String message, Object... arguments) {
        var logCalls = actual.getLogCalls();
        return logCalls.stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && matchedArgumentsContains(call, arguments)
                        && matchedMessage(message, call));
    }

    private boolean haveMessageWithArgumentsMatching(String message, Predicate<Object[]> argumentMatcher) {
        var logCalls = actual.getLogCalls();
        return logCalls.stream()
                .anyMatch(call -> matchedLogger(call)
                        && matchedLevel(call)
                        && matchedMessage(message, call)
                        && argumentMatcher.test(call.getArguments()));
    }

    private boolean haveMessage(String message) {
        return haveMessage(logMessage -> logMessage.contains(message));
    }

    private boolean haveMessage(Predicate<String> message) {
        var logCalls = actual.getLogCalls();
        return logCalls.stream()
                .anyMatch(call -> matchedLogger(call) && matchedLevel(call) && matchedMessage(message, call));
    }

    private int messageIndex(String message, int startIndex) {
        var logCalls = actual.getLogCalls();
        int index = 0;
        for (LogCall call : logCalls) {
            if (index >= startIndex && matchedLogger(call) && matchedLevel(call) && matchedMessage(message, call)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    private long messageMatchCount(String message) {
        var logCalls = actual.getLogCalls();
        return logCalls.stream()
                .filter(call -> matchedLogger(call) && matchedLevel(call) && matchedMessage(message, call))
                .count();
    }

    private static boolean matchedArgumentsContains(LogCall call, Object[] arguments) {
        Object[] callArguments = call.getArguments();
        for (int i = 0; i < arguments.length; i++) {
            if (!Objects.toString(callArguments[i]).contains(Objects.toString(arguments[i]))) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchedArguments(LogCall call, Object[] arguments) {
        return Arrays.equals(call.getArguments(), arguments);
    }

    private static boolean matchedMessage(String message, LogCall call) {
        return matchedMessage(logMessage -> logMessage.contains(message), call);
    }

    private static boolean matchedMessage(Predicate<String> predicate, LogCall call) {
        return predicate.test(call.getMessage()) || predicate.test(call.toLogLikeString());
    }

    private boolean matchedLogger(LogCall call) {
        return loggerClazz == null
                || loggerClazz.getName().equals(call.getContext())
                || loggerClazz.getSimpleName().equals(call.getContext());
    }

    private boolean matchedLevel(LogCall call) {
        return logLevel == null || logLevel == call.getLevel();
    }
}
