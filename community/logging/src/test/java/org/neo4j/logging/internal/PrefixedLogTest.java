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
package org.neo4j.logging.internal;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jLogMessage;
import org.neo4j.logging.Neo4jMessageSupplier;

class PrefixedLogTest {
    private final String prefix = "prefix";
    private final String format = "format %s %s";
    private final String message = "message";
    private final Object[] arguments = {"arg1", "arg2"};
    private final Exception exception = new Exception("exception");
    private final Neo4jLogMessage neo4jMessage = new TestNeo4jLogMessage();

    private final InternalLog mockedLog = mock(InternalLog.class);
    private final PrefixedLog prefixedLog = new PrefixedLog(prefix, mockedLog);

    @Test
    void shouldThrowIfNullPrefix() {
        assertThatThrownBy(() -> new PrefixedLog(null, mockedLog)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowIfNullLog() {
        assertThatThrownBy(() -> new PrefixedLog(prefix, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldDelegateForIsDebugEnabledMethod() {
        when(mockedLog.isDebugEnabled()).thenReturn(true);
        assertThat(prefixedLog.isDebugEnabled()).isTrue();

        when(mockedLog.isDebugEnabled()).thenReturn(false);
        assertThat(prefixedLog.isDebugEnabled()).isFalse();

        verify(mockedLog, times(2)).isDebugEnabled();
        verifyNoMoreInteractions(mockedLog);
    }

    @Test
    void shouldDelegateForJustMessageMethods() {
        prefixedLog.debug(message);
        prefixedLog.info(message);
        prefixedLog.warn(message);
        prefixedLog.error(message);

        verify(mockedLog).debug(format("[%s] %s", prefix, message));
        verify(mockedLog).info(format("[%s] %s", prefix, message));
        verify(mockedLog).warn(format("[%s] %s", prefix, message));
        verify(mockedLog).error(format("[%s] %s", prefix, message));
        verifyNoMoreInteractions(mockedLog);
    }

    @Test
    void shouldDelegateForMessageWithArgumentsMethods() {
        prefixedLog.debug(message, arguments);
        prefixedLog.info(message, arguments);
        prefixedLog.warn(message, arguments);
        prefixedLog.error(message, arguments);

        verify(mockedLog).debug(format("[%s] %s", prefix, message), arguments);
        verify(mockedLog).info(format("[%s] %s", prefix, message), arguments);
        verify(mockedLog).warn(format("[%s] %s", prefix, message), arguments);
        verify(mockedLog).error(format("[%s] %s", prefix, message), arguments);
        verifyNoMoreInteractions(mockedLog);
    }

    @Test
    void shouldDelegateForMessageWithExceptionMethods() {
        prefixedLog.debug(message, exception);
        prefixedLog.info(message, exception);
        prefixedLog.warn(message, exception);
        prefixedLog.error(message, exception);

        verify(mockedLog).debug(format("[%s] %s", prefix, message), exception);
        verify(mockedLog).info(format("[%s] %s", prefix, message), exception);
        verify(mockedLog).warn(format("[%s] %s", prefix, message), exception);
        verify(mockedLog).error(format("[%s] %s", prefix, message), exception);
        verifyNoMoreInteractions(mockedLog);
    }

    @Test
    void shouldDelegateForNeo4jMessageMethods() {
        doAnswer(args -> assertNeo4jLogMessage(args.getArgument(0)))
                .when(mockedLog)
                .debug(any(Neo4jLogMessage.class));
        doAnswer(args -> assertNeo4jLogMessage(args.getArgument(0)))
                .when(mockedLog)
                .info(any(Neo4jLogMessage.class));
        doAnswer(args -> assertNeo4jLogMessage(args.getArgument(0)))
                .when(mockedLog)
                .warn(any(Neo4jLogMessage.class));
        doAnswer(args -> assertNeo4jLogMessage(args.getArgument(0)))
                .when(mockedLog)
                .error(any(Neo4jLogMessage.class));
        doAnswer(args -> assertNeo4jLogMessage(args.getArgument(0)))
                .when(mockedLog)
                .error(any(Neo4jLogMessage.class), any(Throwable.class));

        prefixedLog.debug(neo4jMessage);
        prefixedLog.info(neo4jMessage);
        prefixedLog.warn(neo4jMessage);
        prefixedLog.error(neo4jMessage);
        prefixedLog.error(neo4jMessage, exception);

        verify(mockedLog).debug(any(Neo4jLogMessage.class));
        verify(mockedLog).info(any(Neo4jLogMessage.class));
        verify(mockedLog).warn(any(Neo4jLogMessage.class));
        verify(mockedLog).error(any(Neo4jLogMessage.class));
        verify(mockedLog).error(any(Neo4jLogMessage.class), eq(exception));
        verifyNoMoreInteractions(mockedLog);
    }

    @Test
    void shouldDelegateForNeo4jMessageSupplierMethods() {
        doAnswer(args -> assertNeo4jLogMessageSupplier(args.getArgument(0)))
                .when(mockedLog)
                .debug(any(Neo4jMessageSupplier.class));
        doAnswer(args -> assertNeo4jLogMessageSupplier(args.getArgument(0)))
                .when(mockedLog)
                .info(any(Neo4jMessageSupplier.class));
        doAnswer(args -> assertNeo4jLogMessageSupplier(args.getArgument(0)))
                .when(mockedLog)
                .warn(any(Neo4jMessageSupplier.class));
        doAnswer(args -> assertNeo4jLogMessageSupplier(args.getArgument(0)))
                .when(mockedLog)
                .error(any(Neo4jMessageSupplier.class));

        prefixedLog.debug(() -> neo4jMessage);
        prefixedLog.info(() -> neo4jMessage);
        prefixedLog.warn(() -> neo4jMessage);
        prefixedLog.error(() -> neo4jMessage);

        verify(mockedLog).debug(any(Neo4jMessageSupplier.class));
        verify(mockedLog).info(any(Neo4jMessageSupplier.class));
        verify(mockedLog).warn(any(Neo4jMessageSupplier.class));
        verify(mockedLog).error(any(Neo4jMessageSupplier.class));
        verifyNoMoreInteractions(mockedLog);
    }

    private Object assertNeo4jLogMessageSupplier(Neo4jMessageSupplier message) {
        return assertNeo4jLogMessage(message.get());
    }

    private Object assertNeo4jLogMessage(Neo4jLogMessage message) {
        assertThat(message.getFormat()).isEqualTo(format("[%s] %s", prefix, neo4jMessage.getFormat()));
        assertThat(message.getFormattedMessage())
                .isEqualTo(format("[%s] %s", prefix, neo4jMessage.getFormattedMessage()));
        assertThat(message.getThrowable()).isEqualTo(neo4jMessage.getThrowable());
        assertThat(message.getParameters()).isEqualTo(neo4jMessage.getParameters());
        return null;
    }

    private class TestNeo4jLogMessage implements Neo4jLogMessage {
        @Override
        public String getFormattedMessage() {
            return format(format, arguments);
        }

        @Override
        public String getFormat() {
            return format;
        }

        @Override
        public Object[] getParameters() {
            return arguments;
        }

        @Override
        public Throwable getThrowable() {
            return exception;
        }
    }
}
