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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.Method;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DuplicatingLogTest {
    private final InternalLog log1 = mock(InternalLog.class);
    private final InternalLog log2 = mock(InternalLog.class);

    @ParameterizedTest
    @ValueSource(strings = {"debug", "info", "warn", "error"})
    void shouldOutputToMultipleLogs(String type) throws Exception {
        // Given
        DuplicatingLog log = new DuplicatingLog(log1, log2);
        Method m = InternalLog.class.getMethod(type, String.class);

        // When
        m.invoke(log, "When the going gets weird");

        // Then
        m.invoke(verify(log1), "When the going gets weird");
        m.invoke(verify(log2), "When the going gets weird");
        verifyNoMoreInteractions(log1);
        verifyNoMoreInteractions(log2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"debug", "info", "warn", "error"})
    void shouldOutputToMultipleLogsWithStructureAwareMessage(String type) throws Exception {
        // Given
        DuplicatingLog log = new DuplicatingLog(log1, log2);
        Method messageLog = InternalLog.class.getMethod(type, Neo4jLogMessage.class);

        // When
        MyMessage message = new MyMessage();
        messageLog.invoke(log, message);

        // Then
        messageLog.invoke(verify(log1), message);
        messageLog.invoke(verify(log2), message);
        verifyNoMoreInteractions(log1);
        verifyNoMoreInteractions(log2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"debug", "info", "warn", "error"})
    void shouldOutputToMultipleLogsWithStructureAwareMessageSupplier(String type) throws Exception {
        // Given
        DuplicatingLog log = new DuplicatingLog(log1, log2);
        Method messageLog = InternalLog.class.getMethod(type, Neo4jMessageSupplier.class);

        // When
        Neo4jMessageSupplier supplier = MyMessage::new;
        messageLog.invoke(log, supplier);

        // Then
        messageLog.invoke(verify(log1), supplier);
        messageLog.invoke(verify(log2), supplier);
        verifyNoMoreInteractions(log1);
        verifyNoMoreInteractions(log2);
    }

    private static class MyMessage implements Neo4jLogMessage {
        @Override
        public String getFormattedMessage() {
            return "When the going gets weird";
        }

        @Override
        public String getFormat() {
            return "";
        }

        @Override
        public Object[] getParameters() {
            return new Object[0];
        }

        @Override
        public Throwable getThrowable() {
            return null;
        }
    }
}
