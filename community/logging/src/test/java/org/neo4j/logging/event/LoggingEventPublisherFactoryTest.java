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
package org.neo4j.logging.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

class LoggingEventPublisherFactoryTest {

    private static final ComponentNamespace TEST_NAMESPACE = new ComponentNamespace("test");
    private final InternalLogProvider logProvider = mock(InternalLogProvider.class);
    private final InternalLog log = mock(InternalLog.class);

    @BeforeEach
    void setUp() {
        when(logProvider.getLog(anyString())).thenReturn(log);
    }

    @Test
    void shouldCreateLogForNamespece() {
        LoggingEventPublisherFactory.debugLogEventPublisher(logProvider, TEST_NAMESPACE);
        verify(logProvider).getLog(TEST_NAMESPACE.getName());
    }

    @Test
    void logLinesShouldBeIdenticalIfSameNamespace() {

        var eventPublisher1 = LoggingEventPublisherFactory.debugLogEventPublisher(logProvider, TEST_NAMESPACE);
        var eventPublisher2 = LoggingEventPublisherFactory.debugLogEventPublisher(logProvider, TEST_NAMESPACE);

        eventPublisher1.publish(Type.Info, "hello", Parameters.of("param", TEST_NAMESPACE));
        eventPublisher2.publish(Type.Info, "hello", Parameters.of("param", TEST_NAMESPACE));

        verify(logProvider, times(2)).getLog(TEST_NAMESPACE.getName());

        verify(log, times(2)).info("[Event] %s %s", "hello", Parameters.of("param", TEST_NAMESPACE));
    }

    @Test
    void shouldCreateUserLogPublisher() {
        var eventPublisher1 = LoggingEventPublisherFactory.userLogEventPublisher(logProvider);
        eventPublisher1.publish(TestEvents.START);
        verify(log, times(1)).info(anyString(), any(Type.class), anyString(), anyString(), any());
    }
}
