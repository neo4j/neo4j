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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class LoggingUserEventPublisherTest {

    private final InternalLogProvider logProvider = mock(InternalLogProvider.class);
    private final InternalLog log = mock(InternalLog.class);

    @BeforeEach
    void setUp() {
        when(logProvider.getLog(anyString())).thenReturn(log);
    }

    @Test
    void shouldLogBegin() {
        var publisher = new LoggingUserEventPublisher(logProvider);

        publisher.publish(TestEvents.START);
        verify(log).info(anyString(), eq(Type.Begin), eq(TestEvents.START.getMessage()), anyString(), any());
    }

    @Test
    void shouldLogWarning() {
        var publisher = new LoggingUserEventPublisher(logProvider);

        publisher.publish(TestEvents.END);
        verify(log).warn(anyString(), eq(TestEvents.END.getMessage()), anyString(), any());
    }

    @Test
    void shouldLogWithParams() {
        var publisher = new LoggingUserEventPublisher(logProvider);

        var params = Parameters.of("key", "value");
        publisher.publish(TestEvents.WITH_PARAMS, params);
        verify(log).info(anyString(), eq(TestEvents.WITH_PARAMS.getMessage()), anyString(), eq(params));
    }
}
