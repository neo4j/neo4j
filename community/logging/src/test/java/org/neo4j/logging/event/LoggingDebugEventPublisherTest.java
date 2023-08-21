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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.logging.Log;

class LoggingDebugEventPublisherTest {

    private final Log log = mock(Log.class);

    @ParameterizedTest
    @EnumSource(Type.class)
    void shouldLogEventAndMessage(Type type) {
        var publisher = new LoggingDebugEventPublisher(log);
        publisher.publish(type, "hello");
        switch (type) {
            case Begin, Finish -> verify(log).info("%s - %s %s", type, "hello", Parameters.EMPTY);
            case Info -> verify(log).info("%s %s", "hello", Parameters.EMPTY);
            case Warn -> verify(log).warn("%s %s", "hello", Parameters.EMPTY);
            case Error -> verify(log).error("%s %s", "hello", Parameters.EMPTY);
        }
    }

    @ParameterizedTest
    @EnumSource(Type.class)
    void shouldLogEventAndMessageAndParams(Type type) {
        var publisher = new LoggingDebugEventPublisher(log);
        var parameters = Parameters.of("param", 1);
        publisher.publish(type, "hello", parameters);
        switch (type) {
            case Begin, Finish -> verify(log).info("%s - %s %s", type, "hello", parameters);
            case Info -> verify(log).info("%s %s", "hello", parameters);
            case Warn -> verify(log).warn("%s %s", "hello", parameters);
            case Error -> verify(log).error("%s %s", "hello", parameters);
        }
    }
}
