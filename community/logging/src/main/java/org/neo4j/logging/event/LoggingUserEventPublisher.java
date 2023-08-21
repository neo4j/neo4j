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

import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.PrefixedLogProvider;

public class LoggingUserEventPublisher implements UserEventPublisher {
    private final InternalLogProvider logProvider;

    LoggingUserEventPublisher(InternalLogProvider logProvider) {
        this.logProvider = logProvider;
    }

    @Override
    public void publish(EventType eventType) {
        publish(eventType, Parameters.EMPTY);
    }

    @Override
    public void publish(EventType eventType, Parameters parameters) {
        var prefixLogProvider = new PrefixedLogProvider(logProvider, "Event");
        var userLog = prefixLogProvider.getLog(eventType.getComponentNamespace().getName());
        // no cache needed here as AbstractLogProvider already has one
        var message = eventType.getMessage();
        var separator = parameters.isEmpty() ? "" : " - ";
        switch (eventType.getLoggingLevel()) {
            case Begin, Finish -> userLog.info(
                    "%s - %s%s%s", eventType.getLoggingLevel(), message, separator, parameters);
            case Info -> userLog.info("%s%s%s", message, separator, parameters);
            case Warn -> userLog.warn("%s%s%s", message, separator, parameters);
            case Error -> userLog.error("%s%s%s", message, separator, parameters);
        }
    }
}
