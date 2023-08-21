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
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.PrefixedLogProvider;
import org.neo4j.util.VisibleForTesting;

class LoggingDebugEventPublisher implements DebugEventPublisher {
    private final Log debugLog;

    LoggingDebugEventPublisher(InternalLogProvider logProvider, ComponentNamespace component) {
        var prefixLogProvider = new PrefixedLogProvider(logProvider, "Event");
        this.debugLog = prefixLogProvider.getLog(component.getName());
    }

    LoggingDebugEventPublisher(LogService logService, ComponentNamespace component) {
        var prefixLogger = new PrefixedLogProvider(logService.getInternalLogProvider(), "Event");
        this.debugLog = prefixLogger.getLog(component.getName());
    }

    @VisibleForTesting
    LoggingDebugEventPublisher(Log log) {
        this.debugLog = log;
    }

    @Override
    public void publish(Type type, String message, Parameters parameters) {
        switch (type) {
            case Begin, Finish -> debugLog.info("%s - %s %s", type, message, parameters);
            case Info -> debugLog.info("%s %s", message, parameters);
            case Warn -> debugLog.warn("%s %s", message, parameters);
            case Error -> debugLog.error("%s %s", message, parameters);
        }
    }

    @Override
    public void publish(Type type, String message) {
        publish(type, message, Parameters.EMPTY);
    }
}
