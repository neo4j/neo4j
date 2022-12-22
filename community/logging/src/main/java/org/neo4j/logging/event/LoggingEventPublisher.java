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
package org.neo4j.logging.event;

import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.PrefixedLogProvider;
import org.neo4j.util.VisibleForTesting;

class LoggingEventPublisher implements EventPublisher {
    private final Log log;

    LoggingEventPublisher(InternalLogProvider logProvider, String description) {
        this(new PrefixedLogProvider(logProvider, "Event").getLog(description));
    }

    @VisibleForTesting
    LoggingEventPublisher(Log log) {
        this.log = log;
    }

    @Override
    public void publish(Type type, String message, Parameters parameters) {
        switch (type) {
            case Begin, Finish -> log.info("%s - %s %s", type, message, parameters);
            case Info -> log.info("%s %s", message, parameters);
            case Warn -> log.warn("%s %s", message, parameters);
            case Error -> log.error("%s %s", message, parameters);
        }
    }

    @Override
    public void publish(Type type, String message) {
        publish(type, message, Parameters.EMPTY);
    }
}
