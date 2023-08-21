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

/** Interface for publishing lifecycle event to user log. It cannot publish arbitrary messages to log
 * only events declared as EventTypes. The motivation is to make it easy to enumerate and share the
 * possible messages logged to support and users.
 */
public interface UserEventPublisher {
    UserEventPublisher NO_OP = new UserEventPublisher() {

        @Override
        public void publish(EventType eventType) {}

        @Override
        public void publish(EventType eventType, Parameters parameters) {}
    };

    void publish(EventType eventType);

    void publish(EventType eventType, Parameters parameters);
}
