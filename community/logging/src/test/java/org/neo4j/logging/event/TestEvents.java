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

public enum TestEvents implements EventType {
    START("Test started", Type.Begin),
    END("Test ended", Type.Warn),
    WITH_PARAMS("With Params", Type.Info);

    public static ComponentNamespace TEST_COMPONENT_NAMESPACE = new ComponentNamespace("TestComponent");

    private final String message;
    private final Type level;

    private TestEvents(String message, Type level) {
        this.message = message;
        this.level = level;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Type getLoggingLevel() {
        return level;
    }

    @Override
    public ComponentNamespace getComponentNamespace() {
        return TEST_COMPONENT_NAMESPACE;
    }
}
