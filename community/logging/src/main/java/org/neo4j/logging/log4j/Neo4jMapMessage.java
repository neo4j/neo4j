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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.message.MapMessage;
import org.neo4j.logging.Neo4jLogMessage;

/**
 * Base class for structured log messages.
 * <p>
 * Example of a log message with two fields:
 * <pre>
 *     class MyStructure extends Neo4jMapMessage {
 *         MyStructure() {
 *             super(2);
 *             with("myLong", 7L);
 *             with("myString", "my string");
 *         }
 *
 *         void formatAsString(StringBuilder stringBuilder) {
 *             stringBuilder.append("Text version: ").append(get("myLong")).append("myString");
 *         }
 *     }
 * </pre>
 */
public abstract class Neo4jMapMessage extends MapMessage<Neo4jMapMessage, Object> implements Neo4jLogMessage {
    public Neo4jMapMessage(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    protected void appendMap(StringBuilder sb) {
        formatAsString(sb);
    }

    /**
     * Called when the {@link MapMessage} is serialized to plain text.
     *
     * @param sb StringBuilder accepting output.
     */
    protected abstract void formatAsString(StringBuilder sb);
}
