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
package org.neo4j.bolt.testing.messages;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import org.neo4j.bolt.negotiation.ProtocolVersion;

public interface WireMessageBuilder<T> {
    Map<String, Object> getMeta();

    ProtocolVersion getProtocolVersion();

    T getThis();

    ByteBuf build();

    /**
     * Use this to inject bad key-value pairs into metadata.
     * Do not use it to add metadata with known keys.
     * @param key an unknown metadata key.
     * @param value a value
     * @return this;
     */
    default T withBadKeyPair(String key, Object value) {
        getMeta().put(key, value);
        return getThis();
    }
}
