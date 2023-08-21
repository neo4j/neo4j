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
package org.neo4j.bolt.protocol.common.fsm.response;

import org.neo4j.values.AnyValue;

public interface MetadataConsumer {

    /**
     * Handles the change of a metadata value identified by a given key.
     * <p />
     * Metadata may be produced at arbitrary points within a given operation but should not be
     * considered authoritative until the operation has completed successfully. As such,
     * implementors will likely want to cache the metadata map until that moment.
     * <p />
     * When a given metadata key is encountered multiple times, only the latest instance should be
     * considered when building the final result. Prior instances may be discarded.
     *
     * @param key a unique metadata key.
     * @param value a metadata value.
     */
    void onMetadata(String key, AnyValue value);
}
