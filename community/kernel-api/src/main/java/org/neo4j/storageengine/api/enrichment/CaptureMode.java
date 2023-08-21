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
package org.neo4j.storageengine.api.enrichment;

import java.util.List;
import org.eclipse.collections.api.map.primitive.ImmutableByteObjectMap;
import org.eclipse.collections.impl.factory.primitive.ByteObjectMaps;

public enum CaptureMode {
    /**
     * Capture only the <strong>changed</strong> state of a node/relationship during a transaction
     */
    DIFF((byte) 1),
    /**
     * Capture all the state of a node/relationship <strong>before</strong> the entity was changed in the transaction
     */
    FULL((byte) 2);

    public static final ImmutableByteObjectMap<CaptureMode> BY_ID =
            ByteObjectMaps.immutable.from(List.of(CaptureMode.values()), CaptureMode::id, v -> v);

    private final byte id;

    CaptureMode(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }
}
