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
package org.neo4j.kernel.impl.store.format;

import static org.neo4j.internal.helpers.ArrayUtil.contains;

import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

/**
 * A collection of high level capabilities a store can have, should not be more granular than necessary
 * for differentiating different version from one another.
 */
public enum RecordStorageCapability implements Capability {
    /**
     * 3 bytes relationship type support
     */
    RELATIONSHIP_TYPE_3BYTES(CapabilityType.FORMAT, CapabilityType.STORE),

    /**
     * Records can spill over into secondary units (another record with a header saying it's a secondary unit to another record).
     */
    SECONDARY_RECORD_UNITS(CapabilityType.FORMAT),

    /**
     * Store files are in little-endian format
     */
    LITTLE_ENDIAN(CapabilityType.FORMAT, CapabilityType.STORE),

    /**
     * Store supports mvcc
     */
    MULTI_VERSIONED(CapabilityType.FORMAT, CapabilityType.STORE);

    private final CapabilityType[] types;
    private final boolean additive;

    RecordStorageCapability(CapabilityType... types) {
        this(false, types);
    }

    RecordStorageCapability(boolean additive, CapabilityType... types) {
        this.additive = additive;
        this.types = types;
    }

    @Override
    public boolean isType(CapabilityType type) {
        return contains(types, type);
    }

    /**
     * Whether or not this capability is additive. A capability is additive if data regarding this capability will not change
     * any existing store and therefore not require migration of existing data.
     *
     * @return whether or not this capability is additive.
     */
    @Override
    public boolean isAdditive() {
        return additive;
    }
}
