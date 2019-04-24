/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

import static org.neo4j.internal.helpers.ArrayUtil.contains;

/**
 * A collection of high level capabilities a store can have, should not be more granular than necessary
 * for differentiating different version from one another.
 */
public enum RecordStorageCapability implements Capability
{
    /**
     * Store has schema support
     */
    SCHEMA( CapabilityType.STORE ),

    /**
     * Store has dense node support
     */
    DENSE_NODES( CapabilityType.FORMAT, CapabilityType.STORE ),

    /**
     * 3 bytes relationship type support
     */
    RELATIONSHIP_TYPE_3BYTES( CapabilityType.FORMAT, CapabilityType.STORE ),

    /**
     * Point Geometries are an addition to the format, not a change
     */
    POINT_PROPERTIES( true, CapabilityType.STORE ),

    /**
     * Temporal types are an addition to the format, not a change
     */
    TEMPORAL_PROPERTIES( true, CapabilityType.STORE ),

    /**
     * Records can spill over into secondary units (another record with a header saying it's a secondary unit to another record).
     */
    SECONDARY_RECORD_UNITS( CapabilityType.FORMAT ),

    /**
     * The schema store can store arbitrary properties of schema elements, such as index configurations.
     */
    FLEXIBLE_SCHEMA_STORE( CapabilityType.STORE ),

    /**
     * Tokens can be marked as internal, and internal tokens will not be shown in the product surface, but can still be used, well, internally.
     * <p>
     * This has {@link CapabilityType#STORE} because it is a format addition, and not a format change per se. We just use a record header bit that was free.
     */
    INTERNAL_TOKENS( true, CapabilityType.STORE ),

    /**
     * ID files (neostore.xyz.id) are backed by {@link GBPTree}, transactional and recoverable.
     */
    GBPTREE_ID_FILES( false, CapabilityType.STORE );

    private final CapabilityType[] types;
    private boolean additive;

    RecordStorageCapability( CapabilityType... types )
    {
        this( false, types );
    }

    RecordStorageCapability( boolean additive, CapabilityType... types )
    {
        this.additive = additive;
        this.types = types;
    }

    @Override
    public boolean isType( CapabilityType type )
    {
        return contains( types, type );
    }

    /**
     * Whether or not this capability is additive. A capability is additive if data regarding this capability will not change
     * any existing store and therefore not require migration of existing data.
     *
     * @return whether or not this capability is additive.
     */
    @Override
    public boolean isAdditive()
    {
        return additive;
    }
}
