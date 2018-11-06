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
package org.neo4j.kernel.impl.api.index;

import org.eclipse.collections.api.set.primitive.MutableIntSet;

import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;

/**
 * The PropertyLoader provides a stream-lined interface for getting multiple property values from a node in a single
 * call. This can be used over the index provider API {@link NodePropertyAccessor} for better performance in these cases.
 */
public interface PropertyLoader
{
    /**
     * Loads set of properties for entity. For every target propertyId P, the value for this node is loaded. If the
     * entity has P, the onProperty methods will be called on the sink, with the correct value, and P will be removed
     * from propertyIds. If the entity lacks P no action will be taken.
     * @param entityId id of the entity to be loaded
     * @param type EntityType of the entity to load properties from
     * @param propertyIds set of target property ids to load. Loaded properties are remove from this set.
     * @param sink sink that will receive successfully loaded values for the target properties
     */
    void loadProperties( long entityId, EntityType type, MutableIntSet propertyIds, PropertyLoadSink sink );

    interface PropertyLoadSink
    {
        void onProperty( int entityId, Value value );
    }
}
