/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.storageengine.api;

import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.cursor.Cursor;

/**
 * Represents a single node or relationship cursor item.
 */
public interface EntityItem
{
    /**
     * @return id of current entity
     * @throws IllegalStateException if no current entity is selected
     */
    long id();

    /**
     * @return cursor for properties of current entity
     * @throws IllegalStateException if no current entity is selected
     */
    Cursor<PropertyItem> properties();

    /**
     * @param propertyKeyId of property to find
     * @return cursor for specific property of current entity
     * @throws IllegalStateException if no current entity is selected
     */
    Cursor<PropertyItem> property( int propertyKeyId );

    /**
     * @param propertyKeyId property key token id to check property for.
     * @return whether or not this entity has a property of the given property key token id.
     */
    boolean hasProperty( int propertyKeyId );

    /**
     * @param propertyKeyId property key token id to get property value for.
     * @return property value for the given property key token id on this entity.
     */
    Object getProperty( int propertyKeyId );

    /**
     * @return property key token ids of all properties on this entity.
     */
    PrimitiveIntCollection getPropertyKeys();
}
