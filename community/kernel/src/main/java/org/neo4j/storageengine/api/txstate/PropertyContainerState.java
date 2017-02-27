/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.storageengine.api.txstate;

import java.util.Iterator;

import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.storageengine.api.StorageProperty;

/**
 * Represents the property changes to a {@link NodeState node} or {@link RelationshipState relationship}:
 * <ul>
 * <li>{@linkplain #addedProperties() Added properties},</li>
 * <li>{@linkplain #removedProperties() removed properties}, and </li>
 * <li>{@linkplain #changedProperties() changed property values}.</li>
 * </ul>
 */
public interface PropertyContainerState
{
    Iterator<StorageProperty> addedProperties();

    Iterator<StorageProperty> changedProperties();

    Iterator<Integer> removedProperties();

    Iterator<StorageProperty> addedAndChangedProperties();

    Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator );

    void accept( Visitor visitor ) throws ConstraintValidationException;

    interface Visitor
    {
        void visitPropertyChanges( long entityId, Iterator<StorageProperty> added,
                Iterator<StorageProperty> changed,
                Iterator<Integer> removed ) throws ConstraintValidationException;
    }

    boolean hasChanges();

    StorageProperty getChangedProperty( int propertyKeyId );

    StorageProperty getAddedProperty( int propertyKeyId );

    boolean isPropertyRemoved( int propertyKeyId );
}
