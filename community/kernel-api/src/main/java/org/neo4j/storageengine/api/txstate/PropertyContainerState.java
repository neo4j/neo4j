/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api.txstate;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.Iterator;

import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;

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

    IntIterable removedProperties();

    Iterator<StorageProperty> addedAndChangedProperties();

    boolean hasPropertyChanges();

    boolean isPropertyChangedOrRemoved( int propertyKey );

    Value propertyValue( int propertyKey );

    PropertyContainerState EMPTY = new EmptyPropertyContainerState();

    class EmptyPropertyContainerState implements PropertyContainerState
    {
        @Override
        public Iterator<StorageProperty> addedProperties()
        {
            return emptyIterator();
        }

        @Override
        public Iterator<StorageProperty> changedProperties()
        {
            return emptyIterator();
        }

        @Override
        public IntIterable removedProperties()
        {
            return IntSets.immutable.empty();
        }

        @Override
        public Iterator<StorageProperty> addedAndChangedProperties()
        {
            return emptyIterator();
        }

        @Override
        public boolean hasPropertyChanges()
        {
            return false;
        }

        @Override
        public boolean isPropertyChangedOrRemoved( int propertyKey )
        {
            return false;
        }

        @Override
        public Value propertyValue( int propertyKey )
        {
            return null;
        }
    }
}
