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
package org.neo4j.storageengine.api.txstate;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.storageengine.api.StorageProperty;

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

    boolean hasPropertyChanges();

    StorageProperty getChangedProperty( int propertyKeyId );

    StorageProperty getAddedProperty( int propertyKeyId );

    boolean isPropertyChangedOrRemoved( int propertyKey );

    boolean isPropertyRemoved( int propertyKeyId );

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
        public Iterator<Integer> removedProperties()
        {
            return emptyIterator();
        }

        @Override
        public Iterator<StorageProperty> addedAndChangedProperties()
        {
            return emptyIterator();
        }

        @Override
        public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
        {
            return iterator;
        }

        @Override
        public void accept( Visitor visitor )
        {
        }

        @Override
        public boolean hasPropertyChanges()
        {
            return false;
        }

        @Override
        public StorageProperty getChangedProperty( int propertyKeyId )
        {
            return null;
        }

        @Override
        public StorageProperty getAddedProperty( int propertyKeyId )
        {
            return null;
        }

        @Override
        public boolean isPropertyChangedOrRemoved( int propertyKey )
        {
            return false;
        }

        @Override
        public boolean isPropertyRemoved( int propertyKeyId )
        {
            return false;
        }
    }
}
