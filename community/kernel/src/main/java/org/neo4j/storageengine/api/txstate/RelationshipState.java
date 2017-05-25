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
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageProperty;

import static java.util.Collections.emptyIterator;

/**
 * Represents the transactional changes to a relationship.
 *
 * @see PropertyContainerState
 */
public interface RelationshipState extends PropertyContainerState
{
    long getId();

    <EX extends Exception> boolean accept( RelationshipVisitor<EX> visitor ) throws EX;

    RelationshipState EMPTY = new RelationshipState()
    {
        @Override
        public long getId()
        {
            throw new UnsupportedOperationException( "id" + " not defined" );
        }

        @Override
        public <EX extends Exception> boolean accept( RelationshipVisitor<EX> visitor ) throws EX
        {
            return false;
        }

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
        public void accept( Visitor visitor ) throws ConstraintValidationException
        {
        }

        @Override
        public boolean hasChanges()
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
        public boolean isPropertyRemoved( int propertyKeyId )
        {
            return false;
        }
    };
}
