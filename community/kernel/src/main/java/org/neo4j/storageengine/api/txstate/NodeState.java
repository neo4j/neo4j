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
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.StorageProperty;

import static java.util.Collections.emptyIterator;

/**
 * Represents the transactional changes to a node:
 * <ul>
 * <li>{@linkplain #labelDiffSets() Labels} that have been {@linkplain ReadableDiffSets#getAdded() added}
 * or {@linkplain ReadableDiffSets#getRemoved() removed}.</li>
 * <li>Added and removed relationships.</li>
 * <li>{@linkplain PropertyContainerState Changes to properties}.</li>
 * </ul>
 */
public interface NodeState extends PropertyContainerState
{
    interface Visitor extends PropertyContainerState.Visitor
    {
        void visitLabelChanges( long nodeId, Set<Integer> added, Set<Integer> removed )
                throws ConstraintValidationException;
    }

    ReadableDiffSets<Integer> labelDiffSets();

    PrimitiveIntSet augmentLabels( PrimitiveIntSet labels );

    int augmentDegree( Direction direction, int degree );

    int augmentDegree( Direction direction, int degree, int typeId );

    void accept( NodeState.Visitor visitor ) throws ConstraintValidationException;

    PrimitiveIntSet relationshipTypes();

    long getId();

    PrimitiveLongIterator getAddedRelationships( Direction direction );

    PrimitiveLongIterator getAddedRelationships( Direction direction, int[] relTypes );

    NodeState EMPTY = new NodeState()
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
        public void accept( PropertyContainerState.Visitor visitor ) throws ConstraintValidationException
        {
        }

        @Override
        public ReadableDiffSets<Integer> labelDiffSets()
        {
            return ReadableDiffSets.Empty.instance();
        }

        @Override
        public PrimitiveIntSet augmentLabels( PrimitiveIntSet labels )
        {
            return labels;
        }

        @Override
        public int augmentDegree( Direction direction, int degree )
        {
            return degree;
        }

        @Override
        public int augmentDegree( Direction direction, int degree, int typeId )
        {
            return degree;
        }

        @Override
        public void accept( NodeState.Visitor visitor )
        {
        }

        @Override
        public PrimitiveIntSet relationshipTypes()
        {
            return Primitive.intSet();
        }

        @Override
        public long getId()
        {
            throw new UnsupportedOperationException( "id not defined" );
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

        @Override
        public PrimitiveLongIterator getAddedRelationships( Direction direction )
        {
            return null;
        }

        @Override
        public PrimitiveLongIterator getAddedRelationships( Direction direction, int[] relTypes )
        {
            return null;
        }
    };

}
