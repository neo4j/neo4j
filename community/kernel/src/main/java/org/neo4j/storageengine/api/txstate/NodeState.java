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

import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.impl.newapi.RelationshipDirection;
import org.neo4j.storageengine.api.Direction;

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

    int augmentDegree( Direction direction, int degree );

    int augmentDegree( Direction direction, int degree, int typeId );

    /**
     * This method counts all directions separately, i.e.
     * total count = count(INCOMING) + count(OUTGOING) + count(LOOPS)
     */
    int augmentDegree( RelationshipDirection direction, int degree, int typeId );

    void accept( NodeState.Visitor visitor ) throws ConstraintValidationException;

    PrimitiveIntSet relationshipTypes();

    long getId();

    @Deprecated
    PrimitiveLongIterator getAddedRelationships( Direction direction );

    @Deprecated
    PrimitiveLongIterator getAddedRelationships( Direction direction, int[] relTypes );

    PrimitiveLongIterator getAddedRelationships();

    PrimitiveLongIterator getAddedRelationships( RelationshipDirection direction, int relType );

    boolean hasRelationshipChanges();
}
