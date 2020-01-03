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

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.storageengine.api.RelationshipDirection;

/**
 * Represents the transactional changes to a node:
 * <ul>
 * <li>{@linkplain #labelDiffSets() Labels} that have been {@linkplain DiffSets#getAdded() added}
 * or {@linkplain DiffSets#getRemoved() removed}.</li>
 * <li>Added and removed relationships.</li>
 * <li>{@linkplain PropertyContainerState Changes to properties}.</li>
 * </ul>
 */
public interface NodeState extends PropertyContainerState
{
    LongDiffSets labelDiffSets();

    /**
     * This method counts all directions separately, i.e.
     * total count = count(INCOMING) + count(OUTGOING) + count(LOOPS)
     */
    int augmentDegree( RelationshipDirection direction, int degree, int typeId );

    long getId();

    LongIterator getAddedRelationships();

    LongIterator getAddedRelationships( RelationshipDirection direction, int relType );
}
