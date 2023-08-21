/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.txstate;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.diffset.DiffSets;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;

/**
 * Represents the transactional changes to a node:
 * <ul>
 * <li>{@linkplain #labelDiffSets() Labels} that have been {@linkplain DiffSets#getAdded() added}
 * or {@linkplain DiffSets#getRemoved() removed}.</li>
 * <li>Added and removed relationships.</li>
 * <li>{@linkplain EntityState Changes to properties}.</li>
 * </ul>
 */
public interface NodeState extends EntityState {
    LongDiffSets labelDiffSets();

    boolean fillDegrees(RelationshipSelection selection, Degrees.Mutator degree);

    long getId();

    IntIterable getAddedRelationshipTypes();

    IntIterable getAddedAndRemovedRelationshipTypes();

    LongIterator getAddedRelationships();

    LongIterator getAddedRelationships(Direction direction);

    LongIterator getAddedRelationships(Direction direction, int relType);

    LongIterator getAddedRelationships(RelationshipDirection direction, int relType);
}
