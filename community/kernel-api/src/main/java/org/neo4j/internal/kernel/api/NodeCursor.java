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
package org.neo4j.internal.kernel.api;

import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.RelationshipSelection;

/**
 * Cursor for scanning nodes.
 */
public interface NodeCursor extends EntityCursor {
    @Override
    default long reference() {
        return nodeReference();
    }

    long nodeReference();

    TokenSet labels();

    TokenSet labelsIgnoringTxStateSetRemove();

    boolean hasLabel(int label);

    boolean hasLabel();

    default TokenSet labelsAndProperties(PropertyCursor propertyCursor, PropertySelection selection) {
        properties(propertyCursor, selection);
        return labels();
    }

    void relationships(RelationshipTraversalCursor relationships, RelationshipSelection selection);

    boolean supportsFastRelationshipsTo();

    void relationshipsTo(
            RelationshipTraversalCursor relationships, RelationshipSelection selection, long neighbourNodeReference);

    long relationshipsReference();

    /**
     * @return whether or not this node cursor can decide degree for various relationship selections cheaper than doing a full scan of all relationships.
     */
    boolean supportsFastDegreeLookup();

    int[] relationshipTypes();

    /**
     * Gathers degrees for types and direction provided by the {@link RelationshipSelection}. The returned {@link Degrees} will contain
     * this information and will be able to answer degrees for each individual type/direction and also sums.
     *
     * @param selection which types/directions to get degrees for.
     * @return a {@link Degrees} instance with the selected degree information.
     */
    Degrees degrees(RelationshipSelection selection);

    /**
     * Returns a single total degree for types and directions provided by the {@link RelationshipSelection}.
     *
     * @param selection which types/directions to get degrees for.
     * @return the total degree of all selected relationship types and direction.
     */
    int degree(RelationshipSelection selection);

    /**
     * Returns a min(degree(selection), maxDegree).
     *
     * Allows computation to finish early if what you are interested is only if the
     * degree is above or below a certain limit.
     *
     * @param maxDegree the maximum degree
     * @param selection which types/directions to get degrees for.
     * @return min(degree(selection), maxDegree).
     */
    int degreeWithMax(int maxDegree, RelationshipSelection selection);
}
