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
package org.neo4j.kernel.impl.index.schema;

public interface EntityTokenRange {
    /**
     * @return the range id of this range. This is the base entity id divided by range size.
     * Example: A store with entities 1,3,20,22 and a range size of 16 would return ranges:
     * - rangeId=0, entities=1,3
     * - rangeId=1, entities=20,22
     */
    long id();

    boolean covers(long entityId);

    boolean isBelow(long entityId);

    /**
     * @return entity ids in this range, the entities in this array may or may not have {@link #tokens(long) tokens}
     * attached to it.
     */
    long[] entities();

    /**
     * Returns the token ids (as longs) for the given entity id. The {@code entityId} must be one of the ids
     * from {@link #entities()}.
     *
     * @param entityId the entity id to return tokens for.
     * @return token ids for the given {@code entityId}.
     */
    long[] tokens(long entityId);
}
