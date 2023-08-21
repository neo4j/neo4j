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

/**
 * Provides mapping between entity id and entry in the token index.
 */
public interface TokenIndexIdLayout {
    /**
     * @return id of the entity within its range
     */
    int idWithinRange(long entityId);

    /**
     * @return range which holds
     */
    long rangeOf(long entityId);

    /**
     * @return first entity id of the range
     */
    long firstIdOfRange(long idRange);

    long roundUp(long sizeHint);
}
