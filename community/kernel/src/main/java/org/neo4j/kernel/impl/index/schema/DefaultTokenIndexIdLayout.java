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

import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;

public class DefaultTokenIndexIdLayout implements TokenIndexIdLayout {

    private static final int RANGE_MASK = RANGE_SIZE - 1;
    private static final int RANGE_SHIFT = 31 - Integer.numberOfLeadingZeros(RANGE_SIZE);

    @Override
    public int idWithinRange(long entityId) {
        return (int) entityId & RANGE_MASK;
    }

    @Override
    public long rangeOf(long entityId) {
        return entityId >> RANGE_SHIFT;
    }

    @Override
    public long firstIdOfRange(long idRange) {
        return idRange << RANGE_SHIFT;
    }

    @Override
    public long roundUp(long sizeHint) {
        return ((sizeHint + RANGE_SIZE - 1) / RANGE_SIZE) * RANGE_SIZE;
    }
}
