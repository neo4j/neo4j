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
package org.neo4j.kernel.impl.index.schema;

/**
 * Keys in {@link TokenScanLayout}, each key consists of {@code tokenId} and {@code entityIdRange}, i.e.
 * {@code entityId/rangeSize}, where each range is a small bit set of size {@link TokenScanValue#RANGE_SIZE}.
 */
class TokenScanKey
{
    int tokenId;
    long idRange;

    TokenScanKey()
    {
        clear();
    }

    TokenScanKey( int tokenId, long idRange )
    {
        set( tokenId, idRange );
    }

    /**
     * Sets this key.
     *
     * @param tokenId tokenId for this key.
     * @param idRange entity idRange for this key.
     * @return this key instance, for convenience.
     */
    final TokenScanKey set( int tokenId, long idRange )
    {
        this.tokenId = tokenId;
        this.idRange = idRange;
        return this;
    }

    final void clear()
    {
        set( -1, -1 );
    }

    @Override
    public String toString()
    {
        return "[token:" + tokenId + ",range:" + idRange + "]";
    }
}
