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
package org.neo4j.kernel.impl.index.labelscan;

/**
 * Keys in {@link LabelScanLayout}, each key consists of {@code labelId} and {@code nodeIdRange}, i.e.
 * {@code nodeId/rangeSize}, where each range is a small bit set of size {@link LabelScanValue#RANGE_SIZE}.
 */
class LabelScanKey
{
    int labelId;
    long idRange;

    LabelScanKey()
    {
        clear();
    }

    LabelScanKey( int labelId, long idRange )
    {
        set( labelId, idRange );
    }

    /**
     * Sets this key.
     *
     * @param labelId labelId for this key.
     * @param idRange node idRange for this key.
     * @return this key instance, for convenience.
     */
    final LabelScanKey set( int labelId, long idRange )
    {
        this.labelId = labelId;
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
        return "[label:" + labelId + ",range:" + idRange + "]";
    }
}
