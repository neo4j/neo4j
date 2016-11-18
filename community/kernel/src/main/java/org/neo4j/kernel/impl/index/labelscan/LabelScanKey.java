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
package org.neo4j.kernel.impl.index.labelscan;

/**
 * Keys in {@link LabelScanLayout}, each key consists of {@code labelId} and {@code nodeIdRange}, i.e.
 * {@code nodeId/rangeSize}, where each range is a small bit set of size {@code rangeSize}.
 */
class LabelScanKey
{
    int labelId = -1;
    long idRange = -1;

    /**
     * Sets this key.
     *
     * @param labelId labelId for this key.
     * @param idRange node idRange for this key.
     * @return this key instance, for convenience.
     */
    LabelScanKey set( int labelId, long idRange )
    {
        this.labelId = labelId;
        this.idRange = idRange;
        return this;
    }

    @Override
    public String toString()
    {
        return "[lbl:" + labelId + ",range:" + idRange + "]";
    }
}
