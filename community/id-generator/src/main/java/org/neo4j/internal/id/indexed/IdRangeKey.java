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
package org.neo4j.internal.id.indexed;

/**
 * ID range for an {@link IdRange}. This instance will hold an ID range index which specific which range of IDs can be found in
 * its associated {@link IdRange}. Number of ids per entry also comes into play here. E.g. given idsPerEntry=128 then an iD range index=5
 * specific IDs between 128*5 up to an excluding 128*(5+1).
 */
class IdRangeKey
{
    private long idRangeIdx;

    IdRangeKey( long idRangeIdx )
    {
        this.idRangeIdx = idRangeIdx;
    }

    long getIdRangeIdx()
    {
        return idRangeIdx;
    }

    void setIdRangeIdx( long idRangeIdx )
    {
        this.idRangeIdx = idRangeIdx;
    }
}
