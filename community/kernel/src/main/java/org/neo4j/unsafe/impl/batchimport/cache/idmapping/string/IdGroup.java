/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.unsafe.impl.batchimport.input.Group;

/**
 * A {@link Group} with additional metadata managed by {@link EncodingIdMapper}.
 */
class IdGroup
{
    private final Group group;
    private final int groupId;
    private final long lowDataIndex; // inclusive
    private long highDataIndex = -1; // inclusive

    IdGroup( Group group, long lowDataIndex )
    {
        this.group = group;
        this.lowDataIndex = lowDataIndex;
        this.groupId = group.id();
    }

    void setHighDataIndex( long index )
    {
        this.highDataIndex = index;
    }

    boolean covers( long index )
    {
        return index >= lowDataIndex && index <= highDataIndex;
    }

    int id()
    {
        return groupId;
    }

    String name()
    {
        return group.name();
    }

    @Override
    public String toString()
    {
        return group.toString();
    }
}
