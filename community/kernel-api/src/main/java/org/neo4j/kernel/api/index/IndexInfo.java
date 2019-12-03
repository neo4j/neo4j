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
package org.neo4j.kernel.api.index;

import java.util.Objects;

public class IndexInfo
{
    private final long updates;
    private final long size;

    public IndexInfo( long updates, long size )
    {
        this.updates = updates;
        this.size = size;
    }

    public long getUpdates()
    {
        return updates;
    }

    public long getSize()
    {
        return size;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        IndexInfo that = (IndexInfo) o;
        return updates == that.updates && size == that.size;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( updates, size );
    }

    @Override
    public String toString()
    {
        return "IndexUpdate{" + "updates=" + updates + ", size=" + size + '}';
    }
}
