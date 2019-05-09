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
package org.neo4j.kernel.impl.api.index.stats;

// this is a necessary evil for GBP tree
@SuppressWarnings( {"NonFinalFieldReferenceInEquals", "NonFinalFieldReferencedInHashCode"} )
class IndexStatisticsKey implements Comparable<IndexStatisticsKey>
{
    static final int SIZE = Long.SIZE;

    private long indexId;

    IndexStatisticsKey()
    {
    }

    IndexStatisticsKey( long indexId )
    {
        this.indexId = indexId;
    }

    long getIndexId()
    {
        return indexId;
    }

    void setIndexId( long indexId )
    {
        this.indexId = indexId;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode( indexId );
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

        final IndexStatisticsKey that = (IndexStatisticsKey) o;
        return indexId == that.indexId;
    }

    @Override
    public String toString()
    {
        return "[indexId:" + indexId + "]";
    }

    @Override
    public int compareTo( IndexStatisticsKey other )
    {
        return Long.compare( indexId, other.indexId );
    }
}
