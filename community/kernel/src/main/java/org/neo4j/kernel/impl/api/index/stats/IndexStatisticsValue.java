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
class IndexStatisticsValue
{
    public static final IndexStatisticsValue EMPTY_STATISTICS = new IndexStatisticsValue( 0, 0, 0, 0 );
    static final int SIZE = Long.SIZE * 4;

    private long sampleUniqueValues;
    private long sampleSize;
    private long updatesCount;
    private long indexSize;

    IndexStatisticsValue()
    {
    }

    IndexStatisticsValue( long sampleUniqueValues, long sampleSize, long updatesCount, long indexSize )
    {
        this.sampleUniqueValues = sampleUniqueValues;
        this.sampleSize = sampleSize;
        this.updatesCount = updatesCount;
        this.indexSize = indexSize;
    }

    long getSampleUniqueValues()
    {
        return sampleUniqueValues;
    }

    void setSampleUniqueValues( long sampleUniqueValues )
    {
        this.sampleUniqueValues = sampleUniqueValues;
    }

    public long getSampleSize()
    {
        return sampleSize;
    }

    public void setSampleSize( long sampleSize )
    {
        this.sampleSize = sampleSize;
    }

    long getUpdatesCount()
    {
        return updatesCount;
    }

    void setUpdatesCount( long updatesCount )
    {
        this.updatesCount = updatesCount;
    }

    public long getIndexSize()
    {
        return indexSize;
    }

    public void setIndexSize( long indexSize )
    {
        this.indexSize = indexSize;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (sampleUniqueValues ^ (sampleUniqueValues >>> 32));
        result = 31 * result + (int) (sampleSize ^ (sampleSize >>> 32));
        result = 31 * result + (int) (updatesCount ^ (updatesCount >>> 32));
        result = 31 * result + (int) (indexSize ^ (indexSize >>> 32));
        return result;
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

        final IndexStatisticsValue that = (IndexStatisticsValue) o;

        if ( sampleUniqueValues != that.sampleUniqueValues )
        {
            return false;
        }
        if ( sampleSize != that.sampleSize )
        {
            return false;
        }
        if ( updatesCount != that.updatesCount )
        {
            return false;
        }
        return indexSize == that.indexSize;
    }

    public IndexStatisticsValue copy()
    {
        return new IndexStatisticsValue( sampleUniqueValues, sampleSize, updatesCount, indexSize );
    }
}
