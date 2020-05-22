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
package org.neo4j.kernel.impl.api.index.stats;

class IndexStatisticsValue
{
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
}
