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
package org.neo4j.graphdb.schema;

import java.util.Locale;

import org.neo4j.annotations.api.PublicApi;

/**
 * This class is simply a progress counter of indexing population progress. It has the constraint that
 * {@code 0 <= completed <= total}
 * <p>
 * Use IndexPopulationProgress.NONE if you need an object without any particular progress.
 */
@PublicApi
public class IndexPopulationProgress
{
    public static final IndexPopulationProgress NONE = new IndexPopulationProgress( 0, 0 );
    public static final IndexPopulationProgress DONE = new IndexPopulationProgress( 1, 1 );

    private final long completedCount;
    private final long totalCount;

    public IndexPopulationProgress( long completed, long total )
    {
        if ( completed < 0 || completed > total )
        {
            throw new IllegalArgumentException( "Invalid progress specified: " + completed + '/' + total );
        }
        this.completedCount = completed;
        this.totalCount = total;
    }

    /**
     * @return percentage (from 0 to 100) of totalCount items which have been indexed. If totalCount is 0, returns 0.
     */
    public float getCompletedPercentage()
    {
        return totalCount > 0 ? ((float) (completedCount * 100) / totalCount) : 0.0f;
    }

    @Override
    public String toString()
    {
        return String.format( Locale.ROOT, "%1.1f%%", getCompletedPercentage() );
    }
}
