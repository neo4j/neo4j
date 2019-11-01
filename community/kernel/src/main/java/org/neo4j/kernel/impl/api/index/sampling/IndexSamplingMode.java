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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.Objects;

public final class IndexSamplingMode
{
    public static final long NO_WAIT = 0;

    private final boolean sampleOnlyIfUpdated;
    private final long millisToWaitForCompletion;
    private final String description;

    private IndexSamplingMode( boolean sampleOnlyIfUpdated, long millisToWaitForCompletion, String description )
    {
        this.sampleOnlyIfUpdated = sampleOnlyIfUpdated;
        this.millisToWaitForCompletion = millisToWaitForCompletion;
        this.description = description;
    }

    boolean sampleOnlyIfUpdated()
    {
        return sampleOnlyIfUpdated;
    }

    public long millisToWaitForCompletion()
    {
        return millisToWaitForCompletion;
    }

    @Override
    public String toString()
    {
        return description;
    }

    public static IndexSamplingMode foregroundRebuildUpdated( long millisToWaitForCompletion )
    {
        return new IndexSamplingMode( true, millisToWaitForCompletion, "FOREGROUND-REBUILD UPDATED" );
    }

    public static IndexSamplingMode backgroundRebuildAll()
    {
        return new IndexSamplingMode( false, NO_WAIT, "BACKGROUND-REBUILD ALL" );
    }

    public static IndexSamplingMode backgroundRebuildUpdated()
    {
        return new IndexSamplingMode( true, NO_WAIT, "BACKGROUND-REBUILD UPDATED" );
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
        IndexSamplingMode that = (IndexSamplingMode) o;
        return sampleOnlyIfUpdated == that.sampleOnlyIfUpdated &&
                millisToWaitForCompletion == that.millisToWaitForCompletion &&
                description.equals( that.description );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( sampleOnlyIfUpdated, millisToWaitForCompletion, description );
    }
}
