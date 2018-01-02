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
package org.neo4j.kernel.impl.api.index.sampling;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

public class IndexSamplingConfig
{
    private final int bufferSize;
    private final double updateRatio;
    private final boolean backgroundSampling;

    public IndexSamplingConfig( Config config )
    {
        this.bufferSize = config.get( GraphDatabaseSettings.index_sampling_buffer_size ).intValue();
        this.updateRatio = ((double) config.get( GraphDatabaseSettings.index_sampling_update_percentage )) / 100.0d;
        this.backgroundSampling = config.get( GraphDatabaseSettings.index_background_sampling_enabled );
    }

    public int bufferSize()
    {
        return bufferSize;
    }

    public double updateRatio()
    {
        return updateRatio;
    }

    public int jobLimit()
    {
        return 1;
    }

    public boolean backgroundSampling()
    {
        return backgroundSampling;
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

        IndexSamplingConfig that = (IndexSamplingConfig) o;

        return backgroundSampling == that.backgroundSampling &&
               bufferSize == that.bufferSize &&
               Double.compare( that.updateRatio, updateRatio ) == 0;
    }

    @Override
    public int hashCode()
    {
        int result = bufferSize;
        long temp = Double.doubleToLongBits( updateRatio );
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (backgroundSampling ? 1 : 0);
        return result;
    }
}
