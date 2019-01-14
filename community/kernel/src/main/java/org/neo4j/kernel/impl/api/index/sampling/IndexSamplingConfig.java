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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

public class IndexSamplingConfig
{
    private final int sampleSizeLimit;
    private final double updateRatio;
    private final boolean backgroundSampling;

    public IndexSamplingConfig( Config config )
    {
        this( config.get( GraphDatabaseSettings.index_sample_size_limit ),
                          config.get( GraphDatabaseSettings.index_sampling_update_percentage ) / 100.0d,
                          config.get( GraphDatabaseSettings.index_background_sampling_enabled ) );
    }

    public IndexSamplingConfig( int sampleSizeLimit, double updateRatio, boolean backgroundSampling )
    {
        this.sampleSizeLimit = sampleSizeLimit;
        this.updateRatio = updateRatio;
        this.backgroundSampling = backgroundSampling;
    }

    public int sampleSizeLimit()
    {
        return sampleSizeLimit;
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
               sampleSizeLimit == that.sampleSizeLimit &&
               Double.compare( that.updateRatio, updateRatio ) == 0;
    }

    @Override
    public int hashCode()
    {
        int result = sampleSizeLimit;
        long temp = Double.doubleToLongBits( updateRatio );
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (backgroundSampling ? 1 : 0);
        return result;
    }
}
