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

import org.neo4j.helpers.collection.MultiSet;
import org.neo4j.storageengine.api.schema.IndexSample;

public class DefaultNonUniqueIndexSampler implements NonUniqueIndexSampler
{
    private final int sampleSizeLimit;
    private final MultiSet<String> values;

    private int sampledSteps;

    // kept as longs to side step overflow issues

    private long accumulatedUniqueValues;
    private long accumulatedSampledSize;
    private long sampleSize;

    public DefaultNonUniqueIndexSampler( int sampleSizeLimit )
    {
        this.values = new MultiSet<>( calculateInitialSetSize( sampleSizeLimit ) );
        this.sampleSizeLimit = sampleSizeLimit;
    }

    @Override
    public void include( String value )
    {
        include( value, 1 );
    }

    @Override
    public void include( String value, long increment )
    {
        assert increment > 0;
        if ( sampleSize >= sampleSizeLimit )
        {
            nextStep();
        }

        if ( values.increment( value, increment ) == increment )
        {
            sampleSize += value.length();
        }
    }

    @Override
    public void exclude( String value )
    {
        exclude( value, 1 );
    }

    @Override
    public void exclude( String value, long decrement )
    {
        assert decrement > 0;
        if ( values.increment( value, -decrement ) == 0 )
        {
            sampleSize -= value.length();
        }
    }

    @Override
    public IndexSample result()
    {
        return result( -1 );
    }

    @Override
    public IndexSample result( int numDocs )
    {
        if ( !values.isEmpty() )
        {
            nextStep();
        }

        long uniqueValues = sampledSteps != 0 ? accumulatedUniqueValues / sampledSteps : 0;
        long sampledSize = sampledSteps != 0 ? accumulatedSampledSize / sampledSteps : 0;

        return new IndexSample( numDocs < 0 ? accumulatedSampledSize : numDocs, uniqueValues, sampledSize );
    }

    private void nextStep()
    {
        accumulatedUniqueValues += values.uniqueSize();
        accumulatedSampledSize += values.size();
        sampleSize = 0;

        sampledSteps++;
        values.clear();
    }

    /**
     * Evaluate initial set size that evaluate initial set as log2(sampleSizeLimit) / 2 based on provided sample size
     * limit.
     * Minimum possible size is 1 << 10.
     * Maximum possible size is 1 << 16.
     *
     * @param sampleSizeLimit specified sample size limit
     * @return initial set size
     */
    private int calculateInitialSetSize( int sampleSizeLimit )
    {
        int basedOnSampleSize = Math.max( 10, (int) (Math.log( sampleSizeLimit ) / Math.log( 2 )) / 2 );
        return 1 << Math.min( 16, basedOnSampleSize );
    }
}
