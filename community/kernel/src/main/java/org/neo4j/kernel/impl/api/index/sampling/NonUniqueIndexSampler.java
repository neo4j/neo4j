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

import org.neo4j.helpers.collection.MultiSet;

import static org.neo4j.register.Register.DoubleLong;

public class NonUniqueIndexSampler
{
    private static final int INITIAL_SIZE = 1 << 16;

    private final int bufferSizeLimit;
    private final MultiSet<String> values;

    private int sampledSteps = 0;

    // kept as longs to side step overflow issues

    private long accumulatedUniqueValues = 0;
    private long accumulatedSampledSize = 0;
    private long bufferSize = 0;

    public NonUniqueIndexSampler( int bufferSizeLimit )
    {
        this.bufferSizeLimit = bufferSizeLimit;
        this.values = new MultiSet<>( INITIAL_SIZE );
    }

    public void include( String value )
    {
        include( value, 1 );
    }

    public void include( String value, long increment )
    {
        assert increment > 0;
        if ( bufferSize >= bufferSizeLimit )
        {
            nextStep();
        }

        if ( values.increment( value, increment ) == increment )
        {
            bufferSize += value.length();
        }
    }

    public void exclude( String value )
    {
        exclude( value, 1 );
    }

    public void exclude( String value, long decrement )
    {
        assert decrement > 0;
        if ( values.increment( value, -decrement ) == 0 )
        {
            bufferSize -= value.length();
        }
    }

    public long result( DoubleLong.Out register )
    {
        if ( !values.isEmpty() )
        {
            nextStep();
        }

        long uniqueValues = sampledSteps != 0 ? accumulatedUniqueValues / sampledSteps : 0;
        long sampledSize = sampledSteps != 0 ? accumulatedSampledSize / sampledSteps : 0;
        register.write( uniqueValues, sampledSize );

        return accumulatedSampledSize;
    }

    private void nextStep()
    {
        accumulatedUniqueValues += values.uniqueSize();
        accumulatedSampledSize += values.size();
        bufferSize = 0;

        sampledSteps++;
        values.clear();
    }
}
