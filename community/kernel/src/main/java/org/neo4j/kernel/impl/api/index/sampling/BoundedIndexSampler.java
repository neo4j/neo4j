/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.kernel.api.index.ValueSampler;
import org.neo4j.register.Register;

public class BoundedIndexSampler implements ValueSampler
{
    private MultiSet<String> values;
    private int sampledSteps = 0;
    private long accumulatedUniqueValues = 0;
    private long accumulatedSampledSize = 0;
    private final int numOfUniqueElements;

    public BoundedIndexSampler( int numOfUniqueElements )
    {
        this.numOfUniqueElements = numOfUniqueElements;
        this.values = new MultiSet<>( numOfUniqueElements );
    }

    @Override
    public void include( String value )
    {
        if ( values.uniqueValueSize() >= numOfUniqueElements )
        {
            nextStep();
        }

        values.add( value );
    }

    @Override
    public void exclude( String value )
    {
        values.remove( value );
    }

    @Override
    public long result( Register.DoubleLongRegister register )
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
        accumulatedUniqueValues += values.uniqueValueSize();
        accumulatedSampledSize += values.size();

        sampledSteps++;
        values.clear();
    }

}
