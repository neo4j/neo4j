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
package org.neo4j.kernel.api.impl.index;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.index.ValueSampler;
import org.neo4j.register.Register;

public class SkipOracleSampler implements ValueSampler
{
    private final SkipOracle oracle;

    private long leftToSkip = 0l;
    private long sampleSize = 0l;
    private Set<Object> values = new HashSet<>();

    public SkipOracleSampler( SkipOracle oracle )
    {
        this.oracle = oracle;
    }

    @Override
    public void considerValue( Object value )
    {
        if ( leftToSkip == 0 )
        {
            //
            leftToSkip = oracle.skip();
            values.add( value );
            sampleSize++;
        }
        else
        {
            leftToSkip--;
        }
    }

    @Override
    public void samplingResult( Register.DoubleLongRegister register )
    {
        register.write( values.size(), sampleSize );
    }
}
