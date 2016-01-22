/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.schema.IndexSampler;

public class HashBasedIndexSampler implements IndexSampler
{
    private final Map<Object,Set<Long>> data;

    public HashBasedIndexSampler( Map<Object,Set<Long>> data )
    {
        this.data = data;
    }

    @Override
    public long sampleIndex( Register.DoubleLong.Out result ) throws IndexNotFoundKernelException
    {
        if ( data == null )
        {
            throw new IndexNotFoundKernelException( "Index dropped while sampling." );
        }
        long[] uniqueAndSize = {0, 0};
        try
        {
            data.forEach( ( value, nodeIds ) -> {
                int ids = nodeIds.size();
                if ( ids > 0 )
                {
                    uniqueAndSize[0] += 1;
                    uniqueAndSize[1] += ids;
                }
            } );
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }

        result.write( uniqueAndSize[0], uniqueAndSize[1] );
        return uniqueAndSize[1];
    }
}
