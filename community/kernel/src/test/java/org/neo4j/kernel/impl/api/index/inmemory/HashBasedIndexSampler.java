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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

public class HashBasedIndexSampler implements IndexSampler
{
    private final Map<List<Object>,Set<Long>> data;

    public HashBasedIndexSampler( Map<List<Object>,Set<Long>> data )
    {
        this.data = data;
    }

    @Override
    public IndexSample sampleIndex() throws IndexNotFoundKernelException
    {
        if ( data == null )
        {
            throw new IndexNotFoundKernelException( "Index dropped while sampling." );
        }

        long uniqueValues = 0;
        long indexSize = 0;
        for ( Map.Entry<List<Object>,Set<Long>> entry : data.entrySet() )
        {
            Set<Long> nodeIds = entry.getValue();
            if ( !nodeIds.isEmpty() )
            {
                uniqueValues++;
                indexSize += nodeIds.size();
            }
        }

        return new IndexSample( indexSize, uniqueValues, indexSize );
    }
}
