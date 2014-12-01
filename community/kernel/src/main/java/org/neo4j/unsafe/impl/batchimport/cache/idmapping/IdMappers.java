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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.LongEncoder;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Radix;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.StringEncoder;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * Place to instantiate common {@link IdMapper} implementations.
 */
public class IdMappers
{
    private static class ActualIdMapper implements IdMapper
    {
        @Override
        public void put( Object inputId, long actualId )
        {   // No need to remember anything
        }

        @Override
        public boolean needsPreparation()
        {
            return false;
        }

        @Override
        public void prepare( ResourceIterable<Object> nodeData )
        {   // No need to prepare anything
        }

        @Override
        public long get( Object inputId )
        {
            return ((Long)inputId).longValue();
        }

        @Override
        public void visitMemoryStats( MemoryStatsVisitor visitor )
        {   // No memory usage
        }
    }

    /**
     * An {@link IdMapper} that doesn't touch the input ids, but just asserts that node ids arrive in ascending order.
     */
    public static IdMapper actual()
    {
        return new ActualIdMapper();
    }

    /**
     * An {@link IdMapper} capable of mapping {@link String strings} to long ids.
     *
     * @param cacheFactory {@link NumberArrayFactory} for allocating memory for the cache used by this index.
     * @return {@link IdMapper} for when node ids given to {@link InputNode} and {@link InputRelationship} are
     * strings with o association with the actual ids in the database.
     */
    public static IdMapper strings( NumberArrayFactory cacheFactory )
    {
        return new EncodingIdMapper( cacheFactory, new StringEncoder(), new Radix.String() );
    }

    /**
     * An {@link IdMapper} capable of mapping {@link Long arbitrary longs} to long ids.
     *
     * @param cacheFactory {@link NumberArrayFactory} for allocating memory for the cache used by this index.
     * @return {@link IdMapper} for when node ids given to {@link InputNode} and {@link InputRelationship} are
     * strings with o association with the actual ids in the database.
     */
    public static IdMapper longs( NumberArrayFactory cacheFactory )
    {
        return new EncodingIdMapper( cacheFactory, new LongEncoder(), new Radix.Long() );
    }
}
