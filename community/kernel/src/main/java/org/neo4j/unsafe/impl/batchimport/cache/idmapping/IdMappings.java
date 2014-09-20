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

import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * Place to instantiate common {@link IdMapping} implementations.
 */
public class IdMappings
{
    /**
     * @return an {@link IdMapping} where there are no need for translating between what ids are given in the input
     * and the ids what the database uses. Basically this is used when the actual node ids are given to
     * the {@link InputNode} and {@link InputRelationship} instances directly during an import.
     */
    public static IdMapping actual()
    {
        return new PreInitializedIdMapping( IdMappers.actual(), IdGenerators.fromInput() )
        {
            @Override
            public String toString()
            {
                return "IdMapping[actual node ids]";
            }
        };
    }

    /**
     * @param cacheFactory {@link LongArrayFactory} for allocating memory for the cache used by this index.
     * @return {@link IdMapping} for when node ids given to {@link InputNode} and {@link InputRelationship} are
     * strings with o association with the actual ids in the database.
     */
    public static IdMapping strings( final LongArrayFactory cacheFactory )
    {
        return new PreInitializedIdMapping( IdMappers.strings( cacheFactory ), IdGenerators.startingFrom( 0 ) )
        {
            @Override
            public String toString()
            {
                return "IdMapping[string node ids]";
            }
        };
    }

    private static class PreInitializedIdMapping implements IdMapping
    {
        private final IdMapper idMapper;
        private final IdGenerator idGenerator;

        PreInitializedIdMapping( IdMapper idMapper, IdGenerator idGenerator )
        {
            this.idMapper = idMapper;
            this.idGenerator = idGenerator;
        }

        @Override
        public IdMapper idMapper()
        {
            return idMapper;
        }

        @Override
        public IdGenerator idGenerator()
        {
            return idGenerator;
        }
    }
}
