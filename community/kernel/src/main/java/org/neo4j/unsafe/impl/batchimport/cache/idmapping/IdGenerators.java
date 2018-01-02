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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping;

import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;

/**
 * Common {@link IdGenerator} implementations.
 */
public class IdGenerators
{
    /**
     * @return an {@link IdGenerator} assuming that the input ids are {@link Long} objects and casts to
     * primitive longs. This is for when the {@link InputNode#id()} contains an actual record id, in the
     * form of a {@link Long}.
     */
    public static IdGenerator fromInput()
    {
        return new FromInput();
    }

    private static class FromInput implements IdGenerator
    {
        private long lastSeenId;

        @Override
        public long generate( Object inputId )
        {
            assert inputId instanceof Long;

            long inputLongId = ((Long)inputId).longValue();
            if ( lastSeenId != -1 && inputLongId < lastSeenId )
            {
                throw new IllegalArgumentException( "Cannot go backwards in node id sequence, last seen was " +
                        lastSeenId + ", given id is " + inputLongId );
            }
            lastSeenId = inputLongId;

            return inputLongId;
        }

        @Override
        public boolean dependsOnInput()
        {
            return true;
        }
    }

    /**
     * @param startingId the first id returned. The next one will be this value + 1, then + 2 a.s.o.
     * @return an {@link IdGenerator} that returns ids incrementally, starting from the given id.
     */
    public static IdGenerator startingFrom( final long startingId )
    {
        return new IdGenerator()
        {
            private final IdSequence ids = new BatchingIdSequence( startingId );

            @Override
            public long generate( Object inputId )
            {
                return ids.nextId();
            }

            @Override
            public boolean dependsOnInput()
            {
                return false;
            }
        };
    }

    /**
     * @return an {@link IdGenerator} that returns ids incrementally, starting from 0.
     */
    public static IdGenerator startingFromTheBeginning()
    {
        return startingFrom( 0 );
    }
}
