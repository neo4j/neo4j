/**
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping;

public class IdGenerators
{
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
    }
}
