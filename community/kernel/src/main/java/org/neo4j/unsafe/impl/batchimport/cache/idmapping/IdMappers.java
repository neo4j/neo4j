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

public class IdMappers
{
    /**
     * An {@link IdMapper} that doesn't touch the input ids, but just asserts that node ids arrive in ascending order.
     */
    public static IdMapper actual()
    {
        return new IdMapper()
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
            public void prepare()
            {   // No need to prepare anything
            }

            @Override
            public long get( Object inputId )
            {
                return ((Long)inputId).longValue();
            }
        };
    }
}
