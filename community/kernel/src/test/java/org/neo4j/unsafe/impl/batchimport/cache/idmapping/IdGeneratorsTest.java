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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;

public class IdGeneratorsTest
{
    @Test
    public void shouldNotUseReservedMinusOneId() throws Exception
    {
        // GIVEN
        int idsBefore = 100;
        IdGenerator generator = IdGenerators.startingFrom( IdGeneratorImpl.INTEGER_MINUS_ONE - idsBefore );

        // WHEN/THEN
        long previous = 0;
        for ( int i = 0; i < idsBefore; i++ )
        {
            long current = generator.generate( null ); // This generator doesn't care about the input argument anyway.
            assertTrue( previous < current );
            assertNotEquals( current, IdGeneratorImpl.INTEGER_MINUS_ONE );
            previous = current;
        }
    }
}
