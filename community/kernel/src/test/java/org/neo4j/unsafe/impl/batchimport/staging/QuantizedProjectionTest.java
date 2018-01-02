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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuantizedProjectionTest
{
    @Test
    public void shouldProjectSteps() throws Exception
    {
        // GIVEN
        QuantizedProjection projection = new QuantizedProjection( 9, 7 );

        // WHEN/THEN
        assertTrue( projection.next( 3 ) );
        assertEquals( 2, projection.step() );

        assertTrue( projection.next( 3 ) );
        assertEquals( 3, projection.step() );

        assertTrue( projection.next( 3 ) );
        assertEquals( 2, projection.step() );

        assertFalse( projection.next( 1 ) );
    }
}
