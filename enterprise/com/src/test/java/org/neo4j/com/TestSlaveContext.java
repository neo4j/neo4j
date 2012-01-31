/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class TestSlaveContext
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void assertSimilarity()
    {
        // Different machine ids
        assertFalse( new SlaveContext( 1234, 1, 2, new SlaveContext.Tx[0], 0, 0 ).equals( new SlaveContext( 1234, 2, 2, new SlaveContext.Tx[0], 0, 0 ) ) );

        // Different event identifiers
        assertFalse( new SlaveContext( 1234, 1, 10, new SlaveContext.Tx[0], 0, 0 ).equals( new SlaveContext( 1234, 1, 20, new SlaveContext.Tx[0], 0, 0 ) ) );

        // Different session ids
        assertFalse( new SlaveContext( 1001, 1, 5, new SlaveContext.Tx[0], 0, 0 ).equals( new SlaveContext( 1101, 1, 5, new SlaveContext.Tx[0], 0, 0 ) ) );

        // Same everything
        assertEquals( new SlaveContext( 12345, 4, 9, new SlaveContext.Tx[0], 0, 0 ), new SlaveContext( 12345, 4, 9, new SlaveContext.Tx[0], 0, 0 ) );
    }
}
