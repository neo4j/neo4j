/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class ArrayEncoderTest
{
    @Test
    public void shouldEncodeArrays() throws Exception
    {
        assertEquals( "D1.0|2.0|3.0|", ArrayEncoder.encode( new int[]{1, 2, 3} ) );
        assertEquals( "Ztrue|false|", ArrayEncoder.encode( new boolean[]{true, false} ) );
        assertEquals( "LYWxp|YXJl|eW91|b2s=|", ArrayEncoder.encode( new String[]{"ali", "are", "you", "ok"} ) );
    }
}
