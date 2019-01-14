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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ThresholdConfigValueTest
{
    @Test
    public void shouldParseCorrectly()
    {
        ThresholdConfigParser.ThresholdConfigValue value = ThresholdConfigParser.parse( "25 files" );
        assertEquals( "files", value.type );
        assertEquals( 25, value.value );

        value = ThresholdConfigParser.parse( "4g size" );
        assertEquals( "size", value.type );
        assertEquals( 1L << 32, value.value );
    }

    @Test
    public void shouldThrowExceptionOnUnknownType()
    {
        try
        {
            ThresholdConfigParser.parse( "more than one spaces is invalid" );
            fail("Should not parse unknown types");
        }
        catch ( IllegalArgumentException e )
        {
            // good
        }
    }

    @Test
    public void shouldReturnNoPruningForTrue()
    {
        ThresholdConfigParser.ThresholdConfigValue value = ThresholdConfigParser.parse( "true" );
        assertSame( ThresholdConfigParser.ThresholdConfigValue.NO_PRUNING, value );
    }

    @Test
    public void shouldReturnKeepOneEntryForFalse()
    {
        ThresholdConfigParser.ThresholdConfigValue value = ThresholdConfigParser.parse( "false" );
        assertEquals( "entries", value.type );
        assertEquals( 1, value.value );
    }
}
