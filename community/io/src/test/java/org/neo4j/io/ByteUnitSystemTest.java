/*
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
package org.neo4j.io;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ByteUnitSystemTest
{
    @Test
    public void computingCorrectValuesForEIC() throws Exception
    {
        assertThat( ByteUnitSystem.EIC.factorFromPower( 0 ), is( 1L ) );
        assertThat( ByteUnitSystem.EIC.factorFromPower( 1 ), is( 1024L ) );
        assertThat( ByteUnitSystem.EIC.factorFromPower( 2 ), is( 1024L * 1024L ) );
        assertThat( ByteUnitSystem.EIC.factorFromPower( 3 ), is( 1024L * 1024L * 1024L ) );
        assertThat( ByteUnitSystem.EIC.factorFromPower( 4 ), is( 1024L * 1024L * 1024L * 1024L ) );
        assertThat( ByteUnitSystem.EIC.factorFromPower( 5 ), is( 1024L * 1024L * 1024L * 1024L * 1024L ) );
        assertThat( ByteUnitSystem.EIC.factorFromPower( 6 ), is( 1024L * 1024L * 1024L * 1024L * 1024L * 1024L ) );
    }

    @Test
    public void computingCorrectValuesForSI() throws Exception
    {
        assertThat( ByteUnitSystem.SI.factorFromPower( 0 ), is( 1L ) );
        assertThat( ByteUnitSystem.SI.factorFromPower( 1 ), is( 1000L ) );
        assertThat( ByteUnitSystem.SI.factorFromPower( 2 ), is( 1000L * 1000L ) );
        assertThat( ByteUnitSystem.SI.factorFromPower( 3 ), is( 1000L * 1000L * 1000L ) );
        assertThat( ByteUnitSystem.SI.factorFromPower( 4 ), is( 1000L * 1000L * 1000L * 1000L ) );
        assertThat( ByteUnitSystem.SI.factorFromPower( 5 ), is( 1000L * 1000L * 1000L * 1000L * 1000L ) );
        assertThat( ByteUnitSystem.SI.factorFromPower( 6 ), is( 1000L * 1000L * 1000L * 1000L * 1000L * 1000L ) );
    }
}
