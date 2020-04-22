/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoltProtocolVersionTest
{

    @ParameterizedTest( name = "V{0}.{1}" )
    @CsvSource( {"3, 0", "4, 0", "4, 1", "100, 100", "255, 255", "0, 0"} )
    void shouldParseVersion( int major, int minor )
    {
        BoltProtocolVersion protocolVersion = new BoltProtocolVersion( major, minor );

        BoltProtocolVersion testVersion = BoltProtocolVersion.fromRawBytes( protocolVersion.toInt() );

        assertEquals( major, testVersion.getMajorVersion() );
        assertEquals( minor, testVersion.getMinorVersion() );
    }

    @Test
    void shouldOutputCorrectLongFormatForMajorVersionOnly()
    {
        BoltProtocolVersion version = new BoltProtocolVersion( 4, 0 );
        assertEquals( 4L, version.toInt() );
    }

    @Test
    void shouldOutputCorrectLongFormatForMajorAndMinorVersion()
    {
        BoltProtocolVersion version = new BoltProtocolVersion( 4, 1 );
        assertEquals( 260L, version.toInt() );
    }
}
