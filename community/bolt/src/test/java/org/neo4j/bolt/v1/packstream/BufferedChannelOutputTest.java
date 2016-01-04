/*
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
package org.neo4j.bolt.v1.packstream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;

public class BufferedChannelOutputTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldThrowWhenAskedToWriteMoreThanGiven() throws Throwable
    {
        // Given
        BufferedChannelOutput out = new BufferedChannelOutput( 12 );

        // Expect
        exception.expect( IOException.class );
        exception.expectMessage( "Asked to write 2 bytes, but there is only 1 bytes available in data provided." );

        // When
        out.writeBytes( new byte[]{1}, 0, 2 );
    }
}