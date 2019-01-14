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
package org.neo4j.helpers;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FormatTest
{
    @Test
    public void shouldDisplayBytes()
    {
        // when
        String format = Format.bytes( 123 );

        // then
        assertTrue( format.contains( String.valueOf( 123 ) ) );
        assertTrue( format.endsWith( " B" ) );
    }

    @Test
    public void shouldDisplayKiloBytes()
    {
        // when
        String format = Format.bytes( 1_234 );

        // then
        assertTrue( format.startsWith( "1" ) );
        assertTrue( format.endsWith( " kB" ) );
    }

    @Test
    public void shouldDisplayMegaBytes()
    {
        // when
        String format = Format.bytes( 1_234_567 );

        // then
        assertTrue( format.startsWith( "1" ) );
        assertTrue( format.endsWith( " MB" ) );
    }

    @Test
    public void shouldDisplayGigaBytes()
    {
        // when
        String format = Format.bytes( 1_234_567_890 );

        // then
        assertTrue( format.startsWith( "1" ) );
        assertTrue( format.endsWith( " GB" ) );
    }

    @Test
    public void shouldDisplayPlainCount()
    {
        // when
        String format = Format.count( 10 );

        // then
        assertTrue( format.startsWith( "10" ) );
    }

    @Test
    public void shouldDisplayThousandCount()
    {
        // when
        String format = Format.count( 2_000 );

        // then
        assertTrue( format.startsWith( "2" ) );
        assertTrue( format.endsWith( "k" ) );
    }

    @Test
    public void shouldDisplayMillionCount()
    {
        // when
        String format = Format.count( 2_000_000 );

        // then
        assertTrue( format.startsWith( "2" ) );
        assertTrue( format.endsWith( "M" ) );
    }

    @Test
    public void shouldDisplayBillionCount()
    {
        // when
        String format = Format.count( 2_000_000_000 );

        // then
        assertTrue( format.startsWith( "2" ) );
        assertTrue( format.endsWith( "G" ) );
    }

    @Test
    public void shouldDisplayTrillionCount()
    {
        // when
        String format = Format.count( 4_000_000_000_000L );

        // then
        assertTrue( format.startsWith( "4" ) );
        assertTrue( format.endsWith( "T" ) );
    }

    @Test
    public void displayDuration()
    {
        assertThat( Format.duration( MINUTES.toMillis( 1 ) + SECONDS.toMillis( 2 ) ), is( "1m 2s" ) );
        assertThat( Format.duration( 42 ), is( "42ms" ) );
        assertThat( Format.duration( 0 ), is( "0ms" ) );
    }
}
