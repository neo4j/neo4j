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
package org.neo4j.internal.helpers.progress;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.neo4j.time.FakeClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.internal.helpers.progress.Indicator.Textual.DOTS_PER_LINE;

class IndicatorTest
{
    @Test
    void shouldIncludeDeltaTimes()
    {
        // given
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter( bout );
        FakeClock clock = new FakeClock();
        Indicator.Textual indicator = new Indicator.Textual( "Test", out, true, clock, 'D' );

        // when
        int line = 0;
        clock.forward( 1, TimeUnit.SECONDS );
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 10%
        clock.forward( 100, TimeUnit.MILLISECONDS );
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 20%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 30%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 40%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 50%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 60%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 70%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 80%
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 90%
        clock.forward( 3, TimeUnit.SECONDS );
        indicator.progress( DOTS_PER_LINE * line, DOTS_PER_LINE * ++line ); // 100%

        // then
        out.flush();
        String output = bout.toString();
        assertThat( output, containsString( "10% D1s" ) );
        assertThat( output, containsString( "20% D100ms" ) );
        assertThat( output, containsString( "40% D0ms" ) );
        assertThat( output, containsString( "50% D0ms" ) );
        assertThat( output, containsString( "60% D0ms" ) );
        assertThat( output, containsString( "70% D0ms" ) );
        assertThat( output, containsString( "80% D0ms" ) );
        assertThat( output, containsString( "90% D0ms" ) );
        assertThat( output, containsString( "100% D3s" ) );
    }
}
