/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.csv.reader;

import java.io.StringReader;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.csv.reader.QuoteAwareCharSeeker.quoteAware;

public class QuoteAwareCharSeekerTest
{
    @Test
    public void shouldReadQuotes() throws Exception
    {
        // GIVEN
        CharSeeker seeker = quoteAware( new BufferedCharSeeker( new StringReader(
                "value one\t\"value two\"\tvalue three" ) ), '"' );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value two", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    @Test
    public void shouldReadQuotedValuesWithDelimiterInside() throws Exception
    {
        // GIVEN
        CharSeeker seeker = quoteAware( new BufferedCharSeeker( new StringReader(
                "value one\t'value\ttwo'\tvalue three" ) ), '\'' );
        Mark mark = new Mark();

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value\ttwo", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    @Test
    public void shouldReadQuotedValuesWithNewLinesInside() throws Exception
    {
        // GIVEN
        CharSeeker seeker = quoteAware( new BufferedCharSeeker( new StringReader(
                "value one\t\"value\ntwo\"\tvalue three" ) ), '"' );
        Mark mark = new Mark();

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value\ntwo", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    private static final int[] TAB = new int[] {'\t'};
    private final Mark mark = new Mark();
    private final Extractors extractors = new Extractors( ',' );
}
