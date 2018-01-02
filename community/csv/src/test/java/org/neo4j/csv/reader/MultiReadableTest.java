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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.neo4j.collection.RawIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiReadableTest
{
    @Test
    public void shouldReadFromMultipleReaders() throws Exception
    {
        // GIVEN
        String[][] data = new String[][] {
                {"this is", "the first line"},
                {"where this", "is the second line"},
                {"and here comes", "the third line"}
        };
        RawIterator<Reader,IOException> readers = readerIteratorFromStrings( data, null );
        CharSeeker seeker = CharSeekers.charSeeker( new MultiReadable( readers ), CONFIG, true );

        // WHEN/THEN
        for ( String[] line : data )
        {
            assertNextLine( line, seeker, mark, extractors );
        }
        assertFalse( seeker.seek( mark, delimiter ) );
        seeker.close();
    }

    @Test
    public void shouldHandleSourcesEndingWithNewLine() throws Exception
    {
        // GIVEN
        String[][] data = new String[][] {
                {"this is", "the first line"},
                {"where this", "is the second line"},
        };

        // WHEN
        RawIterator<Reader,IOException> readers = readerIteratorFromStrings( data, '\n' );
        CharSeeker seeker = CharSeekers.charSeeker( Readables.sources( readers ), CONFIG, true );

        // WHEN/THEN
        for ( String[] line : data )
        {
            assertNextLine( line, seeker, mark, extractors );
        }
        assertFalse( seeker.seek( mark, delimiter ) );
        seeker.close();
    }

    @Test
    public void shouldTrackAbsolutePosition() throws Exception
    {
        // GIVEN
        String[][] data = new String[][] {
                {"this is", "the first line"},        // 21+delimiter+newline = 23 characters
                {"where this", "is the second line"}, // 28+delimiter+newline = 30 characters
        };
        RawIterator<Reader,IOException> readers = readerIteratorFromStrings( data, '\n' );
        CharReadable reader = Readables.sources( readers );
        assertEquals( 0L, reader.position() );
        SectionedCharBuffer buffer = new SectionedCharBuffer( 15 );

        // WHEN
        reader.read( buffer, buffer.front() );
        assertEquals( 15, reader.position() );
        reader.read( buffer, buffer.front() );
        assertEquals( "Should not transition to a new reader in the middle of a read", 23, reader.position() );
        assertEquals( "Reader1", reader.sourceDescription() );

        // we will transition to the new reader in the call below
        reader.read( buffer, buffer.front() );
        assertEquals( 23+15, reader.position() );
        reader.read( buffer, buffer.front() );
        assertEquals( 23+30, reader.position() );
        reader.read( buffer, buffer.front() );
        assertFalse( buffer.hasAvailable() );
    }

    private static final Configuration CONFIG = new Configuration.Overridden( Configuration.DEFAULT )
    {
        @Override
        public int bufferSize()
        {
            return 200;
        }
    };

    private void assertNextLine( String[] line, CharSeeker seeker, Mark mark, Extractors extractors ) throws IOException
    {
        for ( String value : line )
        {
            assertTrue( seeker.seek( mark, delimiter ) );
            assertEquals( value, seeker.extract( mark, extractors.string() ).value() );
        }
        assertTrue( mark.isEndOfLine() );
    }

    private RawIterator<Reader,IOException> readerIteratorFromStrings(
            final String[][] data, final Character lineEnding )
    {
        return new RawIterator<Reader,IOException>()
        {
            private int cursor;

            @Override
            public boolean hasNext()
            {
                return cursor < data.length;
            }

            @Override
            public Reader next()
            {
                return new StringReader( join( data[cursor++] ) )
                {
                    @Override
                    public String toString()
                    {
                        return "Reader" + cursor;
                    }
                };
            }

            private String join( String[] strings )
            {
                StringBuilder builder = new StringBuilder();
                for ( String string : strings )
                {
                    builder.append( builder.length() > 0 ? "," : "" ).append( string );
                }
                if ( lineEnding != null )
                {
                    builder.append( lineEnding );
                }
                return builder.toString();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private final Mark mark = new Mark();
    private final Extractors extractors = new Extractors( ';' );
    private final int delimiter = ',';
}
