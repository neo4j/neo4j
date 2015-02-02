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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.IOException;
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
        RawIterator<CharReadable,IOException> readers = readerIteratorFromStrings( data, null );
        CharSeeker seeker = CharSeekers.charSeeker( new MultiReadable( readers ), 200, true, '"' );

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
        RawIterator<CharReadable,IOException> readers = readerIteratorFromStrings( data, '\n' );
        CharSeeker seeker = CharSeekers.charSeeker( Readables.multipleSources( readers ), 200, true, '"' );

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
                {"this is", "the first line"},
                {"where this", "is the second line"},
        };
        RawIterator<CharReadable,IOException> readers = readerIteratorFromStrings( data, '\n' );
        CharReadable reader = Readables.multipleSources( readers );
        assertEquals( 0L, reader.position() );

        // WHEN
        char[] buffer = new char[100];
        int read = reader.read( buffer, 0, 10 );
        assertEquals( 10, reader.position() );
        read += reader.read( buffer, 0, 30 );

        // THEN
        // we should now be well into the other reader
        assertEquals( read, reader.position() );
    }

    private void assertNextLine( String[] line, CharSeeker seeker, Mark mark, Extractors extractors ) throws IOException
    {
        for ( String value : line )
        {
            assertTrue( seeker.seek( mark, delimiter ) );
            assertEquals( value, seeker.extract( mark, extractors.string() ).value() );
        }
        assertTrue( mark.isEndOfLine() );
    }

    private RawIterator<CharReadable,IOException> readerIteratorFromStrings(
            final String[][] data, final Character lineEnding )
    {
        return new RawIterator<CharReadable,IOException>()
        {
            private int cursor;

            @Override
            public boolean hasNext()
            {
                return cursor < data.length;
            }

            @Override
            public CharReadable next()
            {
                return Readables.wrap( new StringReader( join( data[cursor++] ) ) );
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
    private final int[] delimiter = new int[] {','};
}
