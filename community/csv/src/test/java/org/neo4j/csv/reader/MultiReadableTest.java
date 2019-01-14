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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import org.neo4j.collection.RawIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.csv.reader.Readables.wrap;

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
        RawIterator<CharReadable,IOException> readers = readerIteratorFromStrings( data, '\n' );
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
    public void shouldTrackAbsolutePosition() throws Exception
    {
        // GIVEN
        String[][] data = new String[][] {
                {"this is", "the first line"},        // 21+delimiter+newline = 23 characters
                {"where this", "is the second line"}, // 28+delimiter+newline = 30 characters
        };
        RawIterator<CharReadable,IOException> readers = readerIteratorFromStrings( data, '\n' );
        CharReadable reader = new MultiReadable( readers );
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
        assertEquals( 23 + 15, reader.position() );
        reader.read( buffer, buffer.front() );
        assertEquals( 23 + 30, reader.position() );
        reader.read( buffer, buffer.front() );
        assertFalse( buffer.hasAvailable() );
    }

    @Test
    public void shouldNotCrossSourcesInOneRead() throws Exception
    {
        // given
        String source1 = "abcdefghijklm";
        String source2 = "nopqrstuvwxyz";
        String[][] data = new String[][] { {source1}, {source2} };
        CharReadable readable = new MultiReadable( readerIteratorFromStrings( data, '\n' ) );

        // when
        char[] target = new char[source1.length() + source2.length() / 2];
        int read = readable.read( target, 0, target.length );

        // then
        assertEquals( source1.length() + 1/*added newline-char*/, read );

        // and when
        target = new char[source2.length()];
        read = readable.read( target, 0, target.length );

        // then
        assertEquals( source2.length(), read );

        read = readable.read( target, 0, target.length );
        assertEquals( 1/*added newline-char*/, read );
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
                String string = join( data[cursor++] );
                return wrap( new StringReader( string )
                {
                    @Override
                    public String toString()
                    {
                        return "Reader" + cursor;
                    }
                }, string.length() * 2 );
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
