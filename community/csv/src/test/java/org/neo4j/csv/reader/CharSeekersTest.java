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

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CharSeekersTest
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
        Iterator<Readable> readers = readerIteratorFromStrings( data );
        CharSeeker seeker = CharSeekers.charSeeker( readers, 200, false, '"' );

        // WHEN/THEN
        for ( String[] line : data )
        {
            assertNextLine( line, seeker, mark, extractors );
        }
        assertFalse( seeker.seek( mark, delimiter ) );
        seeker.close();
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

    private Iterator<Readable> readerIteratorFromStrings( final String[][] data )
    {
        return new Iterator<Readable>()
        {
            private int cursor;

            @Override
            public boolean hasNext()
            {
                return cursor < data.length;
            }

            @Override
            public Readable next()
            {
                return new StringReader( join( data[cursor++] ) );
            }

            private String join( String[] strings )
            {
                StringBuilder builder = new StringBuilder();
                for ( String string : strings )
                {
                    builder.append( builder.length() > 0 ? "," : "" ).append( string );
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
