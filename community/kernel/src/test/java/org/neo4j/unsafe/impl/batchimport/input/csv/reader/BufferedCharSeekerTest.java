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
package org.neo4j.unsafe.impl.batchimport.input.csv.reader;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractors.LONG;
import static org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractors.STRING;

public class BufferedCharSeekerTest
{
    @Test
    public void shouldFindCertainCharacter() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader( "abcdefg\thijklmnop\tqrstuvxyz" ) );
        Mark mark = new Mark();

        // WHEN/THEN
        // first value
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( '\t', mark.character() );
        assertFalse( mark.isEndOfLine() );
        assertEquals( "abcdefg", seeker.extract( mark, STRING ) );

        // second value
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( '\t', mark.character() );
        assertFalse( mark.isEndOfLine() );
        assertEquals( "hijklmnop", seeker.extract( mark, STRING ) );

        // third value
        assertTrue( seeker.seek( mark, TAB ) );
        assertTrue( mark.isEndOfLine() );
        assertEquals( "qrstuvxyz", seeker.extract( mark, STRING ) );

        // no more values
        assertFalse( seeker.seek( mark, TAB ) );
        assertFalse( seeker.seek( mark, TAB ) );
    }

    @Test
    public void shouldReadMultipleLines() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "1\t2\t3\n" +
                "4\t5\t6\n" ) );
        Mark mark = new Mark();

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 1L, seeker.extract( mark, LONG ).longValue() );
        assertEquals( 1, mark.lineNumber() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 2L, seeker.extract( mark, LONG ).longValue() );
        assertEquals( 1, mark.lineNumber() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 3L, seeker.extract( mark, LONG ).longValue() );
        assertTrue( mark.isEndOfLine() );
        assertEquals( 1, mark.lineNumber() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 4L, seeker.extract( mark, LONG ).longValue() );
        assertEquals( 2, mark.lineNumber() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 5L, seeker.extract( mark, LONG ).longValue() );
        assertEquals( 2, mark.lineNumber() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 6L, seeker.extract( mark, LONG ).longValue() );
        assertEquals( 2, mark.lineNumber() );

        assertTrue( mark.isEndOfLine() );
        assertFalse( seeker.seek( mark, TAB ) );
        assertEquals( 3, mark.lineNumber() ); // since there's a newline in the end of the data in this test
    }

    @Test
    public void shouldSeekThroughAdditionalBufferRead() throws Exception
    {
        // GIVEN
        Mark mark = new Mark();
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                                                  "1234,5678,9012,3456" ), 12 );
        // bufferSIze 12 should have seeker read more here     ^

        // WHEN/THEN
        seeker.seek( mark, COMMA );
        assertEquals( 1234L, seeker.extract( mark, LONG ).longValue() );
        seeker.seek( mark, COMMA );
        assertEquals( 5678L, seeker.extract( mark, LONG ).longValue() );
        seeker.seek( mark, COMMA );
        assertEquals( 9012L, seeker.extract( mark, LONG ).longValue() );
        seeker.seek( mark, COMMA );
        assertEquals( 3456L, seeker.extract( mark, LONG ).longValue() );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldHandleWindowsEndOfLineCharacters() throws Exception
    {
        // GIVEN
        Mark mark = new Mark();
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "here,comes,Windows\r\n" +
                "and,it,has\r" +
                "other,line,endings" ), 100 );

        // WHEN/THEN
        assertEquals( "here", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertEquals( "comes", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertEquals( "Windows", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertTrue( mark.isEndOfLine() );
        assertEquals( "and", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertEquals( "it", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertEquals( "has", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertTrue( mark.isEndOfLine() );
        assertEquals( "other", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertEquals( "line", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertEquals( "endings", seeker.seek( mark, COMMA ) ? seeker.extract( mark, STRING ) : "" );
        assertTrue( mark.isEndOfLine() );
    }

    @Test
    public void shouldHandleReallyWeirdChars() throws Exception
    {
        // GIVEN
        int cols = 3, rows = 3;
        char delimiter = '\t';
        String[][] data = randomWeirdValues( cols, rows, delimiter, '\n', '\r' );
        CharSeeker seeker = new BufferedCharSeeker( new StringReader( join( data, delimiter ) ) );
        Mark mark = new Mark();

        // WHEN/THEN
        for ( int row = 0; row < rows; row++ )
        {
            for ( int col = 0; col < cols; col++ )
            {
                assertTrue( seeker.seek( mark, TAB ) );
                assertEquals( data[row][col], seeker.extract( mark, Extractors.STRING ) );
            }
            assertTrue( mark.isEndOfLine() );
        }
        assertFalse( seeker.seek( mark, TAB ) );
    }

    @Test
    public void shouldHandleEmptyValues() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader( "1,,3,4" ) );
        Mark mark = new Mark();

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 1, seeker.extract( mark, Extractors.INT ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 3, seeker.extract( mark, Extractors.INT ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 4, seeker.extract( mark, Extractors.INT ).intValue() );
    }

    @Test
    public void shouldNotLetEolCharSkippingMessUpPositionsInMark() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader( "12,34,56\n789,901,23" ), 9 );
        //                 reading this char will cause new chunk read          ^
        Mark mark = new Mark();

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 12, seeker.extract( mark, Extractors.INT ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 34, seeker.extract( mark, Extractors.INT ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 56, seeker.extract( mark, Extractors.INT ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 789, seeker.extract( mark, Extractors.INT ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 901, seeker.extract( mark, Extractors.INT ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 23, seeker.extract( mark, Extractors.INT ).intValue() );

        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldSeeEofEvenIfBufferAlignsWithEnd() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader( "123,56" ), 6 );
        Mark mark = new Mark();

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 123, seeker.extract( mark, Extractors.INT ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 56, seeker.extract( mark, Extractors.INT ).intValue() );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldSkipEmptyLastValue() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one,two,three,\n" +
                "uno,dos,tres," ), 100 );
        Mark mark = new Mark();

        // WHEN
        assertNextValue( seeker, mark, COMMA, "one" );
        assertNextValue( seeker, mark, COMMA, "two" );
        assertNextValue( seeker, mark, COMMA, "three" );
        assertNextValue( seeker, mark, COMMA, "" );
        assertTrue( mark.isEndOfLine() );

        assertNextValue( seeker, mark, COMMA, "uno" );
        assertNextValue( seeker, mark, COMMA, "dos" );
        assertNextValue( seeker, mark, COMMA, "tres" );
        assertNextValue( seeker, mark, COMMA, "" );
        assertTrue( mark.isEndOfLine() );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    private void assertNextValue( CharSeeker seeker, Mark mark, int[] delimiter, String expectedValue )
            throws IOException
    {
        assertTrue( seeker.seek( mark, delimiter ) );
        assertEquals( expectedValue, seeker.extract( mark, Extractors.STRING ) );
    }

    @Test
    public void shouldContinueThroughCompletelyEmptyLines() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one,two,three\n\n\nfour,five,six" ), 200 );
        Mark mark = new Mark();

        // WHEN/THEN
        assertArrayEquals( new String[] {"one", "two", "three"}, nextLineOfAllStrings( seeker, mark ) );
        assertArrayEquals( new String[] {"four", "five", "six"}, nextLineOfAllStrings( seeker, mark ) );
    }

    private String[] nextLineOfAllStrings( CharSeeker seeker, Mark mark ) throws IOException
    {
        List<String> line = new ArrayList<>();
        while ( seeker.seek( mark, COMMA ) )
        {
            line.add( seeker.extract( mark, Extractors.STRING ) );
            if ( mark.isEndOfLine() )
            {
                break;
            }
        }
        return line.toArray( new String[line.size()] );
    }

    @Ignore( "TODO add test for characters with surrogate code points or whatever they are called," +
             " basically consisting of two char values instead of one. Add such a test when adding " +
             "support for reading such characters in the BufferedCharSeeker" )
    @Test
    public void shouldHandleDoubleCharValues()
    {
        fail( "Test not implemented" );
    }

    private String[][] randomWeirdValues( int cols, int rows, char... except )
    {
        String[][] data = new String[rows][cols];
        for ( int row = 0; row < rows; row++ )
        {
            for ( int col = 0; col < cols; col++ )
            {
                data[row][col] = randomWeirdValue( except );
            }
        }
        return data;
    }

    private String randomWeirdValue( char... except )
    {
        int length = random.nextInt( 10 )+5;
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            chars[i] = randomWeirdChar( except );
        }
        return new String( chars );
    }

    private char randomWeirdChar( char... except )
    {
        while ( true )
        {
            char candidate = (char) random.nextInt( Character.MAX_VALUE );
            if ( !in( candidate, except ) )
            {
                return candidate;
            }
        }
    }

    private boolean in( char candidate, char[] set )
    {
        for ( char ch : set )
        {
            if ( ch == candidate )
            {
                return true;
            }
        }
        return false;
    }

    private String join( String[][] data, char delimiter )
    {
        String delimiterString = String.valueOf( delimiter );
        StringBuilder builder = new StringBuilder();
        for ( String[] line : data )
        {
            for ( int i = 0; i < line.length; i++ )
            {
                builder.append( i > 0 ? delimiterString : "" ) .append( line[i] );
            }
            builder.append( "\n" );
        }
        return builder.toString();
    }

    private static final int[] TAB = new int[] { '\t' };
    private static final int[] COMMA = new int[] { ',' };
    private static final Random random = new Random();
}
