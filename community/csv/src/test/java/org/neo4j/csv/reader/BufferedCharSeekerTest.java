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

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.wrap;

@RunWith( Parameterized.class )
public class BufferedCharSeekerTest
{
    private static final String TEST_SOURCE = "TestSource";
    private final boolean useThreadAhead;

    @Parameters( name = "{1}" )
    public static Collection<Object[]> data()
    {
        return asList(
                new Object[] {Boolean.FALSE, "without thread-ahead"},
                new Object[] {Boolean.TRUE, "with thread-ahead"} );
    }

    /**
     * @param description used to provider a better description of what the boolean values means,
     * which shows up in the junit results.
     */
    public BufferedCharSeekerTest( boolean useThreadAhead, String description )
    {
        this.useThreadAhead = useThreadAhead;
    }

    @Test
    public void shouldFindCertainCharacter() throws Exception
    {
        // GIVEN
        seeker = seeker( "abcdefg\thijklmnop\tqrstuvxyz" );

        // WHEN/THEN
        // first value
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( '\t', mark.character() );
        assertFalse( mark.isEndOfLine() );
        assertEquals( "abcdefg", seeker.extract( mark, extractors.string() ).value() );

        // second value
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( '\t', mark.character() );
        assertFalse( mark.isEndOfLine() );
        assertEquals( "hijklmnop", seeker.extract( mark, extractors.string() ).value() );

        // third value
        assertTrue( seeker.seek( mark, TAB ) );
        assertTrue( mark.isEndOfLine() );
        assertEquals( "qrstuvxyz", seeker.extract( mark, extractors.string() ).value() );

        // no more values
        assertFalse( seeker.seek( mark, TAB ) );
        assertFalse( seeker.seek( mark, TAB ) );
    }

    @Test
    public void shouldReadMultipleLines() throws Exception
    {
        // GIVEN
        seeker = seeker(
                "1\t2\t3\n" +
                "4\t5\t6\n" );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 1L, seeker.extract( mark, extractors.long_() ).longValue() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 2L, seeker.extract( mark, extractors.long_() ).longValue() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 3L, seeker.extract( mark, extractors.long_() ).longValue() );
        assertTrue( mark.isEndOfLine() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 4L, seeker.extract( mark, extractors.long_() ).longValue() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 5L, seeker.extract( mark, extractors.long_() ).longValue() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( 6L, seeker.extract( mark, extractors.long_() ).longValue() );

        assertTrue( mark.isEndOfLine() );
        assertFalse( seeker.seek( mark, TAB ) );
    }

    @Test
    public void shouldSeekThroughAdditionalBufferRead() throws Exception
    {
        // GIVEN
        seeker = seeker( "1234,5678,9012,3456", config( 12 ) );
        // read more here             ^

        // WHEN/THEN
        seeker.seek( mark, COMMA );
        assertEquals( 1234L, seeker.extract( mark, extractors.long_() ).longValue() );
        seeker.seek( mark, COMMA );
        assertEquals( 5678L, seeker.extract( mark, extractors.long_() ).longValue() );
        seeker.seek( mark, COMMA );
        assertEquals( 9012L, seeker.extract( mark, extractors.long_() ).longValue() );
        seeker.seek( mark, COMMA );
        assertEquals( 3456L, seeker.extract( mark, extractors.long_() ).longValue() );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldHandleWindowsEndOfLineCharacters() throws Exception
    {
        // GIVEN
        seeker = seeker(
                "here,comes,Windows\r\n" +
                "and,it,has\r" +
                "other,line,endings" );

        // WHEN/THEN
        assertEquals( "here", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertEquals( "comes", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertEquals( "Windows", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertTrue( mark.isEndOfLine() );
        assertEquals( "and", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertEquals( "it", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertEquals( "has", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertTrue( mark.isEndOfLine() );
        assertEquals( "other", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertEquals( "line", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertEquals( "endings", seeker.seek( mark, COMMA ) ? seeker.extract( mark, extractors.string() ).value() : "" );
        assertTrue( mark.isEndOfLine() );
    }

    @Test
    public void shouldHandleReallyWeirdChars() throws Exception
    {
        // GIVEN
        int cols = 3, rows = 3;
        char delimiter = '\t';
        String[][] data = randomWeirdValues( cols, rows, delimiter, '\n', '\r' );
        seeker = seeker( join( data, delimiter ) );

        // WHEN/THEN
        for ( int row = 0; row < rows; row++ )
        {
            for ( int col = 0; col < cols; col++ )
            {
                assertTrue( seeker.seek( mark, TAB ) );
                assertEquals( data[row][col], seeker.extract( mark, extractors.string() ).value() );
            }
            assertTrue( mark.isEndOfLine() );
        }
        assertFalse( seeker.seek( mark, TAB ) );
    }

    @Test
    public void shouldHandleEmptyValues() throws Exception
    {
        // GIVEN
        seeker = seeker( "1,,3,4" );

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 1, seeker.extract( mark, extractors.int_() ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 3, seeker.extract( mark, extractors.int_() ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 4, seeker.extract( mark, extractors.int_() ).intValue() );
    }

    @Test
    public void shouldNotLetEolCharSkippingMessUpPositionsInMark() throws Exception
    {
        // GIVEN
        seeker = seeker( "12,34,56\n789,901,23", config( 9 ) );
        // read more here          ^        ^

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 12, seeker.extract( mark, extractors.int_() ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 34, seeker.extract( mark, extractors.int_() ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 56, seeker.extract( mark, extractors.int_() ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 789, seeker.extract( mark, extractors.int_() ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 901, seeker.extract( mark, extractors.int_() ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 23, seeker.extract( mark, extractors.int_() ).intValue() );

        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldSeeEofEvenIfBufferAlignsWithEnd() throws Exception
    {
        // GIVEN
        seeker = seeker( "123,56", config( 6 ) );

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 123, seeker.extract( mark, extractors.int_() ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 56, seeker.extract( mark, extractors.int_() ).intValue() );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldSkipEmptyLastValue() throws Exception
    {
        // GIVEN
        seeker = seeker(
                "one,two,three,\n" +
                "uno,dos,tres," );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "one" );
        assertNextValue( seeker, mark, COMMA, "two" );
        assertNextValue( seeker, mark, COMMA, "three" );
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertTrue( mark.isEndOfLine() );

        assertNextValue( seeker, mark, COMMA, "uno" );
        assertNextValue( seeker, mark, COMMA, "dos" );
        assertNextValue( seeker, mark, COMMA, "tres" );
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertTrue( mark.isEndOfLine() );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldExtractEmptyStringForEmptyQuotedString() throws Exception
    {
        // GIVEN
        seeker = seeker( "\"\",,\"\"" );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "" );
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertNextValue( seeker, mark, COMMA, "" );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldExtractNullForEmptyFieldWhenWeSkipEOLChars() throws Exception
    {
        // GIVEN
        seeker = seeker( "\"\",\r\n" );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "" );
        assertNextValueNotExtracted( seeker, mark, COMMA );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldContinueThroughCompletelyEmptyLines() throws Exception
    {
        // GIVEN
        seeker = seeker( "one,two,three\n\n\nfour,five,six" );

        // WHEN/THEN
        assertArrayEquals( new String[] {"one", "two", "three"}, nextLineOfAllStrings( seeker, mark ) );
        assertArrayEquals( new String[] {"four", "five", "six"}, nextLineOfAllStrings( seeker, mark ) );
    }

    @Ignore( "TODO add test for characters with surrogate code points or whatever they are called," +
             " basically consisting of two char values instead of one. Add such a test when adding " +
             "support for reading such characters in the BufferedCharSeeker" )
    @Test
    public void shouldHandleDoubleCharValues()
    {
        fail( "Test not implemented" );
    }

    @Test
    public void shouldReadQuotes() throws Exception
    {
        // GIVEN
        seeker = seeker( "value one\t\"value two\"\tvalue three" );

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
        seeker = seeker( "value one\t\"value\ttwo\"\tvalue three" );

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
        seeker = seeker( "value one\t\"value\ntwo\"\tvalue three", config( 1_000, true ) );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value\ntwo", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    @Test
    public void shouldHandleDoubleQuotes() throws Exception
    {
        // GIVEN
        seeker = seeker( "\"value \"\"one\"\"\"\t\"\"\"value\"\" two\"\t\"va\"\"lue\"\" three\"" );

        // "value ""one"""
        // """value"" two"
        // "va""lue"" three"

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value \"one\"", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "\"value\" two", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "va\"lue\" three", seeker.extract( mark, extractors.string() ).value() );
    }

    @Test
    public void shouldHandleSlashEncodedQuotes() throws Exception
    {
        // GIVEN
        seeker = seeker( "\"value \\\"one\\\"\"\t\"\\\"value\\\" two\"\t\"va\\\"lue\\\" three\"" );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value \"one\"", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "\"value\" two", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "va\"lue\" three", seeker.extract( mark, extractors.string() ).value() );
    }

    @Test
    public void shouldRecognizeStrayQuoteCharacters() throws Exception
    {
        // GIVEN
        seeker = seeker(
                "one,two\",th\"ree\n" +
                "four,five,s\"ix" );

        // THEN
        assertNextValue( seeker, mark, COMMA, "one" );
        assertNextValue( seeker, mark, COMMA, "two\"" );
        assertNextValue( seeker, mark, COMMA, "th\"ree" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "four" );
        assertNextValue( seeker, mark, COMMA, "five" );
        assertNextValue( seeker, mark, COMMA, "s\"ix" );
        assertTrue( mark.isEndOfLine() );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldNotMisinterpretUnfilledRead() throws Exception
    {
        // GIVEN
        CharReadable readable = new ControlledCharReadable(
                "123,456,789\n" +
                "abc,def,ghi", 5 );
        seeker = seeker( readable );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "123" );
        assertNextValue( seeker, mark, COMMA, "456" );
        assertNextValue( seeker, mark, COMMA, "789" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "abc" );
        assertNextValue( seeker, mark, COMMA, "def" );
        assertNextValue( seeker, mark, COMMA, "ghi" );
        assertTrue( mark.isEndOfLine() );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldNotFindAnyValuesForEmptySource() throws Exception
    {
        // GIVEN
        seeker = seeker( "" );

        // WHEN/THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldSeeQuotesInQuotes() throws Exception
    {
        // GIVEN
        //                4,     """",   "f\oo"
        seeker = seeker( "4,\"\"\"\",\"f\\oo\"" );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "4" );
        assertNextValue( seeker, mark, COMMA, "\"" );
        assertNextValue( seeker, mark, COMMA, "f\\oo" );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldEscapeBackslashesInQuotes() throws Exception
    {
        // GIVEN
        //                4,    "\\\"",   "f\oo"
        seeker = seeker( "4,\"\\\\\\\"\",\"f\\oo\"" );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "4" );
        assertNextValue( seeker, mark, COMMA, "\\\"" );
        assertNextValue( seeker, mark, COMMA, "f\\oo" );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldListenToMusic() throws Exception
    {
        // GIVEN
        String data =
                "\"1\",\"ABBA\",\"1992\"\n" +
                "\"2\",\"Roxette\",\"1986\"\n" +
                "\"3\",\"Europe\",\"1979\"\n" +
                "\"4\",\"The Cardigans\",\"1992\"";
        seeker = seeker( data );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "1" );
        assertNextValue( seeker, mark, COMMA, "ABBA" );
        assertNextValue( seeker, mark, COMMA, "1992" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "2" );
        assertNextValue( seeker, mark, COMMA, "Roxette" );
        assertNextValue( seeker, mark, COMMA, "1986" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "3" );
        assertNextValue( seeker, mark, COMMA, "Europe" );
        assertNextValue( seeker, mark, COMMA, "1979" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "4" );
        assertNextValue( seeker, mark, COMMA, "The Cardigans" );
        assertNextValue( seeker, mark, COMMA, "1992" );
        assertTrue( mark.isEndOfLine() );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @Test
    public void shouldFailOnCharactersAfterEndQuote() throws Exception
    {
        // GIVEN
        String data = "abc,\"def\"ghi,jkl";
        seeker = seeker( data );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "abc" );
        try
        {
            seeker.seek( mark, COMMA );
            fail( "Should've failed" );
        }
        catch ( DataAfterQuoteException e )
        {
            // THEN good
            assertEquals( 0, e.source().lineNumber() );
        }
    }

    @Test
    public void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLineSingleCharNewline() throws Exception
    {
        shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine( "\n" );
    }

    @Test
    public void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLinePlatformNewline() throws Exception
    {
        shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine( "%n" );
    }

    @Test
    public void shouldFailOnReadingFieldLargerThanBufferSize() throws Exception
    {
        // GIVEN
        String data = lines( "\n",
                "a,b,c",
                "d,e,f",
                "\"g,h,i",
                "abcdefghijlkmopqrstuvwxyz,l,m" );
        seeker = seeker( data, config( 20, true ) );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "a" );
        assertNextValue( seeker, mark, COMMA, "b" );
        assertNextValue( seeker, mark, COMMA, "c" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "d" );
        assertNextValue( seeker, mark, COMMA, "e" );
        assertNextValue( seeker, mark, COMMA, "f" );
        assertTrue( mark.isEndOfLine() );

        // THEN
        try
        {
            seeker.seek( mark, COMMA );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // Good
            String source = seeker.sourceDescription();
            assertTrue( e.getMessage().contains( "Tried to read" ) );
            assertTrue( e.getMessage().contains( source + ":3" ) );
        }
    }

    private void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine( String newline ) throws Exception
    {
        // GIVEN
        String data = lines( newline,
                "1,\"Bar\"",
                "2,\"Bar",
                "",
                "Quux",
                "\"",
                "3,\"Bar",
                "",
                "Quux\"",
                "" );
        seeker = seeker( data, config( 1_000, true ) );

        // THEN
        assertNextValue( seeker, mark, COMMA, "1" );
        assertNextValue( seeker, mark, COMMA, "Bar" );
        assertNextValue( seeker, mark, COMMA, "2" );
        assertNextValue( seeker, mark, COMMA, lines( newline,
                "Bar",
                "",
                "Quux",
                "" ) );
        assertNextValue( seeker, mark, COMMA, "3" );
        assertNextValue( seeker, mark, COMMA, lines( newline,
                "Bar",
                "",
                "Quux" ) );
    }

    private String lines( String newline, String... lines )
    {
        StringBuilder builder = new StringBuilder();
        for ( String line : lines )
        {
            if ( builder.length() > 0 )
            {
                builder.append( format( newline ) );
            }
            builder.append( line );
        }
        return builder.toString();
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

    private void assertNextValue( CharSeeker seeker, Mark mark, int delimiter, String expectedValue )
            throws IOException
    {
        assertTrue( seeker.seek( mark, delimiter ) );
        assertEquals( expectedValue, seeker.extract( mark, extractors.string() ).value() );
    }

    private void assertNextValueNotExtracted( CharSeeker seeker, Mark mark, int delimiter ) throws IOException
    {
        assertTrue( seeker.seek( mark, delimiter ) );
        assertFalse( seeker.tryExtract( mark, extractors.string() ) );
    }

    private String[] nextLineOfAllStrings( CharSeeker seeker, Mark mark ) throws IOException
    {
        List<String> line = new ArrayList<>();
        while ( seeker.seek( mark, COMMA ) )
        {
            line.add( seeker.extract( mark, extractors.string() ).value() );
            if ( mark.isEndOfLine() )
            {
                break;
            }
        }
        return line.toArray( new String[line.size()] );
    }

    private CharSeeker seeker( CharReadable readable )
    {
        return seeker( readable, config( 1_000 ) );
    }

    private CharSeeker seeker( CharReadable readable, Configuration config )
    {
        return charSeeker( readable, config, useThreadAhead );
    }

    private CharSeeker seeker( String data )
    {
        return seeker( data, config( 1_000 ) );
    }

    private CharSeeker seeker( String data, Configuration config )
    {
        return seeker( wrap( stringReaderWithName( data, TEST_SOURCE ) ), config );
    }

    private Reader stringReaderWithName( String data, final String name )
    {
        return new StringReader( data )
        {
            @Override
            public String toString()
            {
                return name;
            }
        };
    }

    private static Configuration config( final int bufferSize )
    {
        return config( bufferSize, Configuration.DEFAULT.multilineFields() );
    }

    private static Configuration config( final int bufferSize, final boolean multiline )
    {
        return new Configuration.Overridden( Configuration.DEFAULT )
        {
            @Override
            public boolean multilineFields()
            {
                return multiline;
            }

            @Override
            public int bufferSize()
            {
                return bufferSize;
            }
        };
    }

    private static final int TAB = '\t';
    private static final int COMMA = ',';
    private static final Random random = new Random();
    private final Extractors extractors = new Extractors( ',' );
    private final Mark mark = new Mark();

    private CharSeeker seeker;

    @After
    public void closeSeeker() throws IOException
    {
        if ( seeker != null )
        {
            seeker.close();
        }
    }

    private static class ControlledCharReadable extends CharReadable.Adapter
    {
        private final StringReader reader;
        private final int maxBytesPerRead;
        private int position;

        ControlledCharReadable( String data, int maxBytesPerRead )
        {
            this.reader = new StringReader( data );
            this.maxBytesPerRead = maxBytesPerRead;
        }

        @Override
        public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
        {
            buffer.compact( buffer, from );
            buffer.readFrom( reader, maxBytesPerRead );
            return buffer;
        }

        @Override
        public long position()
        {
            return position;
        }

        @Override
        public String sourceDescription()
        {
            return getClass().getSimpleName();
        }
    }
}
