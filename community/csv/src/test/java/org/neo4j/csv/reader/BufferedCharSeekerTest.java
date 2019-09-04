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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.internal.helpers.collection.Iterators.array;

class BufferedCharSeekerTest
{
    private static final char[] WHITESPACE_CHARS = {
            Character.SPACE_SEPARATOR,
            Character.PARAGRAPH_SEPARATOR,
            '\u00A0',
            '\u2007',
            '\u202F',
            '\t',
            '\f',
            '\u001C',
            '\u001D',
            '\u001E',
            '\u001F'
    };

    private static final char[] DELIMITER_CHARS = {
            ',',
            '\t'
    };

    private static final String TEST_SOURCE = "TestSource";
    private static final int TAB = '\t';
    private static final int COMMA = ',';
    private static final Random random = new Random();
    private final Extractors extractors = new Extractors( ',' );
    private final Mark mark = new Mark();

    private CharSeeker seeker;

    @AfterEach
    void closeSeeker() throws IOException
    {
        if ( seeker != null )
        {
            seeker.close();
        }
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldFindCertainCharacter( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "abcdefg\thijklmnop\tqrstuvxyz", threadAhead );

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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldReadMultipleLines( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker(
            "1\t2\t3\n" +
                "4\t5\t6\n", threadAhead );

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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldSeekThroughAdditionalBufferRead( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "1234,5678,9012,3456", config( 12 ), threadAhead );
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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldHandleWindowsEndOfLineCharacters( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker(
            "here,comes,Windows\r\n" +
                "and,it,has\r" +
                "other,line,endings", threadAhead );

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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldHandleReallyWeirdChars( boolean threadAhead ) throws Exception
    {
        // GIVEN
        int cols = 3;
        int rows = 3;
        char delimiter = '\t';
        String[][] data = randomWeirdValues( cols, rows, delimiter, '\n', '\r' );
        seeker = seeker( join( data, delimiter ), threadAhead );

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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldHandleEmptyValues( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "1,,3,4", threadAhead );

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 1, seeker.extract( mark, extractors.int_() ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 3, seeker.extract( mark, extractors.int_() ).intValue() );

        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 4, seeker.extract( mark, extractors.int_() ).intValue() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldNotLetEolCharSkippingMessUpPositionsInMark( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "12,34,56\n789,901,23", config( 9 ), threadAhead );
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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldSeeEofEvenIfBufferAlignsWithEnd( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "123,56", config( 6 ), threadAhead );

        // WHEN
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 123, seeker.extract( mark, extractors.int_() ).intValue() );
        assertTrue( seeker.seek( mark, COMMA ) );
        assertEquals( 56, seeker.extract( mark, extractors.int_() ).intValue() );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldSkipEmptyLastValue( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker(
            "one,two,three,\n" +
                "uno,dos,tres,", threadAhead );

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
        assertEnd( seeker, mark, COMMA );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldExtractEmptyStringForEmptyQuotedString( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "\"\",,\"\"", threadAhead );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "" );
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertNextValue( seeker, mark, COMMA, "" );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldExtractNullForEmptyFieldWhenWeSkipEOLChars( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "\"\",\r\n", threadAhead );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "" );
        assertNextValueNotExtracted( seeker, mark, COMMA );

        // THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldContinueThroughCompletelyEmptyLines( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "one,two,three\n\n\nfour,five,six", threadAhead );

        // WHEN/THEN
        assertArrayEquals( new String[]{"one", "two", "three"}, nextLineOfAllStrings( seeker, mark ) );
        assertArrayEquals( new String[]{"four", "five", "six"}, nextLineOfAllStrings( seeker, mark ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldHandleDoubleCharValues( boolean threadAhead ) throws IOException
    {
        seeker = seeker( "v\uD800\uDC00lue one\t\"v\uD801\uDC01lue two\"\tv\uD804\uDC03lue three", threadAhead );
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "v𐀀lue one", seeker.extract( mark, extractors.string() ).value() );
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "v𐐁lue two", seeker.extract( mark, extractors.string() ).value() );
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "v𑀃lue three", seeker.extract( mark, extractors.string() ).value() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldReadQuotes( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "value one\t\"value two\"\tvalue three", threadAhead );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value two", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldReadQuotedValuesWithDelimiterInside( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "value one\t\"value\ttwo\"\tvalue three", threadAhead );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value\ttwo", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldReadQuotedValuesWithNewLinesInside( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "value one\t\"value\ntwo\"\tvalue three", withMultilineFields( config(), true ), threadAhead );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value one", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value\ntwo", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value three", seeker.extract( mark, extractors.string() ).value() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldHandleDoubleQuotes( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "\"value \"\"one\"\"\"\t\"\"\"value\"\" two\"\t\"va\"\"lue\"\" three\"", threadAhead );

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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldHandleSlashEncodedQuotesIfConfiguredWithLegacyStyleQuoting( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "\"value \\\"one\\\"\"\t\"\\\"value\\\" two\"\t\"va\\\"lue\\\" three\"",
            withLegacyStyleQuoting( config(), true ), threadAhead );

        // WHEN/THEN
        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "value \"one\"", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "\"value\" two", seeker.extract( mark, extractors.string() ).value() );

        assertTrue( seeker.seek( mark, TAB ) );
        assertEquals( "va\"lue\" three", seeker.extract( mark, extractors.string() ).value() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldRecognizeStrayQuoteCharacters( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker(
            "one,two\",th\"ree\n" +
                "four,five,s\"ix", threadAhead );

        // THEN
        assertNextValue( seeker, mark, COMMA, "one" );
        assertNextValue( seeker, mark, COMMA, "two\"" );
        assertNextValue( seeker, mark, COMMA, "th\"ree" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "four" );
        assertNextValue( seeker, mark, COMMA, "five" );
        assertNextValue( seeker, mark, COMMA, "s\"ix" );
        assertEnd( seeker, mark, COMMA );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldNotMisinterpretUnfilledRead( boolean threadAhead ) throws Exception
    {
        // GIVEN
        CharReadable readable = new ControlledCharReadable(
            "123,456,789\n" +
                "abc,def,ghi", 5 );
        seeker = seeker( readable, threadAhead );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "123" );
        assertNextValue( seeker, mark, COMMA, "456" );
        assertNextValue( seeker, mark, COMMA, "789" );
        assertTrue( mark.isEndOfLine() );
        assertNextValue( seeker, mark, COMMA, "abc" );
        assertNextValue( seeker, mark, COMMA, "def" );
        assertNextValue( seeker, mark, COMMA, "ghi" );
        assertEnd( seeker, mark, COMMA );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldNotFindAnyValuesForEmptySource( boolean threadAhead ) throws Exception
    {
        // GIVEN
        seeker = seeker( "", threadAhead );

        // WHEN/THEN
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldSeeQuotesInQuotes( boolean threadAhead ) throws Exception
    {
        // GIVEN
        //                4,     """",   "f\oo"
        seeker = seeker( "4,\"\"\"\",\"f\\oo\"", threadAhead );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "4" );
        assertNextValue( seeker, mark, COMMA, "\"" );
        assertNextValue( seeker, mark, COMMA, "f\\oo" );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldEscapeBackslashesInQuotesIfConfiguredWithLegacyStyleQuoting( boolean threadAhead ) throws Exception
    {
        // GIVEN
        //                4,    "\\\"",   "f\oo"
        seeker = seeker( "4,\"\\\\\\\"\",\"f\\oo\"", withLegacyStyleQuoting( config(), true ), threadAhead );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "4" );
        assertNextValue( seeker, mark, COMMA, "\\\"" );
        assertNextValue( seeker, mark, COMMA, "f\\oo" );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldListenToMusic( boolean threadAhead ) throws Exception
    {
        // GIVEN
        String data =
            "\"1\",\"ABBA\",\"1992\"\n" +
                "\"2\",\"Roxette\",\"1986\"\n" +
                "\"3\",\"Europe\",\"1979\"\n" +
                "\"4\",\"The Cardigans\",\"1992\"";
        seeker = seeker( data, threadAhead );

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
        assertEnd( seeker, mark, COMMA );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldFailOnCharactersAfterEndQuote( boolean threadAhead ) throws Exception
    {
        // GIVEN
        String data = "abc,\"def\"ghi,jkl";
        seeker = seeker( data, threadAhead );

        // WHEN
        assertNextValue( seeker, mark, COMMA, "abc" );
        DataAfterQuoteException quoteException = assertThrows( DataAfterQuoteException.class, () -> seeker.seek( mark, COMMA ) );
        assertEquals( TEST_SOURCE, quoteException.source().sourceDescription() );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLineSingleCharNewline( boolean threadAhead ) throws Exception
    {
        shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine( "\n", threadAhead );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLinePlatformNewline( boolean threadAhead ) throws Exception
    {
        shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine( "%n", threadAhead );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldFailOnReadingFieldLargerThanBufferSize( boolean threadAhead ) throws Exception
    {
        // GIVEN
        String data = lines( "\n",
            "a,b,c",
            "d,e,f",
            "\"g,h,i",
            "abcdefghijlkmopqrstuvwxyz,l,m" );
        seeker = seeker( data, withMultilineFields( config( 20 ), true ), threadAhead );

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
        IllegalStateException stateException = assertThrows( IllegalStateException.class, () -> seeker.seek( mark, COMMA ) );
        String source = seeker.sourceDescription();
        assertTrue( stateException.getMessage().contains( "Tried to read" ) );
        assertTrue( stateException.getMessage().contains( source + ":3" ) );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldNotInterpretBackslashQuoteDifferentlyIfDisabledLegacyStyleQuoting( boolean threadAhead ) throws Exception
    {
        // GIVEN data with the quote character ' for easier readability
        char slash = '\\';
        String data = lines( "\n", "'abc''def" + slash + "''ghi'" );
        seeker = seeker( data, withLegacyStyleQuoting( withQuoteCharacter( config(), '\'' ), false ), threadAhead );

        // WHEN/THEN
        assertNextValue( seeker, mark, COMMA, "abc'def" + slash + "'ghi" );
        assertFalse( seeker.seek( mark, COMMA ) );
    }

    private void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine( String newline, boolean threadAhead ) throws Exception
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
        seeker = seeker( data, withMultilineFields( config(), true ), threadAhead );

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

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldTrimWhitespace( boolean threadAhead ) throws Exception
    {
        // given
        String data = lines( "\n",
            "Foo, Bar,  Twobar , \"Baz\" , \" Quux \",\"Wiii \" , Waaaa  " );

        // when
        seeker = seeker( data, withTrimStrings( config(), true ), threadAhead );

        // then
        assertNextValue( seeker, mark, COMMA, "Foo" );
        assertNextValue( seeker, mark, COMMA, "Bar" );
        assertNextValue( seeker, mark, COMMA, "Twobar" );
        assertNextValue( seeker, mark, COMMA, "Baz" );
        assertNextValue( seeker, mark, COMMA, " Quux " );
        assertNextValue( seeker, mark, COMMA, "Wiii " );
        assertNextValue( seeker, mark, COMMA, "Waaaa" );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldTrimStringsWithFirstLineCharacterSpace( boolean threadAhead ) throws IOException
    {
        // given
        String line = " ,a, ,b, ";
        seeker = seeker( line, withTrimStrings( config(), true ), threadAhead );

        // when/then
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertNextValue( seeker, mark, COMMA, "a" );
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertNextValue( seeker, mark, COMMA, "b" );
        assertNextValueNotExtracted( seeker, mark, COMMA );
        assertEnd( seeker, mark, COMMA );
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldParseAndTrimRandomStrings( boolean threadAhead ) throws IOException
    {
        // given
        StringBuilder builder = new StringBuilder();
        int columns = random.nextInt( 10 ) + 5;
        int lines = 100;
        List<String> expected = new ArrayList<>();
        char delimiter = randomDelimiter();
        for ( int i = 0; i < lines; i++ )
        {
            for ( int j = 0; j < columns; j++ )
            {
                if ( j > 0 )
                {
                    if ( random.nextBoolean() )
                    {
                        // Space before delimiter
                        builder.append( randomWhitespace( delimiter ) );
                    }
                    builder.append( delimiter );
                    if ( random.nextBoolean() )
                    {
                        // Space before delimiter
                        builder.append( randomWhitespace( delimiter ) );
                    }
                }
                boolean quote = random.nextBoolean();
                if ( random.nextBoolean() )
                {
                    String value = "";
                    if ( quote )
                    {
                        // Quote
                        if ( random.nextBoolean() )
                        {
                            // Space after quote start
                            value += randomWhitespace( delimiter );
                        }
                    }
                    // Actual value
                    value += String.valueOf( random.nextInt() );
                    if ( quote )
                    {
                        if ( random.nextBoolean() )
                        {
                            // Space before quote end
                            value += randomWhitespace( delimiter );
                        }
                    }
                    expected.add( value );
                    builder.append( quote ? "\"" + value + "\"" : value );
                }
                else
                {
                    expected.add( null );
                }
            }
            builder.append( format( "%n" ) );
        }
        String data = builder.toString();
        seeker = seeker( data, withTrimStrings( config(), true ), threadAhead );

        // when
        Iterator<String> next = expected.iterator();
        for ( int i = 0; i < lines; i++ )
        {
            for ( int j = 0; j < columns; j++ )
            {
                // then
                String nextExpected = next.next();
                if ( nextExpected == null )
                {
                    assertNextValueNotExtracted( seeker, mark, delimiter );
                }
                else
                {
                    assertNextValue( seeker, mark, delimiter, nextExpected );
                }
            }
        }
        assertEnd( seeker, mark, delimiter );
    }

    private char randomDelimiter()
    {
        return DELIMITER_CHARS[random.nextInt( DELIMITER_CHARS.length )];
    }

    private char randomWhitespace( char except )
    {
        char ch;
        do
        {
            ch = WHITESPACE_CHARS[random.nextInt( WHITESPACE_CHARS.length )];
        }
        while ( ch == except );
        return ch;
    }

    @ParameterizedTest( name = "thread-ahead: {0}" )
    @ValueSource( booleans = {false, true} )
    void shouldParseNonLatinCharacters( boolean threadAhead ) throws IOException
    {
        // given
        List<String[]> expected = asList(
                array( "普通�?/普通話", "\uD83D\uDE21" ),
                array( "\uD83D\uDE21\uD83D\uDCA9\uD83D\uDC7B", "ⲹ楡�?톜ഷۢ⼈�?�늉�?�₭샺ጚ砧攡跿家䯶�?⬖�?�犽ۼ" ),
                array( " 㺂�?鋦毠", ";먵�?裬岰鷲趫\uA8C5얱㓙髿ᚳᬼ≩�?� " )
        );
        String data = lines( format( "%n" ), expected );

        // when
        seeker = seeker( data, threadAhead );

        // then
        for ( String[] line : expected )
        {
            for ( String cell : line )
            {
                assertNextValue( seeker, mark, COMMA, cell );
            }
        }
        assertEnd( seeker, mark, COMMA );
    }

    private String lines( String newline, List<String[]> cells )
    {
        String[] lines = new String[cells.size()];
        int i = 0;
        for ( String[] columns : cells )
        {
            lines[i++] = StringUtils.join( columns, "," );
        }
        return lines( newline, lines );
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
        int length = random.nextInt( 10 ) + 5;
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

    private void assertEnd( CharSeeker seeker, Mark mark, int delimiter ) throws IOException
    {
        assertTrue( mark.isEndOfLine() );
        assertFalse( seeker.seek( mark, delimiter ) );
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
        return line.toArray( new String[0] );
    }

    private CharSeeker seeker( CharReadable readable, boolean threadAhead )
    {
        return seeker( readable, config(), threadAhead );
    }

    private CharSeeker seeker( CharReadable readable, Configuration config, boolean threadAhead )
    {
        return charSeeker( readable, config, threadAhead );
    }

    private CharSeeker seeker( String data, boolean threadAhead )
    {
        return seeker( data, config(), threadAhead );
    }

    private CharSeeker seeker( String data, Configuration config, boolean threadAhead )
    {
        return seeker( wrap( stringReaderWithName( data, TEST_SOURCE ), data.length() * 2 ), config, threadAhead );
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

    private static Configuration config()
    {
        return config( 1_000 );
    }

    private static Configuration config( final int bufferSize )
    {
        return Configuration.newBuilder().withBufferSize( bufferSize ).build();
    }

    private static Configuration withMultilineFields( Configuration config, boolean multiline )
    {
        return config.toBuilder().withMultilineFields( multiline ).build();
    }

    private static Configuration withLegacyStyleQuoting( Configuration config, boolean legacyStyleQuoting )
    {
        return config.toBuilder().withLegacyStyleQuoting( legacyStyleQuoting ).build();
    }

    private static Configuration withQuoteCharacter( Configuration config, char quoteCharacter )
    {
        return config.toBuilder().withQuotationCharacter( quoteCharacter ).build();
    }

    private static Configuration withTrimStrings( Configuration config, boolean trimStrings )
    {
        return config.toBuilder().withTrimStrings( trimStrings ).build();
    }

    private static class ControlledCharReadable extends CharReadable.Adapter
    {
        private final StringReader reader;
        private final int maxBytesPerRead;
        private final String data;

        ControlledCharReadable( String data, int maxBytesPerRead )
        {
            this.data = data;
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
        public int read( char[] into, int offset, int length )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long position()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return getClass().getSimpleName();
        }

        @Override
        public long length()
        {
            return data.length() * 2;
        }
    }
}
