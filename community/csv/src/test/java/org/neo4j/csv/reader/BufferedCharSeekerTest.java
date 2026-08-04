/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.csv.reader;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowableOfType;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.internal.helpers.collection.Iterators.array;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.common.EntityType;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
class BufferedCharSeekerTest {
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

    private static final char[] DELIMITER_CHARS = {',', '\t'};
    private static final String TEST_SOURCE = "TestSource";
    private static final int TAB = '\t';
    private static final int COMMA = ',';

    @Inject
    private RandomSupport random;

    private final Extractors extractors = new Extractors(',', ',');
    private final Mark mark = new Mark();

    private CharSeeker seeker;

    @AfterEach
    void closeSeeker() throws IOException {
        if (seeker != null) {
            seeker.close();
        }
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldFindCertainCharacter(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("abcdefg\thijklmnop\tqrstuvxyz", threadAhead);

        // WHEN/THEN
        // first value
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(mark.character()).isEqualTo('\t');
        assertThat(mark.isEndOfLine()).isFalse();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("abcdefg");

        // second value
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(mark.character()).isEqualTo('\t');
        assertThat(mark.isEndOfLine()).isFalse();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("hijklmnop");

        // third value
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(mark.isEndOfLine()).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("qrstuvxyz");

        // no more values
        assertThat(seeker.seek(mark, TAB)).isFalse();
        assertThat(seeker.seek(mark, TAB)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldReadMultipleLines(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("""
                1\t2\t3
                4\t5\t6
                """, threadAhead);

        // WHEN/THEN
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isOne();

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(2L);

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(3L);
        assertThat(mark.isEndOfLine()).isTrue();

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(4L);

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(5L);

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(6L);

        assertThat(mark.isEndOfLine()).isTrue();
        assertThat(seeker.seek(mark, TAB)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldSeekThroughAdditionalBufferRead(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("1234,5678,9012,3456", config(12), threadAhead);
        // read more here             ^

        // WHEN/THEN
        seeker.seek(mark, COMMA);
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(1234L);
        seeker.seek(mark, COMMA);
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(5678L);
        seeker.seek(mark, COMMA);
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(9012L);
        seeker.seek(mark, COMMA);
        assertThat(seeker.extract(mark, extractors.long_()).longValue()).isEqualTo(3456L);
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldHandleWindowsEndOfLineCharacters(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("here,comes,Windows\r\n" + "and,it,has\r" + "other,line,endings", threadAhead);

        // WHEN/THEN - \r\n, \r, and implicit EOF all terminate a line
        assertNextValue(seeker, mark, COMMA, "here");
        assertNextValue(seeker, mark, COMMA, "comes");
        assertNextValue(seeker, mark, COMMA, "Windows");
        assertThat(mark.isEndOfLine()).isTrue();

        assertNextValue(seeker, mark, COMMA, "and");
        assertNextValue(seeker, mark, COMMA, "it");
        assertNextValue(seeker, mark, COMMA, "has");
        assertThat(mark.isEndOfLine()).isTrue();

        assertNextValue(seeker, mark, COMMA, "other");
        assertNextValue(seeker, mark, COMMA, "line");
        assertNextValue(seeker, mark, COMMA, "endings");
        assertThat(mark.isEndOfLine()).isTrue();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldHandleReallyWeirdChars(boolean threadAhead) throws Exception {
        // GIVEN
        int cols = 3;
        int rows = 3;
        char delimiter = '\t';
        String[][] data = randomWeirdValues(cols, rows, delimiter, '\n', '\r', '"');
        seeker = seeker(join(data, delimiter), withQuoteCharacter(config(), '"'), threadAhead);

        // WHEN/THEN
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                assertThat(seeker.seek(mark, TAB)).isTrue();
                assertThat(seeker.extract(mark, extractors.string())).isEqualTo(data[row][col]);
            }
            assertThat(mark.isEndOfLine()).isTrue();
        }
        assertThat(seeker.seek(mark, TAB)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldHandleEmptyValues(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("1,,3,4", threadAhead);

        // WHEN
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isOne();

        assertThat(seeker.seek(mark, COMMA)).isTrue();

        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(3);

        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(4);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldNotLetEolCharSkippingMessUpPositionsInMark(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("12,34,56\n789,901,23", config(9), threadAhead);
        // read more here          ^        ^

        // WHEN
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(12);
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(34);
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(56);

        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(789);
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(901);
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(23);

        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldSeeEofEvenIfBufferAlignsWithEnd(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("123,56", config(6), threadAhead);

        // WHEN
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(123);
        assertThat(seeker.seek(mark, COMMA)).isTrue();
        assertThat(seeker.extract(mark, extractors.int_()).intValue()).isEqualTo(56);

        // THEN
        assertThat(seeker.seek(mark, COMMA)).isFalse();
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldSkipEmptyLastValue(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("one,two,three,\n" + "uno,dos,tres,", threadAhead);

        // WHEN
        assertNextValue(seeker, mark, COMMA, "one");
        assertNextValue(seeker, mark, COMMA, "two");
        assertNextValue(seeker, mark, COMMA, "three");
        assertNextValueNotExtracted(seeker, mark, COMMA);
        assertThat(mark.isEndOfLine()).isTrue();

        assertNextValue(seeker, mark, COMMA, "uno");
        assertNextValue(seeker, mark, COMMA, "dos");
        assertNextValue(seeker, mark, COMMA, "tres");
        assertNextValueNotExtracted(seeker, mark, COMMA);
        assertEnd(seeker, mark, COMMA);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldExtractEmptyStringForEmptyQuotedString(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("\"\",,\"\"", threadAhead);

        // WHEN
        assertNextValue(seeker, mark, COMMA, "");
        assertNextValueNotExtracted(seeker, mark, COMMA);
        assertNextValue(seeker, mark, COMMA, "");

        // THEN
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldExtractNullForEmptyFieldWhenWeSkipEOLChars(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("\"\",\r\n", threadAhead);

        // WHEN
        assertNextValue(seeker, mark, COMMA, "");
        assertNextValueNotExtracted(seeker, mark, COMMA);

        // THEN
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldContinueThroughCompletelyEmptyLines(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("one,two,three\n\n\nfour,five,six", threadAhead);

        // WHEN/THEN
        assertThat(nextLineOfAllStrings(seeker, mark)).containsExactly(new String[] {"one", "two", "three"});
        assertThat(nextLineOfAllStrings(seeker, mark)).containsExactly(new String[] {"four", "five", "six"});
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldHandleDoubleCharValues(boolean threadAhead) throws Exception {
        seeker = seeker("v\uD800\uDC00lue one\t\"v\uD801\uDC01lue two\"\tv\uD804\uDC03lue three", threadAhead);
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("v𐀀lue one");
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("v𐐁lue two");
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("v𑀃lue three");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldReadQuotes(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("value one\t\"value two\"\tvalue three", threadAhead);

        // WHEN/THEN
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value one");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value two");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value three");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldReadQuotedValuesWithDelimiterInside(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("value one\t\"value\ttwo\"\tvalue three", threadAhead);

        // WHEN/THEN
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value one");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value\ttwo");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value three");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldReadQuotedValuesWithNewLinesInside(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("value one\t\"value\ntwo\"\tvalue three", withMultilineFields(config()), threadAhead);

        // WHEN/THEN
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value one");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value\ntwo");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value three");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldHandleDoubleQuotes(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("\"value \"\"one\"\"\"\t\"\"\"value\"\" two\"\t\"va\"\"lue\"\" three\"", threadAhead);

        // "value ""one"""
        // """value"" two"
        // "va""lue"" three"

        // WHEN/THEN
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value \"one\"");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("\"value\" two");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("va\"lue\" three");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldHandleSlashEncodedQuotesIfConfiguredWithLegacyStyleQuoting(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker(
                "\"value \\\"one\\\"\"\t\"\\\"value\\\" two\"\t\"va\\\"lue\\\" three\"",
                withLegacyStyleQuoting(config(), true),
                threadAhead);

        // WHEN/THEN
        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("value \"one\"");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("\"value\" two");

        assertThat(seeker.seek(mark, TAB)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo("va\"lue\" three");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldRecognizeStrayQuoteCharacters(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("one,two\",th\"ree\n" + "four,five,s\"ix", threadAhead);

        // THEN
        assertNextValue(seeker, mark, COMMA, "one");
        assertNextValue(seeker, mark, COMMA, "two\"");
        assertNextValue(seeker, mark, COMMA, "th\"ree");
        assertThat(mark.isEndOfLine()).isTrue();
        assertNextValue(seeker, mark, COMMA, "four");
        assertNextValue(seeker, mark, COMMA, "five");
        assertNextValue(seeker, mark, COMMA, "s\"ix");
        assertEnd(seeker, mark, COMMA);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldNotMisinterpretUnfilledRead(boolean threadAhead) throws Exception {
        // GIVEN
        CharReadable readable = new ControlledCharReadable("123,456,789\n" + "abc,def,ghi", 5);
        seeker = seeker(readable, threadAhead);

        // WHEN/THEN
        assertNextValue(seeker, mark, COMMA, "123");
        assertNextValue(seeker, mark, COMMA, "456");
        assertNextValue(seeker, mark, COMMA, "789");
        assertThat(mark.isEndOfLine()).isTrue();
        assertNextValue(seeker, mark, COMMA, "abc");
        assertNextValue(seeker, mark, COMMA, "def");
        assertNextValue(seeker, mark, COMMA, "ghi");
        assertEnd(seeker, mark, COMMA);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldNotFindAnyValuesForEmptySource(boolean threadAhead) throws Exception {
        // GIVEN
        seeker = seeker("", threadAhead);

        // WHEN/THEN
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldSeeQuotesInQuotes(boolean threadAhead) throws Exception {
        // GIVEN
        //                4,     """",   "f\oo"
        seeker = seeker("4,\"\"\"\",\"f\\oo\"", threadAhead);

        // WHEN/THEN
        assertNextValue(seeker, mark, COMMA, "4");
        assertNextValue(seeker, mark, COMMA, "\"");
        assertNextValue(seeker, mark, COMMA, "f\\oo");
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldEscapeBackslashesInQuotesIfConfiguredWithLegacyStyleQuoting(boolean threadAhead) throws Exception {
        // GIVEN
        //                4,    "\\\"",   "f\oo"
        seeker = seeker("4,\"\\\\\\\"\",\"f\\oo\"", withLegacyStyleQuoting(config(), true), threadAhead);

        // WHEN/THEN
        assertNextValue(seeker, mark, COMMA, "4");
        assertNextValue(seeker, mark, COMMA, "\\\"");
        assertNextValue(seeker, mark, COMMA, "f\\oo");
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldListenToMusic(boolean threadAhead) throws Exception {
        // GIVEN
        String data = """
                "1","ABBA","1992"
                "2","Roxette","1986"
                "3","Europe","1979"
                "4","The Cardigans","1992\"""";
        seeker = seeker(data, threadAhead);

        // WHEN
        assertNextValue(seeker, mark, COMMA, "1");
        assertNextValue(seeker, mark, COMMA, "ABBA");
        assertNextValue(seeker, mark, COMMA, "1992");
        assertThat(mark.isEndOfLine()).isTrue();
        assertNextValue(seeker, mark, COMMA, "2");
        assertNextValue(seeker, mark, COMMA, "Roxette");
        assertNextValue(seeker, mark, COMMA, "1986");
        assertThat(mark.isEndOfLine()).isTrue();
        assertNextValue(seeker, mark, COMMA, "3");
        assertNextValue(seeker, mark, COMMA, "Europe");
        assertNextValue(seeker, mark, COMMA, "1979");
        assertThat(mark.isEndOfLine()).isTrue();
        assertNextValue(seeker, mark, COMMA, "4");
        assertNextValue(seeker, mark, COMMA, "The Cardigans");
        assertNextValue(seeker, mark, COMMA, "1992");
        assertEnd(seeker, mark, COMMA);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldFailOnCharactersAfterEndQuote(boolean threadAhead) throws Exception {
        // GIVEN
        String data = "abc,\"def\"ghi,jkl";
        seeker = seeker(data, threadAhead);

        // WHEN
        assertNextValue(seeker, mark, COMMA, "abc");
        assertThat(catchThrowableOfType(DataAfterQuoteException.class, () -> seeker.seek(mark, COMMA)))
                .extracting(e -> e.source().sourceDescription())
                .isEqualTo(TEST_SOURCE);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldFailOnIllegalMultilineException(boolean threadAhead) throws Exception {
        String data = "\"abc\",\"j\nkl\"";
        // We need to disable multiline rows to trigger this exception
        seeker = seeker(data, Configuration.newBuilder().build(), threadAhead);

        assertNextValue(seeker, mark, COMMA, "abc");
        assertThat(catchThrowableOfType(IllegalMultilineFieldException.class, () -> seeker.seek(mark, COMMA)))
                .extracting(e -> e.source().sourceDescription())
                .isEqualTo(TEST_SOURCE);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldFailOnMissingEndQouteException(boolean threadAhead) throws Exception {
        String data = "\"abc\",\"jkl";
        seeker = seeker(data, threadAhead);

        assertNextValue(seeker, mark, COMMA, "abc");
        assertThat(catchThrowableOfType(MissingEndQuoteException.class, () -> seeker.seek(mark, COMMA)))
                .extracting(e -> e.source().sourceDescription())
                .isEqualTo(TEST_SOURCE);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLineSingleCharNewline(boolean threadAhead) throws Exception {
        shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine("\n", threadAhead);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLinePlatformNewline(boolean threadAhead) throws Exception {
        shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine(System.lineSeparator(), threadAhead);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldFailOnReadingFieldLargerThanBufferSize(boolean threadAhead) throws Exception {
        // GIVEN
        String data = lines("\n", "a,b,c", "d,e,f", "\"g,h,i", "abcdefghijlkmopqrstuvwxyz,l,m");
        seeker = seeker(data, withMultilineFields(config(20)), threadAhead);

        // WHEN
        assertNextValue(seeker, mark, COMMA, "a");
        assertNextValue(seeker, mark, COMMA, "b");
        assertNextValue(seeker, mark, COMMA, "c");
        assertThat(mark.isEndOfLine()).isTrue();
        assertNextValue(seeker, mark, COMMA, "d");
        assertNextValue(seeker, mark, COMMA, "e");
        assertNextValue(seeker, mark, COMMA, "f");
        assertThat(mark.isEndOfLine()).isTrue();

        // THEN
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> seeker.seek(mark, COMMA))
                .withMessageContaining("Tried to read")
                .withMessageContaining(seeker.sourceDescription() + ":4");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldNotInterpretBackslashQuoteDifferentlyIfDisabledLegacyStyleQuoting(boolean threadAhead) throws Exception {
        // GIVEN data with the quote character ' for easier readability
        char slash = '\\';
        String data = lines("\n", "'abc''def" + slash + "''ghi'");
        seeker = seeker(data, withLegacyStyleQuoting(withQuoteCharacter(config(), '\''), false), threadAhead);

        // WHEN/THEN
        assertNextValue(seeker, mark, COMMA, "abc'def" + slash + "'ghi");
        assertThat(seeker.seek(mark, COMMA)).isFalse();
    }

    private void shouldParseMultilineFieldWhereEndQuoteIsOnItsOwnLine(String newline, boolean threadAhead)
            throws Exception {
        // GIVEN
        String data = lines(newline, "1,\"Bar\"", "2,\"Bar", "", "Quux", "\"", "3,\"Bar", "", "Quux\"", "");
        seeker = seeker(data, withMultilineFields(config()), threadAhead);

        // THEN
        assertNextValue(seeker, mark, COMMA, "1");
        assertNextValue(seeker, mark, COMMA, "Bar");
        assertNextValue(seeker, mark, COMMA, "2");
        assertNextValue(seeker, mark, COMMA, lines(newline, "Bar", "", "Quux", ""));
        assertNextValue(seeker, mark, COMMA, "3");
        assertNextValue(seeker, mark, COMMA, lines(newline, "Bar", "", "Quux"));
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldTrimWhitespace(boolean threadAhead) throws Exception {
        // given
        String data = lines("\n", "Foo, Bar,  Twobar , \"Baz\" , \" Quux \",\"Wiii \" , Waaaa  ");

        // when
        seeker = seeker(data, withTrimStrings(config()), threadAhead);

        // then
        assertNextValue(seeker, mark, COMMA, "Foo");
        assertNextValue(seeker, mark, COMMA, "Bar");
        assertNextValue(seeker, mark, COMMA, "Twobar");
        assertNextValue(seeker, mark, COMMA, "Baz");
        assertNextValue(seeker, mark, COMMA, " Quux ");
        assertNextValue(seeker, mark, COMMA, "Wiii ");
        assertNextValue(seeker, mark, COMMA, "Waaaa");
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldTrimStringsWithFirstLineCharacterSpace(boolean threadAhead) throws Exception {
        // given
        String line = " ,a, ,b, ";
        seeker = seeker(line, withTrimStrings(config()), threadAhead);

        // when/then
        assertNextValueNotExtracted(seeker, mark, COMMA);
        assertNextValue(seeker, mark, COMMA, "a");
        assertNextValueNotExtracted(seeker, mark, COMMA);
        assertNextValue(seeker, mark, COMMA, "b");
        assertNextValueNotExtracted(seeker, mark, COMMA);
        assertEnd(seeker, mark, COMMA);
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldParseAndTrimRandomStrings(boolean threadAhead) throws Exception {
        // given
        StringBuilder builder = new StringBuilder();
        int columns = random.nextInt(10) + 5;
        int lines = 100;
        List<String> expected = new ArrayList<>();
        char delimiter = randomDelimiter();
        for (int i = 0; i < lines; i++) {
            for (int j = 0; j < columns; j++) {
                if (j > 0) {
                    if (random.nextBoolean()) {
                        // Space before delimiter
                        builder.append(randomWhitespace(delimiter));
                    }
                    builder.append(delimiter);
                    if (random.nextBoolean()) {
                        // Space before delimiter
                        builder.append(randomWhitespace(delimiter));
                    }
                }
                boolean quote = random.nextBoolean();
                if (random.nextBoolean()) {
                    String value = "";
                    if (quote) {
                        // Quote
                        if (random.nextBoolean()) {
                            // Space after quote start
                            value += randomWhitespace(delimiter);
                        }
                    }
                    // Actual value
                    value += String.valueOf(random.nextInt());
                    if (quote) {
                        if (random.nextBoolean()) {
                            // Space before quote end
                            value += randomWhitespace(delimiter);
                        }
                    }
                    expected.add(value);
                    builder.append(quote ? "\"" + value + "\"" : value);
                } else {
                    expected.add(null);
                }
            }
            builder.append(format("%n"));
        }
        String data = builder.toString();
        seeker = seeker(data, withTrimStrings(config()), threadAhead);

        // when
        Iterator<String> next = expected.iterator();
        for (int i = 0; i < lines; i++) {
            for (int j = 0; j < columns; j++) {
                // then
                String nextExpected = next.next();
                if (nextExpected == null) {
                    assertNextValueNotExtracted(seeker, mark, delimiter);
                } else {
                    assertNextValue(seeker, mark, delimiter, nextExpected);
                }
            }
        }
        assertEnd(seeker, mark, delimiter);
    }

    private char randomDelimiter() {
        return DELIMITER_CHARS[random.nextInt(DELIMITER_CHARS.length)];
    }

    private char randomWhitespace(char except) {
        char ch;
        do {
            ch = WHITESPACE_CHARS[random.nextInt(WHITESPACE_CHARS.length)];
        } while (ch == except);
        return ch;
    }

    @ParameterizedTest(name = "thread-ahead: {0}")
    @ValueSource(booleans = {false, true})
    void shouldParseNonLatinCharacters(boolean threadAhead) throws Exception {
        // given
        List<String[]> expected = asList(
                array("普通�?/普通話", "\uD83D\uDE21"),
                array("\uD83D\uDE21\uD83D\uDCA9\uD83D\uDC7B", "ⲹ楡�?톜ഷۢ⼈�?�늉�?�₭샺ጚ砧攡跿家䯶�?⬖�?�犽ۼ"),
                array(" 㺂�?鋦毠", ";먵�?裬岰鷲趫\uA8C5얱㓙髿ᚳᬼ≩�?� "));
        String data = lines(format("%n"), expected);

        // when
        seeker = seeker(data, threadAhead);

        // then
        for (String[] line : expected) {
            for (String cell : line) {
                assertNextValue(seeker, mark, COMMA, cell);
            }
        }
        assertEnd(seeker, mark, COMMA);
    }

    private static String lines(String newline, List<String[]> cells) {
        String[] lines = new String[cells.size()];
        int i = 0;
        for (String[] columns : cells) {
            lines[i++] = StringUtils.join(columns, ",");
        }
        return lines(newline, lines);
    }

    private static String lines(String newline, String... lines) {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            if (!builder.isEmpty()) {
                builder.append(newline);
            }
            builder.append(line);
        }
        return builder.toString();
    }

    private String[][] randomWeirdValues(int cols, int rows, char... except) {
        String[][] data = new String[rows][cols];
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                data[row][col] = randomWeirdValue(except);
            }
        }
        return data;
    }

    private String randomWeirdValue(char... except) {
        int length = random.nextInt(10) + 5;
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = randomWeirdChar(except);
        }
        return new String(chars);
    }

    private char randomWeirdChar(char... except) {
        while (true) {
            char candidate = (char) random.nextInt(Character.MAX_VALUE);
            if (!in(candidate, except)) {
                return candidate;
            }
        }
    }

    private static boolean in(char candidate, char[] set) {
        for (char ch : set) {
            if (ch == candidate) {
                return true;
            }
        }
        return false;
    }

    private static String join(String[][] data, char delimiter) {
        String delimiterString = String.valueOf(delimiter);
        StringBuilder builder = new StringBuilder();
        for (String[] line : data) {
            for (int i = 0; i < line.length; i++) {
                builder.append(i > 0 ? delimiterString : "").append(line[i]);
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    private void assertNextValue(CharSeeker seeker, Mark mark, int delimiter, String expectedValue) throws IOException {
        assertThat(seeker.seek(mark, delimiter)).isTrue();
        assertThat(seeker.extract(mark, extractors.string())).isEqualTo(expectedValue);
    }

    private void assertNextValueNotExtracted(CharSeeker seeker, Mark mark, int delimiter) throws IOException {
        assertThat(seeker.seek(mark, delimiter)).isTrue();
        var extractor = extractors.string();
        assertThat(extractor.isEmpty(seeker.tryExtract(mark, extractor))).isTrue();
    }

    private static void assertEnd(CharSeeker seeker, Mark mark, int delimiter) throws IOException {
        assertThat(mark.isEndOfLine()).isTrue();
        assertThat(seeker.seek(mark, delimiter)).isFalse();
    }

    private String[] nextLineOfAllStrings(CharSeeker seeker, Mark mark) throws IOException {
        List<String> line = new ArrayList<>();
        while (seeker.seek(mark, COMMA)) {
            line.add(seeker.extract(mark, extractors.string()));
            if (mark.isEndOfLine()) {
                break;
            }
        }
        return line.toArray(new String[0]);
    }

    private static CharSeeker seeker(CharReadable readable, boolean threadAhead) {
        return seeker(readable, config(), threadAhead);
    }

    private static CharSeeker seeker(CharReadable readable, Configuration config, boolean threadAhead) {
        return charSeeker(readable, config, threadAhead, EntityType.NODE);
    }

    private static CharSeeker seeker(String data, boolean threadAhead) {
        return seeker(data, config(), threadAhead);
    }

    private static CharSeeker seeker(String data, Configuration config, boolean threadAhead) {
        return seeker(wrap(stringReaderWithName(data), data.length() * 2L, null), config, threadAhead);
    }

    private static Reader stringReaderWithName(String data) {
        return new StringReader(data) {
            @Override
            public String toString() {
                return TEST_SOURCE;
            }
        };
    }

    private static Configuration config() {
        return config(1_000);
    }

    private static Configuration config(final int bufferSize) {
        return Configuration.newBuilder().withBufferSize(bufferSize).build();
    }

    private static Configuration withMultilineFields(Configuration config) {
        return config.toBuilder()
                .withLegacyMultilineBehaviour(EntityType.NODE)
                .withLegacyMultilineBehaviour(EntityType.RELATIONSHIP)
                .build();
    }

    private static Configuration withLegacyStyleQuoting(Configuration config, boolean legacyStyleQuoting) {
        return config.toBuilder().withLegacyStyleQuoting(legacyStyleQuoting).build();
    }

    private static Configuration withQuoteCharacter(Configuration config, char quoteCharacter) {
        return config.toBuilder().withQuotationCharacter(quoteCharacter).build();
    }

    private static Configuration withTrimStrings(Configuration config) {
        return config.toBuilder().withTrimStrings(true).build();
    }

    private static class ControlledCharReadable extends CharReadable.Adapter {
        private final StringReader reader;
        private final int maxBytesPerRead;
        private final String data;

        ControlledCharReadable(String data, int maxBytesPerRead) {
            this.data = data;
            this.reader = new StringReader(data);
            this.maxBytesPerRead = maxBytesPerRead;
        }

        @Override
        public SectionedCharBuffer read(SectionedCharBuffer buffer, int from) throws IOException {
            buffer.compact(buffer, from);
            buffer.readFrom(reader, maxBytesPerRead);
            return buffer;
        }

        @Override
        public int read(char[] into, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String sourceDescription() {
            return getClass().getSimpleName();
        }

        @Override
        public long length() {
            return data.length() * 2L;
        }
    }
}
