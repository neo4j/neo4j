/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.misc.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.cypher.internal.parser.lexer.CypherToken;
import org.neo4j.cypher.internal.parser.lexer.UnicodeEscapeReplacementReader.InvalidUnicodeLiteral;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith({RandomExtension.class})
public class CypherAstLexerTest {
    @Inject
    private RandomSupport rand;

    @Test
    void emptyString() throws IOException {
        assertReasonableOffsets("", new int[0]);
    }

    @Test
    void reasonablePositionsWithAllEscapes() throws IOException {
        final var codepoints =
                IntStream.generate(weightedCodepoint).limit(rand.nextInt(10000)).toArray();
        final var escaped = Arrays.stream(codepoints)
                .flatMap(c ->
                        Character.toString(c).chars().flatMap(cc -> escape(cc).codePoints()))
                .toArray();

        assertReasonableOffsets(new String(escaped, 0, escaped.length), codepoints);
    }

    @Test
    void reasonablePositionsWithArbitraryString() throws IOException {
        final var codepoints = IntStream.generate(weightedCodepoint)
                .filter(c -> c != '\\' && c != 'u') // Ugly way to avoid unicode escape sequences
                .limit(rand.nextInt(10000))
                .toArray();

        // Escape some codepoints
        final var codepointsWithEscapes = Arrays.stream(codepoints)
                .flatMap(cp -> rand.nextInt(8) == 0
                        ? Character.toString(cp).chars().flatMap(c -> escape(c).codePoints())
                        : IntStream.of(cp))
                .toArray();

        final var in = new String(codepointsWithEscapes, 0, codepointsWithEscapes.length);
        assertReasonableOffsets(in, codepoints);
    }

    @Test
    void failureOnInvalidUnicodeEscape() {
        final var in = "\uD80C\uDC00\nᚠ\rhej\r\nhola\\uohno";
        final var lines = in.lines().toList();
        final var expectedOffset = in.indexOf("\\uohno");
        final var expectedLine = lines.size();
        final var expectedCol = lines.get(lines.size() - 1).indexOf("ohno") + 1;

        assertThatThrownBy(() -> CypherAstLexer.fromString(in, rand.nextInt(in.length()) + 2, rand.nextBoolean()))
                .isInstanceOf(InvalidUnicodeLiteral.class)
                .hasMessage("Invalid input 'ohno': expected four hexadecimal digits specifying a unicode character")
                .extracting("offset", "column", "line")
                .containsExactly(expectedOffset, expectedCol, expectedLine);
    }

    // Tests copied from javacc CypherCharStreamTest

    @Test
    void basicHappyPath() throws IOException {
        final var in = "abc d  \nö\r\n\t";
        assertReasonableOffsets(in, in.codePoints().toArray());
    }

    @Test
    void doNodeConvertBackslash() throws IOException {
        final var in = "\\\\ \\a \\\\";
        assertReasonableOffsets(in, in.codePoints().toArray());
    }

    @Test
    void convertEscapedBackslashUnicodeMix() throws IOException {
        final var in = "\\\\u16bc \\\\\\u16bc \\\\\\\\u16bc \\\\\\\\\\u16bc";
        final var expected =
                "\\\\u16bc \\\\ᚼ \\\\\\\\u16bc \\\\\\\\ᚼ".codePoints().toArray();
        assertReasonableOffsets(in, expected);
    }

    @Test
    void beginPosition() throws IOException {
        String Q1 = "ab \nö\r\n\tpk\\uD83D x";
        String Q1_unescaped = "ab \nö\r\n\tpk\uD83D x";
        int[] Q1_offset = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 17};
        int[] Q1_line = {1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3};
        int[] Q1_column = {1, 2, 3, 4, 1, 2, 3, 1, 2, 3, 9, 10, 11};

        final var read = read(Q1);
        assertThat(read.result).containsExactly(Q1_unescaped.codePoints().toArray());
        final var tokens = (TokenFactory<CypherToken>) read.lexer.getTokenFactory();
        final var src = new Pair<TokenSource, CharStream>(read.lexer, read.lexer.getInputStream());

        for (int i = 0; i < Q1_offset.length; i++) {
            final var c = read.result[i];
            final var pos = tokens.create(src, -1, null, -1, i, -1, -1, 1).position();
            assertEquals(Q1_offset[i], pos.offset());
            if (pos.offset() != i) {
                assertEquals(Q1_line[i], pos.line());
                assertEquals(Q1_column[i], pos.column());
            }
        }
    }

    private static String escape(int codepoint) {
        return "\\u" + StringUtils.leftPad(Integer.toString(codepoint, 16), 4, '0');
    }

    @SuppressWarnings("unchecked")
    private void assertReasonableOffsets(String in, int[] expected) throws IOException {
        final var read = read(in);
        assertThat(read.result).containsExactly(expected);
        assertReasonableInputPositions(read);
    }

    private Read read(String in) throws IOException {
        final var lexer = CypherAstLexer.fromString(in, rand.nextInt(4096) + 64, rand.nextBoolean());
        final var stream = (CodePointCharStream) lexer.getInputStream();
        final var codepoints =
                IntStream.range(0, stream.size()).map(i -> stream.LA(i + 1)).toArray();
        return new Read(in, lexer, codepoints);
    }

    private record Read(String input, CypherAstLexer lexer, int[] result) {}

    private void assertReasonableInputPositions(Read read) {
        final var in = read.input;
        final var result = read.result;
        final var tokens = (TokenFactory<CypherToken>) read.lexer.getTokenFactory();
        final var src = new Pair<TokenSource, CharStream>(read.lexer, read.lexer.getInputStream());
        int totalLines = (int) in.lines().count();
        for (int i = 0; i < result.length; ++i) {
            var t = tokens.create(src, -1, null, -1, i, -1, -1, -1);
            var pos = t.position();
            if (!matches(Character.toChars(result[i]), in, pos.offset())) {
                var inputSub = in.substring(pos.offset(), pos.offset() + 12);
                var resultSub = IntStream.range(i, Math.min(i + 12, result.length))
                        .mapToObj(j -> pretty(result[j]))
                        .collect(joining(", "));
                var message =
                        """
                    %nExpected to find: %s
                    Position in result (codepoint offset): %s
                    Position in input (char offset): %s
                    Input at expected offset:
                    ... %s ...
                    Result (codepoints):
                    ... %s ...
                    """;
                fail(message.formatted(pretty(result[i]), i, pos, inputSub, resultSub));
            }
            if (pos.offset() != i) {
                assertThat(pos.line()).isLessThanOrEqualTo(totalLines);
            }
        }
    }

    private boolean matches(char[] exp, String in, int offset) {
        if (in.charAt(offset) == exp[0]) {
            return exp.length == 1 || matches(new char[] {exp[1]}, in, offset + 1);
        } else if (in.startsWith(escape(exp[0]), offset)) {
            return exp.length == 1 || matches(new char[] {exp[1]}, in, offset + 6);
        }
        return false;
    }

    private String pretty(int codepoint) {
        final String print;
        switch (codepoint) {
            case '\n' -> print = "'\\n'";
            case '\r' -> print = "'\\r'";
            default -> print = " '" + Character.toString(codepoint) + "'";
        }
        return "%s (%04X)".formatted(print, codepoint);
    }

    final IntSupplier weightedCodepoint = () -> switch (rand.nextInt(6)) {
        case 0 -> rand.randomValues().nextValidCodePoint();
        case 1 -> rand.among('\n', '\r');
        default -> rand.nextInt('~' - '!') + '!';
    };
}
