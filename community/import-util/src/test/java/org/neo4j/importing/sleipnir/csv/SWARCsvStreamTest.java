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
package org.neo4j.importing.sleipnir.csv;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SWARCsvStreamTest {
    @Test
    void ignoreSeparatorsInQuotes() {
        for (int i = 1; i < Long.SIZE; i++) {
            String firstField = "1".repeat(i);
            for (int j = 0; j < Long.SIZE; j++) {
                String lastField = "c".repeat(j);
                assertThat(toFields(firstField + ",\"a,b\"," + lastField))
                        .containsExactly(firstField, "\"a,b\"", lastField);
                assertThat(toFields(firstField + ",\"a\nb\"," + lastField))
                        .containsExactly(firstField, "\"a\nb\"", lastField);
                assertThat(toFields(firstField + ",\"a\"\"\nb\"," + lastField))
                        .containsExactly(firstField, "\"a\"\"\nb\"", lastField);
            }
        }
    }

    @Test
    void ignoreEscapeCharacters() {
        for (int i = 1; i < Long.SIZE; i++) {
            String firstField = "1".repeat(i);
            for (int j = 1; j < Long.SIZE; j++) {
                String lastField = "c".repeat(j);
                assertThat(toFields(firstField + ",\"a,b\\\"," + lastField))
                        .containsExactly(firstField, "\"a,b\\\"", lastField);
            }
        }
    }

    @Test
    void handelLegacyEscapeCharacters() {
        for (int i = 1; i < Long.SIZE; i++) {
            String firstField = "1".repeat(i);
            for (int j = 1; j < Long.SIZE; j++) {
                String lastField = "c".repeat(j);
                assertThat(toFieldsLegacy(firstField + ",\"\\\"a,b\"," + lastField))
                        .containsExactly(firstField, "\"\\\"a,b\"", lastField);
                assertThat(toFieldsLegacy(firstField + ",\"a,b\\\\\"," + lastField))
                        .containsExactly(firstField, "\"a,b\\\\\"", lastField);
            }
        }
    }

    @Test
    void errorOnUnclosedQuote() {
        for (int i = 1; i < Long.SIZE; i++) {
            String firstField = "1".repeat(i);
            assertThatCode(() -> toFields(firstField + ",\"not closed"))
                    .hasMessageContaining("ended without closing an open quote");
        }
    }

    @Test
    void errorOnDanglingEscape() {
        for (int i = 1; i < Long.SIZE; i++) {
            String firstField = "1".repeat(i);
            assertThatCode(() -> toFieldsLegacy(firstField + ",a\\"))
                    .hasMessageContaining("ended with an escape character");
        }
    }

    @Test
    void failOnUnreasonableSeparators() {
        assertThatCode(() -> new SWARCsvStream('ሴ', '"', false))
                .hasMessageContaining("'ሴ' is not supported as separator");
        assertThatCode(() -> new SWARCsvStream(',', '⍅', false)).hasMessageContaining("'⍅' is not supported as quote");
    }

    @Test
    void shouldReadingFullLegacyCsv() throws IOException {
        byte[] data = loadCSVFile();
        int[] offsets = new int[1024 * 16];
        SWARCsvStream vectoredStream = new SWARCsvStream((byte) ',', (byte) '"', true);
        int offsetSize =
                vectoredStream.indexSeparators(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN), 0, offsets);
        offsets = Arrays.copyOf(offsets, offsetSize);
        int[] expected = getExpected();
        assertThat(offsets).isEqualTo(expected);
    }

    private static List<String> toFields(String s) {
        return toFields(s, false);
    }

    private static List<String> toFieldsLegacy(String s) {
        return toFields(s, true);
    }

    private static List<String> toFields(String s, boolean legacyEscape) {
        ByteBuffer bytes = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)).order(ByteOrder.LITTLE_ENDIAN);
        SWARCsvStream swarCsvStream = new SWARCsvStream((byte) ',', (byte) '"', legacyEscape);
        int[] offsets = new int[1024];
        int offsetSize = swarCsvStream.indexSeparators(bytes, 0, offsets);
        swarCsvStream.validateEnd();

        List<String> ret = new ArrayList<>();
        int previousOffset = 0;
        for (int i = 0; i < offsetSize; i++) {
            int nextOffset = (int) offsets[i];
            ret.add(s.substring(previousOffset, nextOffset));

            previousOffset = nextOffset + 1;
        }
        ret.add(s.substring(previousOffset));

        return ret;
    }

    private byte[] loadCSVFile() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = getClass().getResourceAsStream("test.csv")) {
            in.transferTo(out);
        }

        return out.toByteArray();
    }

    private int[] getExpected() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = getClass().getResourceAsStream("expectedOffsets.txt")) {
            in.transferTo(out);
        }

        String string = out.toString(StandardCharsets.UTF_8);
        String[] split = string.split(",");
        int[] expected = new int[split.length];
        for (int i = 0; i < split.length; i++) {
            expected[i] = Integer.parseInt(split[i].trim());
        }
        return expected;
    }
}
