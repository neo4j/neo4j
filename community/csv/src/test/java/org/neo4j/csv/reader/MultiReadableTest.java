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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringReader;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.EntityType;

class MultiReadableTest {
    private static final Configuration CONFIG =
            Configuration.newBuilder().withBufferSize(200).build();
    private final Mark mark = new Mark();
    private final Extractors extractors = new Extractors();
    private final int delimiter = ',';

    @Test
    void shouldReadFromMultipleReaders() throws Exception {
        // GIVEN
        String[][] data = new String[][] {
            {"this is", "the first line"},
            {"where this", "is the second line"},
            {"and here comes", "the third line"}
        };
        RawIterator<CharReadable, IOException> readers = readerIteratorFromStrings(data, null);
        CharSeeker seeker = CharSeekers.charSeeker(new MultiReadable(readers), CONFIG, true, EntityType.NODE);

        // WHEN/THEN
        for (String[] line : data) {
            assertNextLine(line, seeker, mark, extractors);
        }
        assertThat(seeker.seek(mark, delimiter)).isFalse();
        seeker.close();
    }

    @Test
    void shouldHandleSourcesEndingWithNewLine() throws Exception {
        // GIVEN
        String[][] data = new String[][] {
            {"this is", "the first line"},
            {"where this", "is the second line"},
        };

        // WHEN
        RawIterator<CharReadable, IOException> readers = readerIteratorFromStrings(data, '\n');
        CharSeeker seeker = CharSeekers.charSeeker(new MultiReadable(readers), CONFIG, true, EntityType.NODE);

        // WHEN/THEN
        for (String[] line : data) {
            assertNextLine(line, seeker, mark, extractors);
        }
        assertThat(seeker.seek(mark, delimiter)).isFalse();
        seeker.close();
    }

    @Test
    void shouldTrackAbsolutePosition() throws Exception {
        // GIVEN
        String[][] data = new String[][] {
            {"this is", "the first line"}, // 21+delimiter+newline = 23 characters
            {"where this", "is the second line"}, // 28+delimiter+newline = 30 characters
        };
        RawIterator<CharReadable, IOException> readers = readerIteratorFromStrings(data, '\n');
        CharReadable reader = new MultiReadable(readers);
        assertThat(reader.position()).isZero();
        SectionedCharBuffer buffer = new SectionedCharBuffer(15);

        // WHEN
        reader.read(buffer, buffer.front());
        assertThat(reader.position()).isEqualTo(15);
        reader.read(buffer, buffer.front());
        assertThat(reader.position())
                .as("Should not transition to a new reader in the middle of a read")
                .isEqualTo(23);
        assertThat(reader.sourceDescription()).isEqualTo("Reader1");

        // we will transition to the new reader in the call below
        reader.read(buffer, buffer.front());
        assertThat(reader.position()).isEqualTo(23 + 15);
        reader.read(buffer, buffer.front());
        assertThat(reader.position()).isEqualTo(23 + 30);
        reader.read(buffer, buffer.front());
        assertThat(buffer.hasAvailable()).isFalse();
    }

    @Test
    void shouldNotCrossSourcesInOneRead() throws Exception {
        // given
        String source1 = "abcdefghijklm";
        String source2 = "nopqrstuvwxyz";
        String[][] data = new String[][] {{source1}, {source2}};
        CharReadable readable = new MultiReadable(readerIteratorFromStrings(data, '\n'));

        // when
        char[] target = new char[source1.length() + source2.length() / 2];
        int read = readable.read(target, 0, target.length);

        // then
        assertThat(read).isEqualTo(source1.length() + 1);

        // and when
        target = new char[source2.length()];
        read = readable.read(target, 0, target.length);

        // then
        assertThat(read).isEqualTo(source2.length());

        read = readable.read(target, 0, target.length);
        assertThat(read).isOne();
    }

    private void assertNextLine(String[] line, CharSeeker seeker, Mark mark, Extractors extractors) throws IOException {
        for (String value : line) {
            assertThat(seeker.seek(mark, delimiter)).isTrue();
            assertThat(seeker.extract(mark, extractors.string())).isEqualTo(value);
        }
        assertThat(mark.isEndOfLine()).isTrue();
    }

    private RawIterator<CharReadable, IOException> readerIteratorFromStrings(
            final String[][] data, final Character lineEnding) {
        return new RawIterator<>() {
            private int cursor;

            @Override
            public boolean hasNext() {
                return cursor < data.length;
            }

            @Override
            public CharReadable next() {
                String string = join(data[cursor++]);
                return Readables.wrap(
                        new StringReader(string) {
                            @Override
                            public String toString() {
                                return "Reader" + cursor;
                            }
                        },
                        string.length() * 2,
                        null);
            }

            private String join(String[] strings) {
                StringBuilder builder = new StringBuilder();
                for (String string : strings) {
                    builder.append(builder.isEmpty() ? "" : ",").append(string);
                }
                if (lineEnding != null) {
                    builder.append(lineEnding);
                }
                return builder.toString();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
