/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.util;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.kernel.impl.util.Converters.regexFiles;
import static org.neo4j.kernel.impl.util.Converters.toFiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ConvertersTest {
    @Inject
    private TestDirectory directory;

    @Test
    void shouldSortFilesByNumberCleverly() throws Exception {
        // GIVEN
        Path file1 = existenceOfFile("file1");
        Path file123 = existenceOfFile("file123");
        Path file12 = existenceOfFile("file12");
        Path file2 = existenceOfFile("file2");
        Path file32 = existenceOfFile("file32");

        // WHEN
        Path[] files =
                regexFiles(true).apply(directory.file("file").toAbsolutePath().toString() + ".*");

        // THEN
        assertArrayEquals(new Path[] {file1, file2, file12, file32, file123}, files);
    }

    @Test
    void shouldParseFile() throws IOException {
        // given
        Path file = existenceOfFile("file");

        // when
        Path[] files = regexFiles(true).apply(file.toString());

        // then
        assertEquals(List.of(file), List.of(files));
    }

    @Test
    void shouldParseRegexFileWithDashes() throws IOException {
        assumeFalse(IS_OS_WINDOWS);
        // given
        Path file1 = existenceOfFile("file_1");
        Path file3 = existenceOfFile("file_3");
        Path file12 = existenceOfFile("file_12");

        // when
        Path[] files = regexFiles(true).apply(file1.getParent() + File.separator + "file_\\d+");
        Path[] files2 = regexFiles(true).apply(file1.getParent() + File.separator + "file_\\d{1,5}");

        // then
        assertEquals(List.of(file1, file3, file12), List.of(files));
        assertEquals(List.of(file1, file3, file12), List.of(files2));
    }

    @Test
    void shouldParseRegexFileWithDoubleDashes() throws IOException {
        // given
        Path file1 = existenceOfFile("file_1");
        Path file3 = existenceOfFile("file_3");
        Path file12 = existenceOfFile("file_12");

        // when
        Path[] files = regexFiles(true).apply(file1.getParent() + File.separator + "file_\\\\d+");
        Path[] files2 = regexFiles(true).apply(file1.getParent() + File.separator + "file_\\\\d{1,5}");

        // then
        assertEquals(List.of(file1, file3, file12), List.of(files));
        assertEquals(List.of(file1, file3, file12), List.of(files2));
    }

    @Test
    void shouldConsiderInnerQuotationWhenSplittingMultipleFiles() throws IOException {
        // given
        Path header = existenceOfFile("header.csv");
        Path file1 = existenceOfFile("file_1.csv");
        Path file3 = existenceOfFile("file_3.csv");
        Path file12 = existenceOfFile("file_12.csv");

        // when
        Function<String, Path[]> regexMatcher = regexFiles(true);
        Function<String, Path[]> converter = toFiles(",", regexMatcher);
        Path[] files = converter.apply(header + ",'" + header.getParent() + File.separator + "file_\\\\d{1,5}.csv'");

        // then
        assertEquals(List.of(header, file1, file3, file12), List.of(files));
    }

    @Test
    void shouldFailWithProperErrorMessageOnMissingEndQuote() {
        // given
        Function<String, Path[]> regexMatcher = s -> {
            throw new UnsupportedOperationException("Should not required");
        };
        Function<String, Path[]> converter = toFiles(",", regexMatcher);

        // when/then
        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> converter.apply("thing1,'thing2,test,thing3"));
        assertThat(exception.getMessage(), containsString("no matching end quote"));
    }

    private Path existenceOfFile(String name) throws IOException {
        Path file = directory.file(name);
        Files.createFile(file);
        return file;
    }
}
