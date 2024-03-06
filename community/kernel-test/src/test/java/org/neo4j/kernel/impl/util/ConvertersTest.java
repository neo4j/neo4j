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
package org.neo4j.kernel.impl.util;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.kernel.impl.util.Converters.regexFiles;
import static org.neo4j.kernel.impl.util.Converters.toFiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        Path[] files = regexFiles(directory.getFileSystem(), true)
                .apply(directory.file("file").toAbsolutePath() + ".*");

        // THEN
        assertThat(files).containsExactly(file1, file2, file12, file32, file123);
    }

    @Test
    void shouldParseFile() throws IOException {
        // given
        Path file = existenceOfFile("file");

        // when
        Path[] files = regexFiles(directory.getFileSystem(), true).apply(file.toString());

        // then
        assertThat(files).containsExactly(file);
    }

    @Test
    void shouldParseRegexFileWithDashes() throws IOException {
        assumeFalse(IS_OS_WINDOWS);
        // given
        Path file1 = existenceOfFile("file_1");
        Path file3 = existenceOfFile("file_3");
        Path file12 = existenceOfFile("file_12");

        // when
        Path[] files =
                regexFiles(directory.getFileSystem(), true).apply(file1.getParent() + File.separator + "file_\\d+");
        Path[] files2 =
                regexFiles(directory.getFileSystem(), true).apply(file1.getParent() + File.separator + "file_\\d{1,5}");

        // then
        assertThat(files).containsExactly(file1, file3, file12);
        assertThat(files2).containsExactly(file1, file3, file12);
    }

    @Test
    void shouldParseRegexFileWithDoubleDashes() throws IOException {
        // given
        Path file1 = existenceOfFile("file_1");
        Path file3 = existenceOfFile("file_3");
        Path file12 = existenceOfFile("file_12");

        // when
        Path[] files =
                regexFiles(directory.getFileSystem(), true).apply(file1.getParent() + File.separator + "file_\\\\d+");
        Path[] files2 = regexFiles(directory.getFileSystem(), true)
                .apply(file1.getParent() + File.separator + "file_\\\\d{1,5}");

        // then
        assertThat(files).containsExactly(file1, file3, file12);
        assertThat(files2).containsExactly(file1, file3, file12);
    }

    @Test
    void shouldConsiderInnerQuotationWhenSplittingMultipleFiles() throws IOException {
        // given
        Path header = existenceOfFile("header.csv");
        Path file1 = existenceOfFile("file_1.csv");
        Path file3 = existenceOfFile("file_3.csv");
        Path file12 = existenceOfFile("file_12.csv");

        // when
        Function<String, Path[]> regexMatcher = regexFiles(directory.getFileSystem(), true);
        Function<String, Path[]> converter = toFiles(",", regexMatcher);
        Path[] files = converter.apply(header + ",'" + header.getParent() + File.separator + "file_\\\\d{1,5}.csv'");

        // then
        assertThat(files).containsExactly(header, file1, file3, file12);
    }

    @Test
    void shouldFailWithProperErrorMessageOnMissingEndQuote() {
        // given
        Function<String, Path[]> regexMatcher = s -> {
            throw new UnsupportedOperationException("Should not required");
        };
        Function<String, Path[]> converter = toFiles(",", regexMatcher);

        // when/then
        assertThatThrownBy(() -> converter.apply("thing1,'thing2,test,thing3"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no matching end quote");
    }

    private Path existenceOfFile(String name) throws IOException {
        Path file = directory.file(name);
        Files.createFile(file);
        return file;
    }
}
