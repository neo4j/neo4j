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
package org.neo4j.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.io.fs.FileSystemAbstraction.PatternStyle.REGEX;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.importer.ImportConverters.EscapedCharacterConverter;
import org.neo4j.importer.ImportConverters.FileInputTypeConverter;
import org.neo4j.importer.ImportConverters.IdTypeConverter;
import org.neo4j.importer.ImportConverters.InputFilesGroup;
import org.neo4j.importer.ImportConverters.NodeFilesConverter;
import org.neo4j.importer.ImportConverters.RelationshipFilesConverter;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine.TypeConversionException;

@TestDirectoryExtension
class ImportConvertersTest {
    @Inject
    private TestDirectory testDir;

    @Nested
    class ParseNodeFilesGroup {
        @Test
        void illegalEqualsPosition() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> ImportConverters.parseNodeFilesGroup("=foo.csv"));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> ImportConverters.parseNodeFilesGroup("foo="));
        }

        @Test
        void validateFileExistence() {
            assertThatThrownBy(() -> ImportConverters.parseNodeFilesGroup("nonexisting.file")
                            .toPathArray(testDir.getFileSystem(), REGEX))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(NoSuchFileException.class);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesWithoutLabels(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? foo.toUri() + "," + bar.toUri() : foo + "," + bar;
            final var g = ImportConverters.parseNodeFilesGroup(pathStr);
            assertThat(g.key).isEmpty();
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void singleLabel(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? "BANANA=" + foo.toUri() + "," + bar.toUri() : "BANANA=" + foo + "," + bar;
            final var g = ImportConverters.parseNodeFilesGroup(pathStr);
            assertThat(g.key).containsOnly("BANANA");
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void multipleLabels(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs
                    ? ":APPLE::KIWI : BANANA=" + foo.toUri() + "," + bar.toUri()
                    : ":APPLE::KIWI : BANANA=" + foo + "," + bar;
            final var g = ImportConverters.parseNodeFilesGroup(pathStr);
            assertThat(g.key).containsOnly("BANANA", "KIWI", "APPLE");
            assertPathsFound(testDir, g, foo, bar);
        }

        @Test
        void labelsKeepTheOrderTheyWereSpecifiedIn() {
            final var foo = testDir.createFile("foo.csv");
            final var g = ImportConverters.parseNodeFilesGroup("Middle:Alpha:Zebra=" + foo);
            assertThat(g.key).containsExactly("Middle", "Alpha", "Zebra");
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesRegex(boolean useURIs) {
            final var foo1 = testDir.createFile("foo-1.csv");
            final var foo2 = testDir.createFile("foo-2.csv");
            testDir.createFile("foo-X.csv");
            final var pathStr = useURIs
                    ? "BANANA=" + testDir.absolutePath().toUri() + "/foo-[0-9].csv"
                    : "BANANA=" + testDir.absolutePath() + File.separator + "foo-[0-9].csv";
            final var g = ImportConverters.parseNodeFilesGroup(pathStr);
            assertThat(g.key).containsOnly("BANANA");
            assertPathsFound(testDir, g, foo1, foo2);
        }
    }

    @Nested
    class ParseRelationshipFilesGroup {
        @Test
        void illegalEqualsPosition() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> ImportConverters.parseRelationshipFilesGroup("=foo.csv"));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> ImportConverters.parseRelationshipFilesGroup("foo="));
        }

        @Test
        void validateFileExistence() {
            assertThatThrownBy(() -> ImportConverters.parseRelationshipFilesGroup("nonexisting.file")
                            .toPathArray(testDir.getFileSystem(), REGEX))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(NoSuchFileException.class);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesWithoutRelType(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? foo.toUri() + "," + bar.toUri() : foo + "," + bar;
            final var g = ImportConverters.parseRelationshipFilesGroup(pathStr);
            assertThat(g.key).isNull();
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void withDefaultRelType(boolean useURIs) {
            final var foo = testDir.createFile("foo.csv");
            final var bar = testDir.createFile("bar.csv");
            final var pathStr = useURIs ? "BANANA=" + foo.toUri() + "," + bar.toUri() : "BANANA=" + foo + "," + bar;
            final var g = ImportConverters.parseRelationshipFilesGroup(pathStr);
            assertThat(g.key).isEqualTo("BANANA");
            assertPathsFound(testDir, g, foo, bar);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void filesRegex(boolean useURIs) {
            final var foo1 = testDir.createFile("foo-1.csv");
            final var foo2 = testDir.createFile("foo-2.csv");
            testDir.createFile("foo-X.csv");
            final var pathStr = useURIs
                    ? "BANANA=" + testDir.absolutePath().toUri() + "/foo-[0-9].csv"
                    : "BANANA=" + testDir.absolutePath() + File.separator + "foo-[0-9].csv";
            final var g = ImportConverters.parseRelationshipFilesGroup(pathStr);
            assertThat(g.key).isEqualTo("BANANA");
            assertPathsFound(testDir, g, foo1, foo2);
        }

        @Test
        void relTypeIsTrimmed() {
            final var foo = testDir.createFile("foo.csv");
            final var g = ImportConverters.parseRelationshipFilesGroup(" BANANA =" + foo);
            assertThat(g.key).isEqualTo("BANANA");
        }
    }

    @Nested
    class Converters {
        @Test
        void nodeFilesConverterParsesTheGroup() {
            final var foo = testDir.createFile("foo.csv");
            final var g = new NodeFilesConverter().convert("Alpha:Zebra=" + foo);
            assertThat(g.key).containsExactly("Alpha", "Zebra");
            assertThat(g.files).isEqualTo(foo.toString());
        }

        @Test
        void nodeFilesConverterWrapsParseFailures() {
            assertThatThrownBy(() -> new NodeFilesConverter().convert("=foo.csv"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid nodes file: =foo.csv");
        }

        @Test
        void relationshipFilesConverterParsesTheGroup() {
            final var foo = testDir.createFile("foo.csv");
            final var g = new RelationshipFilesConverter().convert("BANANA=" + foo);
            assertThat(g.key).isEqualTo("BANANA");
            assertThat(g.files).isEqualTo(foo.toString());
        }

        @Test
        void relationshipFilesConverterWrapsParseFailures() {
            assertThatThrownBy(() -> new RelationshipFilesConverter().convert("foo="))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid relationships file: foo=");
        }

        private static Stream<Arguments> escapedCharacters() {
            return Stream.of(
                    arguments(",", ','),
                    arguments(";", ';'),
                    arguments("TAB", '\t'),
                    arguments("\\t", '\t'),
                    arguments("U+002A", '*'),
                    arguments("u+002a", '*'),
                    arguments("\\u002A", '*'),
                    arguments("\\44", ','));
        }

        @ParameterizedTest
        @MethodSource("escapedCharacters")
        void escapedCharacterConverter(String value, char expected) {
            assertThat(new EscapedCharacterConverter().convert(value)).isEqualTo(expected);
        }

        @Test
        void escapedCharacterConverterRejectsUnsupportedValues() {
            assertThatThrownBy(() -> new EscapedCharacterConverter().convert("nonsense"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported character 'nonsense'");
        }

        @Test
        void idTypeConverterRejectsUnknownIdTypes() {
            assertThatThrownBy(() -> new IdTypeConverter().convert("banana"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid id type: banana");
        }

        @Test
        void fileInputTypeConverterListsTheSupportedTypes() {
            assertThatThrownBy(() -> new FileInputTypeConverter().convert("banana"))
                    .isInstanceOf(TypeConversionException.class)
                    .hasMessageContaining("Invalid file import type: banana");
        }
    }

    private static void assertPathsFound(TestDirectory dir, InputFilesGroup<?> group, Path... expectedPaths) {
        // the groups will have either local paths or file URIs so will be handled by the simple scheme system below
        try (var fs = new SchemeFileSystemAbstraction(dir.getFileSystem())) {
            assertThat(group.toPathArray(fs, REGEX)).containsOnly(expectedPaths);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
