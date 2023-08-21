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
package org.neo4j.configuration.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class FromPathsIT {
    @Inject
    private TestDirectory testDirectory;

    private Path neo4j1Directory;
    private Path mongo1Directory;
    private Path redis1Directory;
    private Path dbRoot1Directory;
    private Path neo4j2Directory;
    private Path dbRoot2Directory;

    @BeforeEach
    void setUp() {
        this.neo4j1Directory = testDirectory.directory("neo4j", "db1");
        this.mongo1Directory = testDirectory.directory("mongo", "db1");
        this.redis1Directory = testDirectory.directory("redis", "db1");
        this.neo4j2Directory = testDirectory.directory("neo4j", "db2");
        this.dbRoot1Directory = neo4j1Directory.getParent();
        this.dbRoot2Directory = neo4j2Directory.getParent();
    }

    @Test
    void shouldReturnTheInputValueIfThereIsNoRegexInPath() {
        final var filteredPaths =
                new FromPaths(dbRoot1Directory.toAbsolutePath().toString()).getPaths();

        final var expected = Set.of(dbRoot1Directory);
        assertThat(filteredPaths).containsAll(expected);
    }

    @Test
    void shouldGetAllFoldersThatMatchIfFilterIsApplied() {
        assertThat(new FromPaths(concatenateSubPath(
                                dbRoot1Directory.toAbsolutePath().toString(), "n*"))
                        .getPaths())
                .containsAll(Set.of(neo4j1Directory));
        assertThat(new FromPaths(concatenateSubPath(
                                dbRoot1Directory.toAbsolutePath().toString(), "neo4?"))
                        .getPaths())
                .containsAll(Set.of(neo4j1Directory));
        assertThat(new FromPaths(concatenateSubPath(
                                dbRoot1Directory.toAbsolutePath().toString(), "neo4j"))
                        .getPaths())
                .containsAll(Set.of(neo4j1Directory));
        assertThat(new FromPaths(concatenateSubPath(
                                dbRoot1Directory.toAbsolutePath().toString(), "*4*"))
                        .getPaths())
                .containsAll(Set.of(neo4j1Directory));
        assertThat(new FromPaths(concatenateSubPath(
                                dbRoot1Directory.toAbsolutePath().toString(), "*"))
                        .getPaths())
                .containsAll(Set.of(neo4j1Directory, mongo1Directory, redis1Directory));
    }

    @Test
    void isSingleShouldReturnTrueIfInputIsSingleValue() {
        assertThat(new FromPaths(concatenateSubPath("a", "b")).isSingle()).isTrue();
    }

    @Test
    void isSingleShouldReturnFalseIfInputIsListOfValues() {
        assertThat(new FromPaths(concatenateSubPath("a", "b") + "," + concatenateSubPath("c", "d")).isSingle())
                .isFalse();
    }

    @Test
    void shouldReturnTheInputListIfFilterIsNotApplied() {
        final var paths =
                new FromPaths(dbRoot1Directory.toAbsolutePath() + ", " + dbRoot2Directory.toAbsolutePath()).getPaths();

        final var expected = Set.of(dbRoot1Directory, dbRoot2Directory);
        assertThat(paths).containsAll(expected);
    }

    @Test
    void shouldGetAllFoldersFromTheListOfPathsIfFilterIsApplied() {
        final var filteredPaths = new FromPaths(concatenateSubPath(
                                dbRoot1Directory.toAbsolutePath().toString(), "n*") + ", "
                        + concatenateSubPath(dbRoot2Directory.toAbsolutePath().toString(), "n*"))
                .getPaths();

        final var expected = Set.of(neo4j1Directory, neo4j2Directory);
        assertThat(filteredPaths).containsAll(expected);
    }

    @Test
    void shouldThrowExceptionIfFromPathIllegal() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> assertValid(""));
        assertEquals("The provided from parameter is empty.", e.getMessage());

        Exception e2 = assertThrows(NullPointerException.class, () -> assertValid(null));
        assertEquals("The provided from parameter is empty.", e2.getMessage());

        Exception e3 = assertThrows(IllegalArgumentException.class, () -> assertValid(concatenateSubPath("a*", "b")));
        assertThat(e3.getMessage()).contains("Asterisks and question marks should be placed in the last subpath");

        Exception e4 = assertThrows(IllegalArgumentException.class, () -> assertValid(concatenateSubPath("a", "b->")));
        assertThat(e4.getMessage()).contains("is in illegal format.");
    }

    @Test
    void inputShouldNotPointToTheRootOfTheFileSystem() {
        Iterator<Path> rootDirectories =
                FileSystems.getDefault().getRootDirectories().iterator();
        if (!rootDirectories.hasNext()) {
            //noinspection ConstantConditions
            assumeTrue(false); // Report as ignored
        }
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> assertValid(rootDirectories.next().toAbsolutePath().toString()));
        assertThat(exception.getMessage()).contains("should not point to the root of the file system");
    }

    @Test
    void shouldNotThrowExceptionWhenPathContainsTwoSubpath() {
        new FromPaths(concatenateSubPath("a", "b"));
    }

    private static void assertValid(String name) {
        new FromPaths(name);
    }

    private static String concatenateSubPath(String... paths) {
        return String.join(File.separator, paths);
    }
}
