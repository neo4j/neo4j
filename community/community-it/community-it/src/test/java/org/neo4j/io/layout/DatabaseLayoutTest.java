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
package org.neo4j.io.layout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class DatabaseLayoutTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void databaseLayoutForAbsoluteFile() {
        Path databaseDir = testDirectory.directory("neo4j");
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(databaseDir);
        assertEquals(databaseLayout.databaseDirectory(), databaseDir);
    }

    @Test
    void databaseLayoutResolvesLinks() throws IOException {
        Path basePath = testDirectory.homePath();
        Path databaseDir = databaseLayout.databaseDirectory();
        Path linkPath = basePath.resolve("link");
        Path symbolicLink = null;
        try {
            symbolicLink = Files.createSymbolicLink(linkPath, databaseDir);
        } catch (FileSystemException e) {
            if (e.getMessage().contains("privilege")) {
                assumeTrue(false, "Permission issues creating symbolic links in this environment: " + e);
            }
        }
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(symbolicLink);
        assertEquals(databaseLayout.databaseDirectory(), databaseDir);
    }

    @Test
    void databaseLayoutUseCanonicalRepresentation() {
        Path dbDir = testDirectory.directory("notCanonical");
        Path notCanonicalPath = dbDir.resolve("../anotherdatabase");
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(notCanonicalPath);
        assertEquals(testDirectory.directory("anotherdatabase"), databaseLayout.databaseDirectory());
    }

    @Test
    void databaseLayoutForName() {
        String databaseName = "testdatabase";
        Neo4jLayout storeLayout = neo4jLayout;
        DatabaseLayout testDatabase = storeLayout.databaseLayout(databaseName);
        assertEquals(storeLayout.databasesDirectory().resolve(databaseName), testDatabase.databaseDirectory());
    }

    @Test
    void databaseLayoutForFolderAndName() {
        String database = "database";
        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout(database);
        assertEquals(database, databaseLayout.databaseDirectory().getFileName().toString());
    }

    @Test
    void databaseLayoutProvideCorrectDatabaseName() {
        assertEquals("neo4j", databaseLayout.getDatabaseName());
        assertEquals("testdb", neo4jLayout.databaseLayout("testdb").getDatabaseName());
    }

    @Test
    void storeFilesThrowsForPlainLayout() {
        String database = "database";
        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout(database);
        assertThrows(IllegalStateException.class, databaseLayout::storeFiles);
    }

    @Test
    void mandatoryStoreFilesThrowsForPlainLayout() {
        String database = "database";
        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout(database);
        assertThrows(IllegalStateException.class, databaseLayout::mandatoryStoreFiles);
    }
}
