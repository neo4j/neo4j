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
package org.neo4j.kernel.impl.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;

import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
public class TestExceptionTypeOnInvalidIds {
    private static final long SMALL_POSITIVE_INTEGER = 5;
    private static final long SMALL_NEGATIVE_INTEGER = -5;
    private static final long BIG_POSITIVE_INTEGER = Integer.MAX_VALUE;
    private static final long BIG_NEGATIVE_INTEGER = Integer.MIN_VALUE;
    private static final long SMALL_POSITIVE_LONG = ((long) Integer.MAX_VALUE) + 1;
    private static final long SMALL_NEGATIVE_LONG = -((long) Integer.MIN_VALUE) - 1;
    private static final long BIG_POSITIVE_LONG = Long.MAX_VALUE;
    private static final long BIG_NEGATIVE_LONG = Long.MIN_VALUE;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService writableService;
    private DatabaseManagementService readOnlyService;
    private GraphDatabaseService writableDb;
    private GraphDatabaseService readOnlyDb;

    @BeforeEach
    void createDatabase() {
        Path writableLayout = testDirectory.homePath("writable");
        writableService = new TestDatabaseManagementServiceBuilder(writableLayout)
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(testDirectory.getFileSystem()))
                .build();
        writableDb = writableService.database(DEFAULT_DATABASE_NAME);

        Path readOnlyLayout = testDirectory.homePath("readOnly");
        TestDatabaseManagementServiceBuilder readOnlyBuilder = new TestDatabaseManagementServiceBuilder(readOnlyLayout);
        readOnlyBuilder.setFileSystem(new UncloseableDelegatingFileSystemAbstraction(testDirectory.getFileSystem()));
        // Create database
        readOnlyBuilder.build().shutdown();
        readOnlyService =
                readOnlyBuilder.setConfig(read_only_database_default, true).build();
        readOnlyDb = readOnlyService.database(DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void destroyDatabase() {
        readOnlyService.shutdown();
        writableService.shutdown();
        writableDb = null;
        readOnlyDb = null;
    }

    private static Stream<Long> inputValues() {
        return Stream.of(
                SMALL_POSITIVE_INTEGER,
                SMALL_NEGATIVE_INTEGER,
                BIG_POSITIVE_INTEGER,
                BIG_NEGATIVE_INTEGER,
                SMALL_POSITIVE_LONG,
                SMALL_NEGATIVE_LONG,
                BIG_POSITIVE_LONG,
                BIG_NEGATIVE_LONG);
    }

    @ParameterizedTest
    @MethodSource("inputValues")
    void shouldThrowOnGetNodeByIdWithNonExistingId(long id) {
        getNonExistingNodeById(writableDb, id);
        getNonExistingNodeById(readOnlyDb, id);
    }

    @ParameterizedTest
    @MethodSource("inputValues")
    void shouldThrowOnGetRelationshipByIdWithNonExistingId(long id) {
        getNonExistingRelationshipById(writableDb, id);
        getNonExistingRelationshipById(readOnlyDb, id);
    }

    private static void getNonExistingNodeById(GraphDatabaseService db, long index) {
        try (Transaction tx = db.beginTx()) {
            assertThrows(NotFoundException.class, () -> tx.getNodeById(index));
        }
    }

    private static void getNonExistingRelationshipById(GraphDatabaseService db, long index) {
        try (Transaction tx = db.beginTx()) {
            assertThrows(NotFoundException.class, () -> tx.getRelationshipById(index));
        }
    }
}
