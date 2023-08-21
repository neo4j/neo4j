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
package org.neo4j.logging.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DatabaseLogServiceTest {
    @Inject
    private TestDirectory testDirectory;

    private DatabaseLogService logService;
    private final NamedDatabaseId namedDatabaseId = DatabaseIdFactory.from("foo", UUID.randomUUID());
    private Path file;
    private Neo4jLoggerContext ctx;

    @BeforeEach
    void setUp() {
        file = testDirectory.file("test.log");
        ctx = LogConfig.createTemporaryLoggerToSingleFile(testDirectory.getFileSystem(), file, Level.DEBUG, true);
        InternalLogProvider logProvider = new Log4jLogProvider(ctx);
        logService = new DatabaseLogService(namedDatabaseId, new SimpleLogService(logProvider));
    }

    @AfterEach
    void tearDown() {
        ctx.close();
    }

    @Test
    void shouldReturnUserLogProvider() throws IOException {
        var logProvider = logService.getUserLogProvider();
        var log = logProvider.getLog("log_name");
        log.info("message");

        assertLogged("[log_name] " + "[" + namedDatabaseId.logPrefix() + "] message");
    }

    @Test
    void shouldReturnInternalLogProvider() throws IOException {
        var logProvider = logService.getInternalLogProvider();
        var log = logProvider.getLog(Object.class);
        log.info("message");

        assertLogged("[j.l.Object] " + "[" + namedDatabaseId.logPrefix() + "] message");
    }

    @Test
    void shouldReturnDifferentUserAndInternalLogProviders() {
        var userLogProvider = logService.getUserLogProvider();
        var internalLogProvider = logService.getInternalLogProvider();

        assertNotEquals(userLogProvider, internalLogProvider);
    }

    @Test
    void shouldAlwaysReturnSameUserLogProvider() {
        var logProvider1 = logService.getUserLogProvider();
        var logProvider2 = logService.getUserLogProvider();

        assertSame(logProvider1, logProvider2);
    }

    @Test
    void shouldAlwaysReturnSameInternalLogProvider() {
        var logProvider1 = logService.getInternalLogProvider();
        var logProvider2 = logService.getInternalLogProvider();

        assertSame(logProvider1, logProvider2);
    }

    private void assertLogged(String message) throws IOException {
        assertThat(Files.readString(file)).contains(message);
    }
}
