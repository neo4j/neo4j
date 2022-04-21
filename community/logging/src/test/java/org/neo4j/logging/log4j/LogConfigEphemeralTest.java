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
package org.neo4j.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Scanner;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class LogConfigEphemeralTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    private Neo4jLoggerContext ctx;

    @AfterEach
    void tearDown() {
        ctx.close();
    }

    @Test
    void shouldLogToEphemeralFileSystemAbstraction() throws IOException {
        Path targetFile = dir.homePath().resolve("debug.log");

        ctx = LogConfig.createBuilder(fs, targetFile, Level.DEBUG).build();
        ExtendedLogger logger = ctx.getLogger("test");
        logger.warn("test");

        assertThat(fs.fileExists(targetFile)).isEqualTo(true);
        try (InputStream inputStream = fs.openAsInputStream(targetFile)) {
            Scanner scanner = new Scanner(inputStream);
            String line = scanner.nextLine();
            assertThat(line).contains("WARN  [test] test");
        }
    }
}
