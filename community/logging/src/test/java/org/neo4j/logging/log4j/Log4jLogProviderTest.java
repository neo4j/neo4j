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
package org.neo4j.logging.log4j;

import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.logging.log4j.LogConfigTest.DATE_PATTERN;

import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Level;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class Log4jLogProviderTest {
    private static final int WAIT_TIMEOUT_MINUTES = 1;
    private static final int ITERATIONS = 10000;

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    @Test
    void getLogShouldReturnLogWithCorrectCategory() throws IOException {
        Path file = dir.file("test.log");
        try (Neo4jLoggerContext ctx = LogConfig.createTemporaryLoggerToSingleFile(fs, file, Level.INFO, true)) {
            Log4jLogProvider logProvider = new Log4jLogProvider(ctx);

            InternalLog log = logProvider.getLog("stringAsCategory");
            log.info("testMessage");

            InternalLog log2 = logProvider.getLog(Log4jLog.class);
            log2.info("testMessage2");

            assertThat(Files.readString(file))
                    .matches(format(
                            DATE_PATTERN + " %-5s \\[stringAsCategory\\] testMessage%n" + DATE_PATTERN
                                    + " %-5s \\[o.n.l.l.Log4jLog\\] testMessage2%n",
                            Level.INFO,
                            Level.INFO));
        }
    }

    @Test
    void closeCreatedLogProviders() {
        for (int i = 0; i < 1_000; i++) {
            try (Log4jLogProvider log4jLogProvider = new Log4jLogProvider(
                    LogConfig.createTemporaryLoggerToSingleFile(fs, dir.file("test.log" + i), Level.INFO, true))) {
                log4jLogProvider.getLog("test").info("message");
            }
        }

        await().atMost(ofMinutes(WAIT_TIMEOUT_MINUTES))
                .untilAsserted(() -> assertThat(extractLogManagersMap()).hasSizeLessThan(ITERATIONS));
    }

    @Test
    void doNotCreateDefaultLogLayouts() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertNotNull(Neo4jDebugLogLayout.createLayout("testLayout" + i, new Neo4jConfiguration()));
        }

        await().atMost(ofMinutes(WAIT_TIMEOUT_MINUTES))
                .untilAsserted(() -> assertThat(extractLogManagersMap()).hasSizeLessThan(ITERATIONS));
    }

    private HashMap<String, AbstractManager> extractLogManagersMap() throws IllegalAccessException {
        // make sure we see some variant of the latest map
        VarHandle.fullFence();
        return (HashMap<String, AbstractManager>) FieldUtils.readStaticField(AbstractManager.class, "MAP", true);
    }
}
