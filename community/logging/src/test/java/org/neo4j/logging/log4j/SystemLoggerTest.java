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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@ResourceLock(Resources.SYSTEM_OUT)
@TestDirectoryExtension
@ExtendWith(SuppressOutputExtension.class)
class SystemLoggerTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void interceptLoggingErrors() throws IOException {
        SystemLogger.installErrorListener();
        try {
            Path log4jConfig = testDirectory.file("user-logs.xml");
            FileSystemUtils.writeString(fs, log4jConfig, "<Configuration><", EmptyMemoryTracker.INSTANCE);
            LogConfig.createLoggerFromXmlConfig(fs, log4jConfig);
            assertThat(SystemLogger.errorsEncounteredDuringSetup()).isTrue();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            SystemLogger.printErrorMessages(new PrintStream(out));
            assertThat(out.toString()).contains("ERROR Error parsing");
            assertThat(suppressOutput.getErrorVoice().containsMessage("[Fatal Error] user-logs.xml:1:17"))
                    .isTrue();
        } finally {
            // Make sure to clear listeners
            SystemLogger.errorsEncounteredDuringSetup();
        }
    }
}
