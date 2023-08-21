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
package org.neo4j.server.startup;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TabCompletionGeneratorTest {
    @Inject
    TestDirectory dir;

    @Test
    void shouldGenerateCompletionFile() throws Exception {
        TabCompletionGenerator.main(new String[] {dir.homePath().toString()});
        assertCompletionScriptExists("neo4j");
        assertCompletionScriptExists("neo4j-admin");
    }

    void assertCompletionScriptExists(String name) throws IOException {
        Path file = dir.homePath().resolve(name + "_completion");
        assertThat(file).exists();
        assertThat(Files.readString(file)).contains("Bash completion support for the `" + name + "` command");
    }
}
