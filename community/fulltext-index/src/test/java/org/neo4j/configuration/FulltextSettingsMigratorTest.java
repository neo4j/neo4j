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
package org.neo4j.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.FulltextSettings.eventually_consistent;
import static org.neo4j.configuration.FulltextSettings.eventually_consistent_index_update_queue_max_length;
import static org.neo4j.configuration.FulltextSettings.fulltext_default_analyzer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class FulltextSettingsMigratorTest {

    @Inject
    private TestDirectory testDirectory;

    @Test
    void migrateSamplingSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "db.index.fulltext.default_analyzer=ukrainian",
                        "db.index.fulltext.eventually_consistent=false",
                        "db.index.fulltext.eventually_consistent_index_update_queue_max_length=125"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals("ukrainian", config.get(fulltext_default_analyzer));
        assertFalse(config.get(eventually_consistent));
        assertEquals(125, config.get(eventually_consistent_index_update_queue_max_length));
    }
}
