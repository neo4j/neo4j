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
package org.neo4j.storageengine.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.block.BlockDatabaseExistMarker;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class StorageEngineFactorySelectionTest {
    @Inject
    TestDirectory dir;

    @Inject
    FileSystemAbstraction fs;

    @Test
    void shouldDetectBlockStorageInMessageWhenNotAvailable() throws IOException {
        assumeThat(StorageEngineFactory.allAvailableStorageEngines()).isEmpty();
        DatabaseLayout layout = Neo4jLayout.of(dir.homePath()).databaseLayout(DEFAULT_DATABASE_NAME);
        Path dbDir = layout.databaseDirectory();

        Path blockExistMarker = dbDir.resolve(BlockDatabaseExistMarker.NAME);
        fs.mkdirs(blockExistMarker.getParent());
        FileSystemUtils.writeString(fs, blockExistMarker, "", EmptyMemoryTracker.INSTANCE);

        assertThat(StorageEngineFactory.selectStorageEngine(fs, layout)).isEmpty();
        assertThatThrownBy(() -> StorageEngineFactory.selectStorageEngine(fs, layout, Config.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Block format detected for database neo4j but unavailable in this edition.");
    }
}
