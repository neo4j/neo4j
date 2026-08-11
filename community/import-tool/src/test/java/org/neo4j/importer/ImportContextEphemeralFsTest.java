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
package org.neo4j.importer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * What {@link ImportContext} puts in the context directory goes through the {@link FileSystemAbstraction} it was
 * given, so an abstraction holding its files somewhere other than the default filesystem has to work too. The
 * write-protected records are the interesting ones: taking the write permission off a file is the one step that
 * cannot go through the abstraction, so it has to step aside rather than fail on a path the default filesystem does
 * not have.
 */
@EphemeralTestDirectoryExtension
class ImportContextEphemeralFsTest {

    private static final NormalizedDatabaseName DB = new NormalizedDatabaseName("foo");

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDir;

    private Config config;

    @BeforeEach
    void setup() {
        config = Config.defaults(neo4j_home, testDir.homePath());
    }

    @Test
    void shouldPersistAndReadBackTheRecordsOfAnAttempt() throws IOException {
        var args = List.of("--nodes=foo.csv");
        Path baseDir;
        try (var importContext = ImportContext.create(fs, DB, null, config, null, args, false, true, false)) {
            baseDir = importContext.baseDir();
            importContext.persistCliArgs();
            importContext.persistNodesPerRange(42L);
            assertThat(ImportContext.wasSuccessful(fs, baseDir)).isFalse();

            importContext.markSuccessful();
        }

        assertThat(ImportContext.readCliArgs(fs, baseDir)).contains(args);
        assertThat(ImportContext.wasSuccessful(fs, baseDir)).isTrue();
        assertThat(contents(baseDir.resolve(ImportContext.NODES_PER_RANGE_FILE_NAME)))
                .isEqualTo("42");
    }

    @Test
    void shouldOverwriteTheRecordsOfTheAttemptBeingResumed() throws IOException {
        var args = List.of("--nodes=foo.csv");
        Path baseDir;
        try (var importContext = ImportContext.create(fs, DB, null, config, null, args, false, true, false)) {
            baseDir = importContext.baseDir();
            importContext.persistCliArgs();
        }

        var resumedArgs = List.of("--nodes=bar.csv", "--resume");
        try (var importContext = ImportContext.create(fs, DB, baseDir, config, null, resumedArgs, false, true, false)) {
            importContext.persistCliArgs();
        }

        assertThat(ImportContext.readCliArgs(fs, baseDir)).contains(resumedArgs);
    }

    @Test
    void shouldWriteCollectorOutputIntoTheContextDirectory() throws IOException {
        Path baseDir;
        try (var importContext = ImportContext.create(fs, DB, null, config, null, List.of(), false, true, false)) {
            baseDir = importContext.baseDir();
            importContext.collectorChannel().writeAll(ByteBuffer.wrap("bad tings".getBytes(UTF_8)));
        }

        assertThat(contents(baseDir.resolve(ImportContext.DEFAULT_REPORT_FILE_NAME)))
                .isEqualTo("bad tings");
    }

    private String contents(Path path) throws IOException {
        try (var input = fs.openAsInputStream(path)) {
            return new String(input.readAllBytes(), UTF_8);
        }
    }
}
