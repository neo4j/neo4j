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
package org.neo4j.storemigration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.MigrateStoreCommand;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import picocli.CommandLine;

public class StoreMigrationTestUtils {
    public static Result runStoreMigrationCommandFromSameJvm(Neo4jLayout neo4jLayout, String... args) {
        return runStoreMigrationCommandFromSameJvm(neo4jLayout, MigrateStoreCommand::new, args);
    }

    public static Result runStoreMigrationCommandFromSameJvm(
            Neo4jLayout neo4jLayout, Function<ExecutionContext, MigrateStoreCommand> commandFactory, String... args) {
        var homeDir = neo4jLayout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve("conf");
        var out = new Output();
        var err = new Output();

        var ctx = new ExecutionContext(
                homeDir, configDir, out.printStream, err.printStream, new DefaultFileSystemAbstraction());

        var command = CommandLine.populateCommand(commandFactory.apply(ctx), args);

        try {
            int exitCode = command.call();
            return new Result(exitCode, out.toString(), err.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertFormatFamily(GraphDatabaseService db, String formatFamily) {
        StoreVersion storeVersion = getStoreVersion(db);
        assertEquals(formatFamily, storeVersion.formatName());
    }

    public static StoreVersion getStoreVersion(GraphDatabaseService db) {
        var dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        var storageEngineFactory = dependencyResolver.resolveDependency(StorageEngineFactory.class);
        var cursorContextFactory = dependencyResolver.resolveDependency(CursorContextFactory.class);
        try (var cursorContext = cursorContextFactory.create("Test")) {
            StoreId storeId = storageEngineFactory.retrieveStoreId(
                    dependencyResolver.resolveDependency(FileSystemAbstraction.class),
                    dependencyResolver.resolveDependency(DatabaseLayout.class),
                    dependencyResolver.resolveDependency(PageCache.class),
                    cursorContext);
            return storageEngineFactory.versionInformation(storeId).orElseThrow();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class Output {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(buffer);

        @Override
        public String toString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

    public record Result(int exitCode, String out, String err) {}
}
