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
package org.neo4j.dbms.archive;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.TreeSet;
import org.neo4j.annotations.service.Service;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.CheckDatabase.Source.DataTxnSource;
import org.neo4j.dbms.archive.CheckDatabase.Source.PathSource;
import org.neo4j.io.IOUtils.AutoCloseables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageEngineFactory;

@Service
public interface CheckDatabase {
    /**
     * @param autoCloseables Collection to add resources that should be closed/cleaned up after done with the
     *                       extracted data.
     */
    static DatabaseLayout selectAndExtract(
            FileSystemAbstraction fs,
            Source source,
            NormalizedDatabaseName database,
            InternalLogProvider logProvider,
            boolean verbose,
            Config config,
            boolean force,
            AutoCloseables<?> autoCloseables)
            throws IOException {

        for (final var checkDatabase : all()) {
            if (!checkDatabase.containsPotentiallyCheckableDatabase(fs, source, database)) {
                continue;
            }

            try {
                final var targetLayout = checkDatabase.targetLayoutFrom(fs, source, database, autoCloseables);
                checkDatabase.tryExtract(fs, config, targetLayout, source, database, logProvider, verbose, force);

                final var storageEngineFactory = StorageEngineFactory.selectStorageEngine(fs, targetLayout)
                        .orElseThrow(() ->
                                new IllegalArgumentException("No storage engine found for '%s' with database name '%s'"
                                        .formatted(targetLayout.getNeo4jLayout(), targetLayout.getDatabaseName())));
                return storageEngineFactory.formatSpecificDatabaseLayout(targetLayout);

            } catch (UnsupportedOperationException u) {
                // Let it through
                throw u;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid input for a " + checkDatabase.getClass().getSimpleName(), e);
            }
        }

        throw new IllegalArgumentException("Could not find a valid %s named '%s' to check at %s"
                .formatted(allNames("or"), database.name(), source));
    }

    String name();

    boolean containsPotentiallyCheckableDatabase(
            FileSystemAbstraction fs, Source source, NormalizedDatabaseName database);

    DatabaseLayout targetLayoutFrom(
            FileSystemAbstraction fs, Source source, NormalizedDatabaseName database, AutoCloseables<?> autoCloseables)
            throws IOException, CannotWriteException;

    void tryExtract(
            FileSystemAbstraction fs,
            Config config,
            DatabaseLayout targetLayout,
            Source source,
            NormalizedDatabaseName database,
            InternalLogProvider logProvider,
            boolean verbose,
            boolean force)
            throws Exception;

    static Collection<CheckDatabase> all() {
        return Services.loadAll(CheckDatabase.class);
    }

    private static String allNames(String logical) {
        final var namesSet = new TreeSet<String>();
        all().forEach(check -> namesSet.add(check.name()));
        final var names = namesSet.iterator();

        if (!names.hasNext()) {
            throw new IllegalStateException(
                    "At least one %s is expected".formatted(CheckDatabase.class.getSimpleName()));
        }

        final var sb = new StringBuilder(names.next());
        while (names.hasNext()) {
            sb.append(", ");
            final var name = names.next();
            if (!names.hasNext()) {
                sb.append(logical).append(" ");
            }
            sb.append(name);
        }

        return sb.toString();
    }

    class TempDir implements AutoCloseable {
        private final FileSystemAbstraction fs;
        public final Path path;

        public TempDir(FileSystemAbstraction fs) throws IOException {
            this(fs, null);
        }

        public TempDir(FileSystemAbstraction fs, Path dir) throws IOException {
            this.fs = fs;
            this.path = dir != null ? fs.createTempDirectory(dir, null) : fs.createTempDirectory(null);
        }

        @Override
        public void close() throws IOException {
            fs.deleteRecursively(path);
        }
    }

    abstract sealed class Source permits PathSource, DataTxnSource {

        public final Neo4jLayout layout;

        public Source(Neo4jLayout layout) {
            this.layout = layout;
        }

        public static final class PathSource extends Source {
            public final Path path;
            public final Path tmpRoot;

            public PathSource(Path path) {
                this(path, null);
            }

            public PathSource(Path path, Path tmpRoot) {
                super(Neo4jLayout.ofFlat(path.toAbsolutePath().normalize()));
                this.path = path.toAbsolutePath().normalize();
                this.tmpRoot = tmpRoot != null ? tmpRoot.toAbsolutePath().normalize() : null;
            }

            public TempDir createTemporaryDirectory(FileSystemAbstraction fs) throws CannotWriteException, IOException {
                if (fs.fileExists(tmpRoot) && !Files.isWritable(tmpRoot)) {
                    throw new CannotWriteException(tmpRoot);
                }
                return new TempDir(fs, tmpRoot);
            }

            @Override
            public String toString() {
                return "path: '%s'".formatted(path);
            }
        }

        public static final class DataTxnSource extends Source {
            public DataTxnSource(Config config) {
                this(
                        config.get(neo4j_home),
                        config.get(data_directory),
                        config.get(transaction_logs_root_path),
                        config.get(databases_root_path));
            }

            public DataTxnSource(Path dataPath, Path txnPath) {
                this(dataPath, dataPath, txnPath, dataPath);
            }

            public DataTxnSource(Path neo4jHome, Path data, Path transactionLogsRoot, Path databasesRoot) {
                super(Neo4jLayout.of(Config.newBuilder()
                        .set(neo4j_home, neo4jHome.toAbsolutePath().normalize())
                        .set(data_directory, data.toAbsolutePath().normalize())
                        .set(
                                transaction_logs_root_path,
                                transactionLogsRoot.toAbsolutePath().normalize())
                        .set(databases_root_path, databasesRoot.toAbsolutePath().normalize())
                        .build()));
            }

            @Override
            public String toString() {
                return "databases path: '%s', transaction log path: '%s'"
                        .formatted(layout.databasesDirectory(), layout.transactionLogsRootDirectory());
            }
        }

        public static <S extends Source> S expected(Class<S> clazz, Source source) {
            if (!clazz.isInstance(source)) {
                throw new IllegalStateException("Expected a %s, however was given a %s."
                        .formatted(
                                clazz.getName(),
                                source != null ? source.getClass().getName() : null));
            }

            return clazz.cast(source);
        }
    }
}
