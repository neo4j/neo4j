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

import static org.neo4j.dbms.archive.Dumper.DUMP_EXTENSION;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.CheckDatabase.Source.PathSource;
import org.neo4j.dbms.archive.Loader.SizeMeta;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils.AutoCloseables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.InternalLogProvider;

@ServiceProvider
public class CheckDump implements CheckDatabase {
    private static final float CONSERVATIVE_DISK_SPACE_SCALAR = 1.2f;

    @Override
    public String name() {
        return "dump";
    }

    @Override
    public boolean containsPotentiallyCheckableDatabase(
            FileSystemAbstraction fs, Source source, NormalizedDatabaseName database) {
        return source instanceof final PathSource pathSource && fs.fileExists(dumpFile(pathSource.path, database));
    }

    @Override
    public DatabaseLayout targetLayoutFrom(
            FileSystemAbstraction fs, Source source, NormalizedDatabaseName database, AutoCloseables<?> autoCloseables)
            throws IOException, CannotWriteException {
        final var pathSource = Source.expected(PathSource.class, source);
        final var temporaryHome = autoCloseables.add(pathSource.createTemporaryDirectory(fs));
        return Neo4jLayout.ofFlat(temporaryHome.path).databaseLayout(database.name());
    }

    @Override
    public void tryExtract(
            FileSystemAbstraction fs,
            Config config,
            DatabaseLayout targetLayout,
            Source source,
            NormalizedDatabaseName database,
            InternalLogProvider logProvider,
            boolean verbose,
            boolean force)
            throws IOException, IncorrectFormat {
        final var pathSource = Source.expected(PathSource.class, source);

        final var dump = dumpFile(pathSource.path, database);
        final var loader = verbose ? new Loader(fs, logProvider) : new Loader(fs);
        checkDiskSpace(fs, dump, loader, force);

        final var log = logProvider.getLog(getClass());
        log.info(
                "Loading dump from: %s%ninto: %s%n",
                dump, targetLayout.getNeo4jLayout().dataDirectory());

        loader.load(dump, targetLayout, false, false, DumpFormatSelector::decompress);
    }

    private static Path dumpFile(Path directory, NormalizedDatabaseName database) {
        return directory.resolve(database.name() + DUMP_EXTENSION);
    }

    private void checkDiskSpace(FileSystemAbstraction fs, Path dump, Loader loader, boolean force) throws IOException {
        if (force) {
            return;
        }

        final var usableSpaceHint = ByteUnit.bytes(dump.toFile().getUsableSpace());
        if (usableSpaceHint == ByteUnit.bytes(0)) {
            return; // implies the usable space cannot be obtained
        }

        final var dumpMeta = loader.getMetaData(() -> fs.openAsInputStream(dump), DumpFormatSelector::decompress);
        SizeMeta sizeMeta = dumpMeta.sizeMeta();
        if (sizeMeta == null) {
            return;
        }
        final var dumpSize = ByteUnit.bytes(sizeMeta.bytes());
        final var conservativeSize = ByteUnit.bytes((long) (dumpSize * CONSERVATIVE_DISK_SPACE_SCALAR));
        if (conservativeSize >= usableSpaceHint) {
            throw new FileSystemException(
                    ("Cannot extract the %s {size: %s} unless there is at least %s of disk space available; "
                                    + "this can be overridden by enabling <force>.")
                            .formatted(
                                    name(),
                                    ByteUnit.bytesToString(dumpSize),
                                    ByteUnit.bytesToString(conservativeSize)));
        }
    }
}
