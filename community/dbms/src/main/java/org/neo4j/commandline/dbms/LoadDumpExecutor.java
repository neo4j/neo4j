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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.wrapIOException;
import static org.neo4j.io.fs.FileUtils.deleteDirectory;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.DecompressionSelector;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

public class LoadDumpExecutor {

    public static final String BACKUP_EXTENSION = ".backup";
    private final Config config;

    private final FileSystemAbstraction fs;

    private final PrintStream errorOutput;

    private final Loader loader;
    private final DecompressionSelector decompressionSelector;

    public LoadDumpExecutor(
            Config config,
            FileSystemAbstraction fs,
            PrintStream errorOutput,
            Loader loader,
            DecompressionSelector decompressionSelector) {
        this.config = config;
        this.fs = fs;
        this.errorOutput = errorOutput;
        this.loader = loader;
        this.decompressionSelector = decompressionSelector;
    }

    public void execute(DumpInput dumpInput, String database, boolean force) throws IOException {
        CursorContextFactory contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

        DatabaseLayout databaseLayout = Neo4jLayout.of(config).databaseLayout(database);
        fs.mkdirs(databaseLayout.databaseDirectory());
        fs.mkdirs(databaseLayout.getNeo4jLayout().transactionLogsRootDirectory());
        try (Closeable ignore = LockChecker.checkDatabaseLock(databaseLayout)) {
            deleteIfNecessary(databaseLayout, force);
            load(dumpInput, databaseLayout);
        } catch (FileLockException e) {
            throw new CommandFailedException(
                    "The database is in use. Stop database '" + database + "' and try again.", e);
        } catch (IOException e) {
            wrapIOException(e);
        } catch (CannotWriteException e) {
            throw new CommandFailedException("You do not have permission to load the database'" + database + "'.", e);
        }

        StoreVersionLoader.Result result = loader.getStoreVersion(fs, config, databaseLayout, contextFactory);
        if (result.migrationNeeded) {
            errorOutput.printf(
                    "The loaded database '%s' is not on a supported version (current format: %s introduced in %s). "
                            + "Use the 'neo4j-admin database migrate' command%n",
                    database,
                    result.currentFormat.getStoreVersionUserString(),
                    result.currentFormatIntroductionVersion);
        }
    }

    private void load(DumpInput dumpInput, DatabaseLayout databaseLayout) {
        try {
            loader.load(
                    databaseLayout,
                    false,
                    true,
                    decompressionSelector,
                    dumpInput.streamSupplier,
                    dumpInput.description);
        } catch (FileAlreadyExistsException e) {
            throw new CommandFailedException("Database already exists: " + databaseLayout.getDatabaseName(), e);
        } catch (AccessDeniedException e) {
            throw new CommandFailedException(
                    format("You do not have permission to load the database '%s'.", databaseLayout.getDatabaseName()),
                    e);
        } catch (IOException e) {
            wrapIOException(e);
        } catch (IncorrectFormat incorrectFormat) {
            throw new CommandFailedException("Not a valid Neo4j archive: " + dumpInput.description, incorrectFormat);
        }
    }

    private static void deleteIfNecessary(DatabaseLayout databaseLayout, boolean force) {
        try {
            if (force) {
                // we remove everything except our database lock
                deleteDirectory(
                        databaseLayout.databaseDirectory(), path -> !path.equals(databaseLayout.databaseLockFile()));
                FileUtils.deleteDirectory(databaseLayout.getTransactionLogsDirectory());
            }
        } catch (IOException e) {
            wrapIOException(e);
        }
    }

    public record DumpInput(ThrowingSupplier<InputStream, IOException> streamSupplier, String description) {}
}
