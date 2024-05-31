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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles.DEFAULT_FILENAME_FILTER;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.kernel.impl.storemigration.legacystore.v43.Legacy43Store;
import org.neo4j.test.Unzip;

public final class MigrationTestUtils {
    private MigrationTestUtils() {}

    public static void prepareSampleLegacyDatabase(
            RecordFormats format, FileSystemAbstraction fs, RecordDatabaseLayout databaseLayout, Path prepareDirectory)
            throws IOException {
        if (Files.notExists(prepareDirectory)) {
            throw new IllegalArgumentException("bad prepare directory");
        }
        Path source = findFormatStoreDirectoryForVersion(format, prepareDirectory);
        Path dbDirectory = databaseLayout.databaseDirectory();
        Path txDirectory = databaseLayout.getTransactionLogsDirectory();
        prepareDirectory(fs, dbDirectory);
        prepareDirectory(fs, txDirectory);

        fs.copyRecursively(source, dbDirectory);
        Path[] logFiles = fs.listFiles(dbDirectory, DEFAULT_FILENAME_FILTER);
        for (Path logFile : logFiles) {
            fs.moveToDirectory(logFile, txDirectory);
        }
    }

    private static void prepareDirectory(FileSystemAbstraction fs, Path destination) throws IOException {
        fs.deleteRecursively(destination);
        fs.mkdirs(destination);
    }

    static Path findFormatStoreDirectoryForVersion(RecordFormats format, Path targetDir) throws IOException {
        if (StandardV4_3.RECORD_FORMATS.name().equals(format.name())) {
            return find43FormatStoreDirectory(targetDir);
        } else {
            throw new IllegalArgumentException("Unknown version");
        }
    }

    private static Path find43FormatStoreDirectory(Path targetDir) throws IOException {
        return Unzip.unzip(Legacy43Store.class, "upgradeTest43Db.zip", targetDir);
    }
}
