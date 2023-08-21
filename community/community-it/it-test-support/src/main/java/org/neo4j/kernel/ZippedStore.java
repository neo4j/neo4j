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
package org.neo4j.kernel;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.Unzip;

public interface ZippedStore {
    DbStatistics statistics();

    String name();

    String zipFileName();

    default void copyZipTo(FileSystemAbstraction fs, Path target) throws IOException {
        var url = requireNonNull(getClass().getResource(zipFileName()));
        try (var inputStream = url.openStream();
                var outputStream = fs.openAsOutputStream(target, false)) {
            inputStream.transferTo(outputStream);
        }
    }

    default void unzip(Path targetDirectory) throws IOException {
        Unzip.unzip(getClass(), zipFileName(), targetDirectory);
    }

    default Path resolveDbPath(Path targetDirectory) {
        return targetDirectory.resolve("data/databases/neo4j");
    }

    default Path resolveTxPath(Path targetDirectory) {
        return targetDirectory.resolve("data/transactions/neo4j");
    }
}
