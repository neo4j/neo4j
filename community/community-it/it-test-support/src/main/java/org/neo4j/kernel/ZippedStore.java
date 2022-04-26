/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

public interface ZippedStore {
    Path pathToZip() throws URISyntaxException;

    void unzip(Path targetDirectory) throws IOException;

    DbStatistics statistics();

    String name();

    default Path resolveDbPath(Path targetDirectory) {
        return targetDirectory.resolve("data/databases/neo4j");
    }

    default Path resolveTxPath(Path targetDirectory) {
        return targetDirectory.resolve("data/transactions/neo4j");
    }
}
