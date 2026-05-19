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
package org.neo4j.batchimport.api.input;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A group of files that have been specified together in one command line argument.
 * E.g. `--nodes file1,file2,file3` would be represented as one FileGroup.
 * They are internally viewed as a single stream of data. The first file must contain the header.
 * Each file is assigned a global id.
 */
public record FileGroup(NumberedFile... files) {
    /**
     * @return the number of files in the group
     */
    public int fileCount() {
        return files.length;
    }

    /**
     * @return the file paths as a {@link Stream}
     */
    public Stream<Path> streamPaths() {
        return Arrays.stream(files).map(NumberedFile::path);
    }

    public record NumberedFile(int globalId, Path path) {}
}
