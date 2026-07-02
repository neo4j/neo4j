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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.neo4j.io.fs.FileSystemAbstraction;

public record StoreFile(Path baseSegment) {
    public StoreFile(Path baseSegment) {
        this.baseSegment = Objects.requireNonNull(baseSegment);
    }

    public Collection<Path> allSegments(FileSystemAbstraction fs) {
        if (!fs.fileExists(baseSegment)) {
            return List.of(baseSegment);
        }
        List<Path> segments = new ArrayList<>();
        segments.add(baseSegment);
        for (int index = 1; ; index++) {
            Path segment = segmentPath(baseSegment, index);
            if (!fs.fileExists(segment)) {
                return segments;
            }
            segments.add(segment);
        }
    }

    public void delete(FileSystemAbstraction fs) throws IOException {
        for (Path segment : allSegments(fs)) {
            fs.deleteFile(segment);
        }
    }

    public void rename(StoreFile destination, FileSystemAbstraction fs) throws IOException {
        Path destinationBase = destination.baseSegment();
        int index = 0;
        for (Path segment : allSegments(fs)) {
            fs.renameFile(segment, segmentPath(destinationBase, index));
            index++;
        }
    }

    public Path storeBaseFileName() {
        return baseSegment.getFileName();
    }

    public boolean exists(FileSystemAbstraction fs) {
        return fs.fileExists(baseSegment);
    }

    public long size(FileSystemAbstraction fileSystem) throws IOException {
        long size = 0;
        for (Path segment : allSegments(fileSystem)) {
            size += fileSystem.getFileSize(segment);
        }
        return size;
    }

    @Override
    public String toString() {
        return " path with base: " + baseSegment;
    }

    public static Path segmentPath(Path mainSegmentPath, int segmentIndex) {
        if (segmentIndex == 0) {
            return mainSegmentPath;
        }
        return mainSegmentPath.resolveSibling(mainSegmentPath.getFileName() + "." + segmentIndex);
    }
}
