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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.file.Path;
import java.util.Objects;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.util.VisibleForTesting;

/**
 * Instances of this class act as individual factories for {@link StoreChannel}s single store files.
 *
 * They are primarily used by components trying to send files over the network.
 */
public final class StoreResource {
    private final Path path;
    private final String relativePath;
    private final FileSystemAbstraction fs;

    public StoreResource(StoreFileMetadata storeFileMetadata, DatabaseLayout dbLayout, FileSystemAbstraction fs) {
        this(storeFileMetadata.path(), relativeFilePath(storeFileMetadata, dbLayout), fs);
    }

    @VisibleForTesting
    public StoreResource(Path path, String relativePath, FileSystemAbstraction fs) {
        this.path = path;
        this.relativePath = relativePath;
        this.fs = fs;
    }

    private static String relativeFilePath(StoreFileMetadata storeFileMetadata, DatabaseLayout dbLayout) {
        return dbLayout.databaseDirectory().relativize(storeFileMetadata.path()).toString();
    }

    /**
     * @return a new {@link ByteChannel} for this resource's store file.
     */
    public StoreChannel open() throws IOException {
        return fs.read(path);
    }

    /**
     * @see DatabaseLayout#databaseDirectory()
     * @return a string representation of the {@link Path} for this resource's store file, relative to the root of the database which the file belongs to.
     */
    public String relativePath() {
        return relativePath;
    }

    /**
     * @return the path of this resource's store file
     */
    public Path path() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoreResource that = (StoreResource) o;
        return Objects.equals(path, that.path) && Objects.equals(relativePath, that.relativePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, relativePath);
    }

    @Override
    public String toString() {
        return "StoreResource{path='" + relativePath + "'}";
    }
}
