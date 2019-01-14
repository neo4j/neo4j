/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.index.internal.gbptree.GBPTree;

/**
 * Utilities for common operations around a {@link GBPTree}.
 */
public interface GBPTreeFileUtil
{
    /**
     * Deletes store file backing a {@link GBPTree}.
     * Undefined behaviour if storeFile is a directory.
     *
     * @param storeFile the {@link File} to delete.
     * @throws NoSuchFileException if the {@code storeFile} doesn't exist according to the {@code pageCache}.
     * @throws IOException on failure to delete existing {@code storeFile}.
     */
    void deleteFile( File storeFile ) throws IOException;

    /**
     * Deletes store file backing a {@link GBPTree}, if it exists according to the {@code pageCache}.
     * Undefined behaviour if storeFile is a directory.
     *
     * @param storeFile the {@link File} to delete.
     * @throws IOException on failure to delete existing {@code storeFile}.
     */
    void deleteFileIfPresent( File storeFile ) throws IOException;

    /**
     * Checks whether or not {@code storeFile} exists according to {@code pageCache}.
     * Undefined behaviour if storeFile is a directory.
     *
     * @param storeFile the {@link File} to check for existence.
     * @return {@code true} if {@code storeFile} exists according to {@code pageCache}, otherwise {@code false}.
     */
    boolean storeFileExists( File storeFile );

    /**
     * Creates the directory named by this abstract pathname, including any
     * necessary but nonexistent parent directories.
     *
     * @param dir the directory path to create
     * @throws IOException on failure to create directories.
     */
    void mkdirs( File dir ) throws IOException;
}
