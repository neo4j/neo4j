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
package org.neo4j.batchimport.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.batchimport.api.input.Input;

/**
 * Imports graph data given as {@link Input}.
 */
public interface BatchImporter extends Closeable {

    /**
     * Levels of hardware validation that the importer should perform
     */
    enum HardwareValidation {
        NONE,
        INFORMATION,
        WARNING,
        STRICT
    }

    void doDryRun(Input input, PrintStream output) throws IOException;

    void doImport(Input input) throws IOException;

    default void doSkidbladnirImport(
            Input input, Charset encoding, Map<Set<String>, List<FileGroup>> nodeFileGroupsByAdditionalLabels)
            throws IOException {
        throw new UnsupportedOperationException("Skidbladnir import is not supported like this.");
    }

    @Override
    default void close() throws IOException {}
}
