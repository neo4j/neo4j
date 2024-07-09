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
import org.neo4j.batchimport.api.input.Input;

public interface IncrementalBatchImporter extends BatchImporter, Closeable {
    @Override
    default void doImport(Input input) throws IOException {
        prepare(input);
        build(input);
        merge();
    }

    void prepare(Input input) throws IOException;

    void build(Input input) throws IOException;

    void merge() throws IOException;
}
