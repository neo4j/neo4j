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
package org.neo4j.internal.batchimport;

import java.io.Closeable;
import java.io.IOException;

/**
 * Used by the {@link BatchImporter} to publish index entries as it imports node & relationship records.
 */
public interface IndexImporter extends Closeable {
    IndexImporter EMPTY_IMPORTER = new EmptyIndexImporter();

    /**
     * Called by the batch importer for entity that is imported
     * @param entity the id of the entity (node id/relationship id)
     * @param tokens the tokens associated with the entity (labels/relationship types)
     */
    void add(long entity, long[] tokens);

    class EmptyIndexImporter implements IndexImporter {
        @Override
        public void add(long entity, long[] tokens) {}

        @Override
        public void close() throws IOException {}
    }
}
