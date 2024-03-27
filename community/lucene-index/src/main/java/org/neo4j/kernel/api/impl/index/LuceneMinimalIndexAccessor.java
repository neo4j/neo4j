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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.values.storable.Value;

public class LuceneMinimalIndexAccessor<READER extends ValueIndexReader> implements MinimalIndexAccessor {
    private final IndexDescriptor indexDescriptor;
    private final DatabaseIndex<READER> index;
    private final boolean readOnly;

    public LuceneMinimalIndexAccessor(IndexDescriptor indexDescriptor, DatabaseIndex<READER> index, boolean readOnly) {
        this.indexDescriptor = indexDescriptor;
        this.index = index;
        this.readOnly = readOnly;
    }

    @Override
    public void drop() {
        if (readOnly) {
            throw new IllegalStateException("Cannot drop read-only index.");
        }
        index.drop();
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        return index.snapshotFiles();
    }

    @Override
    public Map<String, Value> indexConfig() {
        return indexDescriptor.getIndexConfig().asMap();
    }
}
