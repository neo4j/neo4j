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
package org.neo4j.kernel.impl.api;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexReader;

/**
 * Cache which maps IndexDescriptors to IndexReaders. This is intended for reusing IndexReaders during a transaction.
 */
public class IndexReaderCache<T extends IndexReader> {
    private final Map<IndexDescriptor, T> indexReaders;
    private final ThrowingFunction<IndexDescriptor, T, IndexNotFoundKernelException> indexSupplier;

    public IndexReaderCache(ThrowingFunction<IndexDescriptor, T, IndexNotFoundKernelException> indexSupplier) {
        this.indexSupplier = indexSupplier;
        this.indexReaders = new HashMap<>();
    }

    public T getOrCreate(IndexDescriptor descriptor) throws IndexNotFoundKernelException {
        var reader = indexReaders.get(descriptor);
        if (reader == null) {
            reader = indexSupplier.apply(descriptor);
            indexReaders.put(descriptor, reader);
        }
        return reader;
    }

    public void close() {
        if (indexReaders.isEmpty()) {
            return;
        }

        for (IndexReader indexReader : indexReaders.values()) {
            indexReader.close();
        }
        indexReaders.clear();
    }
}
