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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.ValueIndexReader;

class IndexReaders implements Closeable {
    private final List<ValueIndexReader> indexReaders = new ArrayList<>();
    private final IndexDescriptor descriptor;
    private final KernelRead read;

    IndexReaders(IndexDescriptor descriptor, KernelRead read) {
        this.descriptor = descriptor;
        this.read = read;
    }

    ValueIndexReader createReader() throws IndexNotFoundKernelException {
        var indexReader = read.newValueIndexReader(descriptor);
        indexReaders.add(indexReader);
        return indexReader;
    }

    @Override
    public void close() {
        closeAllUnchecked(indexReaders);
    }
}
