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
package org.neo4j.internal.batchimport.input;

import static java.lang.Long.min;

import java.io.IOException;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.kernel.impl.store.CommonAbstractStore;

public abstract class LenientInputChunkIterator implements InputIterator {
    private final int batchSize;
    private final long highId;
    private long id;

    LenientInputChunkIterator(CommonAbstractStore<?, ?> store) {
        this.batchSize = store.getRecordsPerPage() * 10;
        try {
            this.highId = store.getRecordsPerPage() * (store.getLastPageId() + 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized boolean next(InputChunk chunk) {
        if (id >= highId) {
            return false;
        }
        long startId = id;
        id = min(highId, startId + batchSize);
        ((LenientStoreInputChunk) chunk).setChunkRange(startId, id);
        return true;
    }

    @Override
    public void close() {}
}
