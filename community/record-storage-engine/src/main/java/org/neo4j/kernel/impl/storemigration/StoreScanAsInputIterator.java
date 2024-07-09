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
package org.neo4j.kernel.impl.storemigration;

import static java.lang.Long.min;

import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * An {@link InputIterator} backed by a {@link RecordStore}, iterating over all used records.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}
 */
abstract class StoreScanAsInputIterator<RECORD extends AbstractBaseRecord> implements InputIterator {
    private final int batchSize;
    private final long highId;
    private long id;

    StoreScanAsInputIterator(RecordStore<RECORD> store) {
        this.batchSize = store.getRecordsPerPage() * 10;
        this.highId = store.getIdGenerator().getHighId();
    }

    @Override
    public void close() {}

    @Override
    public synchronized boolean next(InputChunk chunk) {
        if (id >= highId) {
            return false;
        }
        long startId = id;
        id = min(highId, startId + batchSize);
        ((StoreScanChunk<?>) chunk).initialize(startId, id);
        return true;
    }
}
