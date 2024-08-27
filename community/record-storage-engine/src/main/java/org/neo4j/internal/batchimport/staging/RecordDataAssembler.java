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
package org.neo4j.internal.batchimport.staging;

import java.lang.reflect.Array;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.neo4j.function.Predicates;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

/**
 * Convenience for reading and assembling records w/ potential filtering into an array.
 *
 * @param <RECORD> type of {@link AbstractBaseRecord}.
 */
public class RecordDataAssembler<RECORD extends AbstractBaseRecord> {
    private final Supplier<RECORD> factory;
    private final Class<RECORD> klass;
    private final Predicate<RECORD> filter;
    private final RecordLoad loadMode;

    public RecordDataAssembler(Supplier<RECORD> factory, boolean forScan) {
        this(factory, Predicates.alwaysTrue(), forScan);
    }

    @SuppressWarnings("unchecked")
    public RecordDataAssembler(Supplier<RECORD> factory, Predicate<RECORD> filter, boolean forScan) {
        this.factory = factory;
        this.filter = filter;
        this.klass = (Class<RECORD>) factory.get().getClass();
        this.loadMode = forScan ? RecordLoad.LENIENT_CHECK : RecordLoad.CHECK;
    }

    @SuppressWarnings("unchecked")
    public RECORD[] newBatchObject(int batchSize) {
        Object array = Array.newInstance(klass, batchSize);
        for (int i = 0; i < batchSize; i++) {
            Array.set(array, i, factory.get());
        }
        return (RECORD[]) array;
    }

    public boolean append(
            RecordStore<RECORD> store,
            PageCursor cursor,
            RECORD[] array,
            long id,
            int index,
            MemoryTracker memoryTracker) {
        RECORD record = array[index];
        store.getRecordByCursor(id, record, loadMode, cursor, memoryTracker);
        return record.inUse() && filter.test(record);
    }

    public RECORD[] cutOffAt(RECORD[] array, int length) {
        for (int i = length; i < array.length; i++) {
            array[i].clear();
        }
        return array;
    }
}
