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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.neo4j.storageengine.api.LazyValueIndexEntryUpdate.ValueSupplier.NULL_SUPPLIER;
import static org.neo4j.storageengine.api.LazyValueIndexEntryUpdate.ValueSupplier.constant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.LazyValueIndexEntryUpdate;
import org.neo4j.storageengine.api.LazyValueIndexEntryUpdate.ValueSupplier;
import org.neo4j.values.storable.Value;

class LazyValueIndexEntryUpdateTest extends AbstractValueIndexEntryUpdateTest {
    @Override
    LazyValueIndexEntryUpdate add(long entityId, IndexDescriptor indexKey, Value... values) {
        ValueSupplier[] suppliers = new ValueSupplier[values.length];
        for (int i = 0; i < values.length; i++) {
            suppliers[i] = constant(values[i]);
        }
        return LazyValueIndexEntryUpdate.add(entityId, indexKey, suppliers);
    }

    @Override
    LazyValueIndexEntryUpdate remove(long entityId, IndexDescriptor indexKey, Value... values) {
        ValueSupplier[] suppliers = new ValueSupplier[values.length];
        for (int i = 0; i < values.length; i++) {
            suppliers[i] = constant(values[i]);
        }
        return LazyValueIndexEntryUpdate.remove(entityId, indexKey, suppliers);
    }

    @Override
    LazyValueIndexEntryUpdate change(long entityId, IndexDescriptor indexKey, Value before, Value after) {
        return LazyValueIndexEntryUpdate.change(entityId, indexKey, constant(before), constant(after));
    }

    @Override
    LazyValueIndexEntryUpdate change(long entityId, IndexDescriptor indexKey, Value[] before, Value... after) {
        ValueSupplier[] beforeSuppliers = new ValueSupplier[before.length];
        for (int i = 0; i < before.length; i++) {
            beforeSuppliers[i] = constant(before[i]);
        }
        ValueSupplier[] afterSuppliers = new ValueSupplier[after.length];
        for (int i = 0; i < after.length; i++) {
            afterSuppliers[i] = constant(after[i]);
        }
        return LazyValueIndexEntryUpdate.change(entityId, indexKey, beforeSuppliers, afterSuppliers);
    }

    @Test
    void cachesSuppliers() {
        Supplier<Value> before = throwingOn2ndCallSupplier();
        Supplier<Value> after = throwingOn2ndCallSupplier();

        LazyValueIndexEntryUpdate a = LazyValueIndexEntryUpdate.change(
                0, forLabel(3, 4), new ValueSupplier(before), new ValueSupplier(after));
        a.values();
        assertThatCode(a::values).doesNotThrowAnyException();

        a.beforeValues();
        assertThatCode(a::beforeValues).doesNotThrowAnyException();
    }

    @Test
    void constantSuppliersMustBeSafeToShareBetweenThreads() throws Exception {
        int threads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            ValueSupplier shared = constant(singleValue);
            List<Future<Void>> results = new ArrayList<>(threads);
            for (int thread = 0; thread < threads; thread++) {
                long staggerMillis = thread * 50L;
                results.add(executor.submit(() -> {
                    Thread.sleep(staggerMillis);
                    assertThat(shared.get()).isEqualTo(singleValue);
                    assertThat(NULL_SUPPLIER.get()).isNull();
                    return null;
                }));
            }
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private Supplier<Value> throwingOn2ndCallSupplier() {
        return new Supplier<>() {
            private boolean calledOnce = false;

            @Override
            public Value get() {
                if (calledOnce) {
                    throw new IllegalStateException("Should not be called more than once");
                }
                calledOnce = true;
                return singleValue;
            }
        };
    }
}
