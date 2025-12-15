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
package org.neo4j.storageengine.api;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

/**
 * Lazy version of {@link EagerValueIndexEntryUpdate}.
 * <p/>
 * This class is not thread-safe. If so desired, its ownership can be transferred to
 * other threads, but only in a way that ensures safe publication.
 */
public final class LazyValueIndexEntryUpdate extends IndexEntryUpdate implements ValueIndexEntryUpdate {

    /**
     * A {@link Supplier} that caches the value returned from the delegate supplier
     * upon the first call to {@link #get()}.
     * @param <T>
     */
    private static final class CachingSupplier<T> implements Supplier<T> {
        private Supplier<T> delegate;
        private T value;
        // Extra boolean since delegate can return null
        private boolean isValueSet = false;

        public CachingSupplier(Supplier<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T get() {
            if (!isValueSet) {
                value = delegate.get();
                isValueSet = true;
                // Release reference to delegate to allow it to be GC'ed
                delegate = null;
            }
            return value;
        }
    }

    /**
     * Adds {@code equals}, {@code hashCode} and {@code toString} to a {@link Supplier} of {@link Value}.
     * Moreover, caches the value returned from the delegate supplier upon the first call to {@link #get()}.
     */
    public static final class ValueSupplier implements Supplier<Value> {

        public static final ValueSupplier NULL_SUPPLIER = constant(null);

        public static ValueSupplier constant(Value value) {
            return new ValueSupplier(() -> value);
        }

        private Supplier<Value> supplier;

        public ValueSupplier(Supplier<Value> supplier) {
            this.supplier = new CachingSupplier<>(supplier);
        }

        @Override
        public Value get() {
            return supplier.get();
        }

        /**
         * {@inheritDoc}
         * <p/>
         * Note that calling this method will compute the lazy value held by this supplier.
         */
        @Override
        public String toString() {
            return "%s(%s)".formatted(ValueSupplier.class.getSimpleName(), get());
        }

        /**
         * {@inheritDoc}
         * <p/>
         * Note that calling this method will compute the lazy value held by this supplier.
         */
        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ValueSupplier that = (ValueSupplier) o;
            // To have a reliable equals and hashCode, we need to materialize the value.
            // The assumption is that equals and hashCode are not called in production code.
            return Objects.equals(get(), that.get());
        }

        /**
         * {@inheritDoc}
         * <p/>
         * Note that calling this method will compute the lazy value held by this supplier.
         */
        @Override
        public int hashCode() {
            // To have a reliable equals and hashCode, we need to materialize the value.
            // The assumption is that equals and hashCode are not called in production code.
            return Objects.hashCode(get());
        }
    }

    private ValueSupplier[] before;
    private ValueSupplier[] values;

    private LazyValueIndexEntryUpdate(
            long entityId, IndexDescriptor indexKey, UpdateMode updateMode, ValueSupplier[] values) {
        this(entityId, indexKey, updateMode, null, values);
    }

    private LazyValueIndexEntryUpdate(
            long entityId,
            IndexDescriptor indexKey,
            UpdateMode updateMode,
            ValueSupplier[] before,
            ValueSupplier[] values) {
        super(entityId, indexKey, updateMode);
        EagerValueIndexEntryUpdate.validateValuesLength(indexKey(), before, values);

        this.before = before;
        this.values = values;
    }

    public static LazyValueIndexEntryUpdate add(long entityId, IndexDescriptor indexKey, ValueSupplier... values) {
        return new LazyValueIndexEntryUpdate(entityId, indexKey, UpdateMode.ADDED, values);
    }

    public static LazyValueIndexEntryUpdate remove(long entityId, IndexDescriptor indexKey, ValueSupplier... values) {
        return new LazyValueIndexEntryUpdate(entityId, indexKey, UpdateMode.REMOVED, values);
    }

    public static LazyValueIndexEntryUpdate change(
            long entityId, IndexDescriptor indexKey, ValueSupplier before, ValueSupplier after) {
        return new LazyValueIndexEntryUpdate(
                entityId, indexKey, UpdateMode.CHANGED, new ValueSupplier[] {before}, new ValueSupplier[] {after});
    }

    public static LazyValueIndexEntryUpdate change(
            long entityId, IndexDescriptor indexKey, ValueSupplier[] before, ValueSupplier[] after) {
        return new LazyValueIndexEntryUpdate(entityId, indexKey, UpdateMode.CHANGED, before, after);
    }

    @Override
    public Value[] values() {
        Value[] vals = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            vals[i] = values[i].get();
        }
        return vals;
    }

    @Override
    public Value[] beforeValues() {
        if (before == null) {
            throw new UnsupportedOperationException("beforeValues is only valid for `UpdateMode.CHANGED");
        }
        Value[] vals = new Value[before.length];
        for (int i = 0; i < before.length; i++) {
            vals[i] = before[i].get();
        }
        return vals;
    }

    /**
     * {@inheritDoc}
     * </p>
     * Note that calling this method will compute all lazy values held by this update.
     */
    @Override
    public long roughSizeOfUpdate() {
        return EagerValueIndexEntryUpdate.heapSizeOf(values())
                + (updateMode() == UpdateMode.CHANGED ? EagerValueIndexEntryUpdate.heapSizeOf(beforeValues()) : 0);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Note that calling this method will compute all or some lazy values held by this update.
     */
    @Override
    protected boolean valueEquals(IndexEntryUpdate o) {
        if (!(o instanceof LazyValueIndexEntryUpdate that)) {
            return false;
        }
        return Arrays.equals(before, that.before) && Arrays.equals(values, that.values);
    }

    /**
     * {@inheritDoc}
     * </p>
     * Note that calling this method will compute all lazy values held by this update.
     */
    @Override
    protected int valueHash() {
        return Objects.hash(Arrays.hashCode(before), Arrays.hashCode(values));
    }

    /**
     * {@inheritDoc}
     * </p>
     * Note that calling this method will compute all lazy values held by this update.
     */
    @Override
    protected String valueToString() {
        return "beforeValues=%s, values=%s".formatted(Arrays.toString(before), Arrays.toString(values));
    }

    /**
     * {@inheritDoc}
     * </p>
     * Note that calling this method will compute all lazy values held by this update.
     */
    @Override
    public String toString() {
        return "LazyValueIndexEntryUpdate{entity=%d, before=%s, values=%s, updateMode=%s, index:%s}"
                .formatted(getEntityId(), Arrays.toString(before), Arrays.toString(values), updateMode(), indexKey());
    }

    @Override
    public IndexEntryUpdate withEntityId(long entityId) {
        return new LazyValueIndexEntryUpdate(entityId, indexKey(), updateMode(), before, values);
    }

    @Override
    public IndexEntryUpdate eagerly() {
        return new EagerValueIndexEntryUpdate(
                getEntityId(), indexKey(), updateMode(), before == null ? null : beforeValues(), values());
    }
}
