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
package org.neo4j.values.virtual;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ValueRepresentation;

public abstract class ListValueBuilder {
    /**
     * @return a collector for {@link ListValue}s
     */
    public static Collector<AnyValue, ?, ListValue> collector() {
        return LIST_VALUE_COLLECTOR;
    }

    /**
     * Start building a list of known size
     * @param size the final size of the list
     * @return a new builder
     */
    public static ListValueBuilder newListBuilder(int size) {
        return new FixedSizeListValueBuilder(size);
    }

    /**
     * Start building a list of unknown size
     * @return a new builder
     */
    public static ListValueBuilder newListBuilder() {
        return new UnknownSizeListValueBuilder();
    }

    protected ListValueBuilder() {
        valueRepresentation = ValueRepresentation.ANYTHING;
    }

    protected long estimatedHeapSize;
    protected ValueRepresentation valueRepresentation;

    public final void add(AnyValue value) {
        estimatedHeapSize += value.estimatedHeapUsage();
        valueRepresentation = valueRepresentation.coerce(value.valueRepresentation());
        internalAdd(value);
    }

    public abstract ListValue build();

    protected abstract void internalAdd(AnyValue value);

    private static class FixedSizeListValueBuilder extends ListValueBuilder {
        private final AnyValue[] values;
        private int index;

        private FixedSizeListValueBuilder(int size) {
            super();
            this.values = new AnyValue[size];
        }

        @Override
        public ListValue build() {
            return new ListValue.ArrayListValue(values, estimatedHeapSize, valueRepresentation);
        }

        @Override
        public void internalAdd(AnyValue value) {
            values[index++] = value;
        }
    }

    private static final long ARRAY_LIST_SHALLOW_SIZE = shallowSizeOfInstance(ArrayList.class);

    private static class UnknownSizeListValueBuilder extends ListValueBuilder {
        private final List<AnyValue> values = new ArrayList<>();

        UnknownSizeListValueBuilder() {
            super();
            estimatedHeapSize += ARRAY_LIST_SHALLOW_SIZE;
        }

        public UnknownSizeListValueBuilder combine(UnknownSizeListValueBuilder rhs) {
            values.addAll(rhs.values);
            estimatedHeapSize += rhs.estimatedHeapSize;
            return this;
        }

        @Override
        public ListValue build() {
            return new ListValue.JavaListListValue(values, estimatedHeapSize, valueRepresentation);
        }

        @Override
        public void internalAdd(AnyValue value) {
            values.add(value);
        }
    }

    private static final Collector<AnyValue, UnknownSizeListValueBuilder, ListValue> LIST_VALUE_COLLECTOR =
            new Collector<>() {
                @Override
                public Supplier<UnknownSizeListValueBuilder> supplier() {
                    return UnknownSizeListValueBuilder::new;
                }

                @Override
                public BiConsumer<UnknownSizeListValueBuilder, AnyValue> accumulator() {
                    return ListValueBuilder::add;
                }

                @Override
                public BinaryOperator<UnknownSizeListValueBuilder> combiner() {
                    return UnknownSizeListValueBuilder::combine;
                }

                @Override
                public Function<UnknownSizeListValueBuilder, ListValue> finisher() {
                    return UnknownSizeListValueBuilder::build;
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return Collections.emptySet();
                }
            };
}
