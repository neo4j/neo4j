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
package org.neo4j.bolt.testing.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;

public class MockResult {
    private final List<String> fields;
    private final List<MockRecord> records;
    private final Map<String, AnyValue> metadata;

    private Iterator<MockRecord> it;

    private MockResult(List<String> fields, List<MockRecord> records, Map<String, AnyValue> metadata) {
        this.fields = fields;
        this.records = records;
        this.metadata = metadata;
    }

    public static Factory newFactory() {
        return new Factory();
    }

    public static MockResult newInstance(Consumer<Factory> configurer) {
        var factory = newFactory();
        configurer.accept(factory);
        return factory.build();
    }

    public List<String> fieldNames() {
        return this.fields;
    }

    public List<MockRecord> records() {
        return this.records;
    }

    public boolean hasRemaining() {
        var it = this.it;

        if (it == null) {
            return !this.records.isEmpty();
        }
        return it.hasNext();
    }

    public void reset() {
        this.it = null;
    }

    private void iterate(long n, ResponseHandler responseHandler, ResultIterator func) throws IOException {
        var it = this.it;
        if (it == null) {
            this.it = it = this.records.iterator();
        }

        // this is technically not quite valid as we could potentially produce more results but this
        // covers our test cases nicely for now
        if (n == -1) {
            n = Integer.MAX_VALUE;
        }

        var recordHandler = responseHandler.onBeginStreaming(this.fieldNames());
        for (var i = 0; it.hasNext() && i < n; ++i) {
            var record = it.next();
            func.accept(record, responseHandler, recordHandler);
        }

        if (!it.hasNext()) {
            this.metadata.forEach((key, value) -> responseHandler.onMetadata(key, value));

            responseHandler.onMetadata("t_last", Values.longValue(42));
        } else {
            responseHandler.onMetadata("has_more", BooleanValue.TRUE);
        }

        responseHandler.onCompleteStreaming(it.hasNext());
    }

    public void consume(ResponseHandler handler, long n) throws IOException {
        this.iterate(n, handler, MockRecord::consume);
        handler.onSuccess();
    }

    public void discard(ResponseHandler handler, long n) throws IOException {
        this.iterate(n, handler, (record, responseHandler, recordHandler) -> {});
        handler.onSuccess();
    }

    public static final class Factory {
        private final List<String> fields = new ArrayList<>();
        private final List<MockRecord> records = new ArrayList<>();
        private final Map<String, AnyValue> metadata = new HashMap<>();

        private Factory() {}

        public MockResult build() {
            return new MockResult(
                    new ArrayList<>(this.fields), new ArrayList<>(this.records), new HashMap<>(this.metadata));
        }

        public Factory withField(String name) {
            this.fields.add(name);
            return this;
        }

        public Factory withField(String... name) {
            this.fields.addAll(Arrays.asList(name));
            return this;
        }

        public Factory withRecord(MockRecord record) {
            this.records.add(record);
            return this;
        }

        public Factory withRecord(Consumer<MockRecord.Factory> configurer) {
            return this.withRecord(MockRecord.newInstance(configurer));
        }

        public Factory withRecord(List<AnyValue> values) {
            return this.withRecord(MockRecord.newInstance(factory -> factory.withField(values)));
        }

        public Factory withRecord(AnyValue... values) {
            return this.withRecord(Arrays.asList(values));
        }

        public Factory withSimpleRecords(int n, IntFunction<AnyValue> function) {
            return this.withRecords(n, i -> List.of(function.apply(i)));
        }

        public Factory withRecords(int n, IntFunction<List<AnyValue>> function) {
            for (var i = 0; i < n; ++i) {
                this.withRecord(function.apply(i));
            }

            return this;
        }

        public Factory withMetadata(String key, AnyValue value) {
            this.metadata.put(key, value);
            return this;
        }
    }

    @FunctionalInterface
    private interface ResultIterator {

        void accept(MockRecord record, ResponseHandler responseHandler, RecordHandler recordHandler) throws IOException;
    }
}
