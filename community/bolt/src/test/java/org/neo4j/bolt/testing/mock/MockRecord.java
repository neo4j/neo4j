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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.values.AnyValue;

public record MockRecord(List<AnyValue> fields, Map<String, AnyValue> metadata) {

    public static Factory newFactory() {
        return new Factory();
    }

    public static MockRecord newInstance(Consumer<Factory> configurer) {
        var factory = newFactory();
        configurer.accept(factory);
        return factory.build();
    }

    public void consume(ResponseHandler responseHandler, RecordHandler recordHandler) throws IOException {
        recordHandler.onBegin();
        for (var field : this.fields) {
            recordHandler.onField(field);
        }
        for (var metadata : this.metadata.entrySet()) {
            responseHandler.onMetadata(metadata.getKey(), metadata.getValue());
        }
        recordHandler.onCompleted();
    }

    public static final class Factory {
        private final List<AnyValue> fields = new ArrayList<>();
        private final Map<String, AnyValue> metadata = new HashMap<>();

        private Factory() {}

        public MockRecord build() {
            return new MockRecord(this.fields, this.metadata);
        }

        public Factory withField(List<AnyValue> values) {
            this.fields.addAll(values);
            return this;
        }

        public Factory withField(AnyValue... values) {
            return this.withField(Arrays.asList(values));
        }

        public Factory withMetadata(String key, AnyValue value) {
            this.metadata.put(key, value);
            return this;
        }
    }
}
