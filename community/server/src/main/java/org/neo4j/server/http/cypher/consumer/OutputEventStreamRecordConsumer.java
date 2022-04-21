/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher.consumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.server.http.cypher.OutputEventStream;
import org.neo4j.server.http.cypher.TransactionIndependentValueMapper;
import org.neo4j.values.AnyValue;

public class OutputEventStreamRecordConsumer implements BoltResult.RecordConsumer {
    private final BoltResult boltResult;
    private final OutputEventStream outputEventStream;
    private final Supplier<Map<String, Object>> resultsSupplier;
    private final TransactionIndependentValueMapper valueMapper;

    private Map<String, Object> results;
    private Map<String, AnyValue> metadataMap;
    private int fieldIndex;

    OutputEventStreamRecordConsumer(
            BoltResult boltResult, OutputEventStream outputEventStream, TransactionIndependentValueMapper valueMapper) {
        this(boltResult, outputEventStream, valueMapper, HashMap::new);
    }

    protected OutputEventStreamRecordConsumer(
            BoltResult boltResult,
            OutputEventStream outputEventStream,
            TransactionIndependentValueMapper valueMapper,
            Supplier<Map<String, Object>> resultsSupplier) {
        this.boltResult = boltResult;
        this.outputEventStream = outputEventStream;
        this.valueMapper = valueMapper;
        this.resultsSupplier = resultsSupplier;
        this.results = resultsSupplier.get();
        this.metadataMap = new HashMap<>();
    }

    @Override
    public void addMetadata(String key, AnyValue value) {
        metadataMap.put(key, value);
    }

    @Override
    public void beginRecord(int numberOfFields) throws IOException {
        fieldIndex = 0;
        results = resultsSupplier.get();
        metadataMap = new HashMap<>();
    }

    @Override
    public void consumeField(AnyValue value) throws IOException {
        // we need to map the "AnyValue" type back to the standard graph types expected by the HTTP serialization
        // mechanism
        results.put(boltResult.fieldNames()[fieldIndex], value.map(valueMapper));
        fieldIndex++;
    }

    @Override
    public void endRecord() throws IOException {
        outputEventStream.writeRecord(Arrays.asList(boltResult.fieldNames().clone()), results::get);
    }

    @Override
    public void onError() throws IOException {
        // dont think this is possible but throw an error here in case.
        throw new IOException("An error occurred whilst processing query results");
    }

    public Map<String, AnyValue> metadataMap() {
        return metadataMap;
    }
}
