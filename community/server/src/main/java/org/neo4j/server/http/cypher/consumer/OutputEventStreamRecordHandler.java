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
package org.neo4j.server.http.cypher.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.neo4j.bolt.protocol.common.fsm.response.RecordHandler;
import org.neo4j.server.http.cypher.OutputEventStream;
import org.neo4j.server.http.cypher.TransactionIndependentValueMapper;
import org.neo4j.values.AnyValue;

public class OutputEventStreamRecordHandler implements RecordHandler {
    private final List<String> fieldNames;
    private final OutputEventStream outputEventStream;
    private final Supplier<Map<String, Object>> resultsSupplier;
    private final TransactionIndependentValueMapper valueMapper;

    private Map<String, Object> results;
    private int fieldIndex;

    OutputEventStreamRecordHandler(
            List<String> fieldNames,
            OutputEventStream outputEventStream,
            TransactionIndependentValueMapper valueMapper) {
        this(fieldNames, outputEventStream, valueMapper, HashMap::new);
    }

    protected OutputEventStreamRecordHandler(
            List<String> fieldNames,
            OutputEventStream outputEventStream,
            TransactionIndependentValueMapper valueMapper,
            Supplier<Map<String, Object>> resultsSupplier) {
        this.fieldNames = fieldNames;
        this.outputEventStream = outputEventStream;
        this.valueMapper = valueMapper;
        this.resultsSupplier = resultsSupplier;
        this.results = resultsSupplier.get();
    }

    @Override
    public void onBegin() {
        fieldIndex = 0;
        results = resultsSupplier.get();
    }

    @Override
    public void onField(AnyValue value) {
        // we need to map the "AnyValue" type back to the standard graph types expected by the HTTP serialization
        // mechanism
        results.put(fieldNames.get(fieldIndex++), value.map(valueMapper));
    }

    @Override
    public void onCompleted() {
        outputEventStream.writeRecord(this.fieldNames, results::get);
    }

    @Override
    public void onFailure() {}
}
