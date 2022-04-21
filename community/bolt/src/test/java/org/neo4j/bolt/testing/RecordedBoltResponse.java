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
package org.neo4j.bolt.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.messaging.BoltResponseMessage;
import org.neo4j.values.AnyValue;

public class RecordedBoltResponse {
    private final List<AnyValue[]> records = new ArrayList<>();
    private final Map<String, AnyValue> metadata = new HashMap<>();
    private BoltResponseMessage response;

    void addFields(AnyValue[] fields) {
        records.add(fields);
    }

    void addMetadata(String key, AnyValue value) {
        metadata.put(key, value);
    }

    public BoltResponseMessage message() {
        return response;
    }

    public void setResponse(BoltResponseMessage message) {
        this.response = message;
    }

    public boolean hasMetadata(String key) {
        return metadata.containsKey(key);
    }

    public AnyValue metadata(String key) {
        return metadata.get(key);
    }

    public void assertRecord(int index, AnyValue... values) {
        assertThat(index).isLessThan(records.size());
        assertArrayEquals(records.get(index), values);
    }

    public List<AnyValue[]> records() {
        return new ArrayList<>(records);
    }

    public AnyValue singleValueRecord() {
        var records = records();
        assertThat(records.size()).isEqualTo(1);
        var values = records.get(0);
        assertThat(values.length).isEqualTo(1);
        return values[0];
    }

    @Override
    public String toString() {
        return "RecordedBoltResponse{" + "records=" + records + ", response=" + response + ", metadata=" + metadata
                + '}';
    }
}
