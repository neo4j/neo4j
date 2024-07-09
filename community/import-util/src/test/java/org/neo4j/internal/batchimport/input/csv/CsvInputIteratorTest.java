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
package org.neo4j.internal.batchimport.input.csv;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.batchimport.api.input.Collector.EMPTY;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.Header.NO_MONITOR;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.MultiReadable;
import org.neo4j.csv.reader.Readables;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;

class CsvInputIteratorTest {
    @Test
    void shouldAutoSkipHeadersIfExists() throws IOException {
        // given
        CharReadable stream = readableOverFiles(List.of(
                List.of(":ID,:LABEL", "1,foo", "2,bar"),
                List.of(":ID,:LABEL,prop", "3,foo", "4,bar"),
                List.of(":ID,prop:int", "5,1", "6,2")));
        Configuration config = COMMAS;
        try (CsvInputIterator iterator = new CsvInputIterator(
                stream,
                e -> e,
                defaultFormatNodeFileHeader(),
                IdType.ACTUAL,
                config,
                new Groups(),
                EMPTY,
                new Extractors(config.arrayDelimiter()),
                0,
                true,
                NO_MONITOR)) {
            CsvInputChunkProxy chunk = new CsvInputChunkProxy();
            InputEntity visitor = new InputEntity();
            long nextExpectedId = 1;
            while (iterator.next(chunk)) {
                while (chunk.next(visitor)) {
                    assertThat(visitor.longId).isEqualTo(nextExpectedId++);
                }
            }
            assertThat(nextExpectedId).isEqualTo(7);
        }
    }

    private CharReadable readableOverFiles(List<List<String>> data) {
        MutableInt counter = new MutableInt();
        CharReadable[] sources = data.stream()
                .map(contents -> reader(contents, counter.getAndIncrement()))
                .toArray(CharReadable[]::new);
        return new MultiReadable(Readables.iterator(r -> r, sources));
    }

    private static CharReadable reader(List<String> contents, int id) {
        String data = StringUtils.join(contents, format("%n"));
        return Readables.wrap("source_" + id, data);
    }
}
