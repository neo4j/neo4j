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
import static org.apache.commons.lang3.StringUtils.join;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.batchimport.api.input.Collector.STRICT;
import static org.neo4j.batchimport.api.input.IdType.INTEGER;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;

import java.io.IOException;
import java.util.ArrayList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;

class EagerParserChunkerTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSkipHeaderIfThereIsOne(boolean includeHeaderInData) throws IOException {
        // given
        var groups = new Groups();
        var group = groups.getOrCreate("group");
        var extractors = new Extractors(';');
        var header = new Header(
                new Header.Entry(null, Type.ID, group, extractors.long_()),
                new Header.Entry("prop", Type.PROPERTY, null, extractors.string()));
        var data = new ArrayList<String>();
        if (includeHeaderInData) {
            data.add(":ID,prop");
        }
        data.add("1,v1");
        data.add("2,v2");
        try (var chunker = new EagerParserChunker(
                wrap(join(data, format("%n"))),
                INTEGER,
                header,
                STRICT,
                extractors,
                1_000,
                COMMAS,
                NO_DECORATOR,
                true)) {
            // when
            var chunk = new EagerCsvInputChunk();
            chunker.nextChunk(chunk);

            // then
            var entity = new InputEntity();
            assertThat(chunk.next(entity)).isTrue();
            assertThat(entity.objectId).isEqualTo(1L);
            assertThat(entity.propertyCount()).isEqualTo(1);
            assertThat(entity.propertyValue(0)).isEqualTo("v1");
            assertThat(chunk.next(entity)).isTrue();
            assertThat(entity.objectId).isEqualTo(2L);
            assertThat(entity.propertyCount()).isEqualTo(1);
            assertThat(entity.propertyValue(0)).isEqualTo("v2");
            assertThat(chunk.next(entity)).isFalse();
        }
    }
}
