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
package org.neo4j.bolt.protocol.common.message.decoder.util;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

class AuthenticationMetadataUtilsTest {

    @Test
    void shouldExtractAuthToken() {
        var originalMap = Map.<String, Object>of(
                "foo", "bar",
                "bar", "baz",
                "answer", 42);
        var ignoredFields = List.of("bar");

        var result = AuthenticationMetadataUtils.extractAuthToken(ignoredFields, originalMap);

        Assertions.assertThat(result)
                .isNotSameAs(originalMap)
                .doesNotContainKey("bar")
                .containsEntry("foo", "bar")
                .containsEntry("answer", 42);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConvertExtraMap() throws PackstreamReaderException {
        var builder = new MapValueBuilder();
        builder.add("scheme", Values.stringValue("basic"));
        builder.add("principal", Values.stringValue("alice"));
        builder.add("credentials", Values.stringValue("0m3g4s3cr37"));
        var meta = builder.build();

        var reader = (PackstreamValueReader<Connection>) Mockito.mock(PackstreamValueReader.class);

        Mockito.doReturn(meta).when(reader).readPrimitiveMap(Mockito.anyLong());

        var result = AuthenticationMetadataUtils.convertExtraMap(reader, 42);

        Mockito.verify(reader).readPrimitiveMap(42);

        Assertions.assertThat(result)
                .isNotNull()
                .isNotEmpty()
                .containsEntry("scheme", "basic")
                .containsEntry("principal", "alice")
                .hasEntrySatisfying("credentials", credentials -> Assertions.assertThat(credentials)
                        .isInstanceOf(byte[].class));
    }
}
