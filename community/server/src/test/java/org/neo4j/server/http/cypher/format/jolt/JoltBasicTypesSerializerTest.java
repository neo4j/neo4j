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
package org.neo4j.server.http.cypher.format.jolt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.http.cypher.format.jolt.v1.JoltV1Codec;
import org.neo4j.server.http.cypher.format.jolt.v2.JoltV2Codec;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

public class JoltBasicTypesSerializerTest {

    private static final ObjectMapper om = new ObjectMapper();

    public static Stream<? extends ObjectMapper> sparseMappers() {
        return Stream.of(new JoltV1Codec(false), new JoltV2Codec(false));
    }

    public static Stream<? extends ObjectMapper> strictMappers() {
        return Stream.of(new JoltV1Codec(true), new JoltV2Codec(true));
    }

    @ParameterizedTest
    @MethodSource("sparseMappers")
    void shouldUseSparseJSONString(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString("Hello, World");
        assertValidJSON(result);
        assertThat(result).isEqualTo("\"Hello, World\"");
    }

    @ParameterizedTest
    @MethodSource("sparseMappers")
    void shouldUseSparseJSONBoolean(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(true);
        assertValidJSON(result);
        assertThat(result).isEqualTo("true");
    }

    @ParameterizedTest
    @MethodSource("sparseMappers")
    void shouldUseSparseJSONList(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(List.of(1, 2, "3"));
        assertValidJSON(result);
        assertThat(result).isEqualTo("[1,2,\"3\"]");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeNull(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(null);
        assertValidJSON(result);
        assertThat(result).isEqualTo("null");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeInteger(ObjectMapper mapper) throws JsonProcessingException {

        var result = mapper.writeValueAsString(123);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"Z\":\"123\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeBoolean(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(true);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"?\":\"true\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeLongInsideInt32Range(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(123L);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"Z\":\"123\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeLongAboveInt32Range(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString((long) Integer.MAX_VALUE + 1);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"R\":\"2147483648\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeLongBelowInt32Range(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString((long) Integer.MIN_VALUE - 1);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"R\":\"-2147483649\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeDouble(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(42.23);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"R\":\"42.23\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeString(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString("Hello, World");
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"U\":\"Hello, World\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializePoint(ObjectMapper mapper) throws JsonProcessingException {
        var point = Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.994823, 55.612191);
        var result = mapper.writeValueAsString(point);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"@\":\"SRID=4326;POINT(12.994823 55.612191)\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeDuration(ObjectMapper mapper) throws JsonProcessingException {
        var duration = DurationValue.duration(Duration.ofDays(20));
        var result = mapper.writeValueAsString(duration);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"PT480H\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeLargeDuration(ObjectMapper mapper) throws JsonProcessingException {
        var durationString = "P3Y6M4DT12H30M5S";
        var durationValue = DurationValue.parse(durationString);
        var result = mapper.writeValueAsString(durationValue);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"" + durationString + "\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeDate(ObjectMapper mapper) throws JsonProcessingException {
        var dateString = "2020-08-25";
        var date = LocalDate.parse(dateString);
        var result = mapper.writeValueAsString(date);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"" + dateString + "\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeTime(ObjectMapper mapper) throws JsonProcessingException {
        var timeString = "12:52:58.513775";
        var time = LocalTime.parse(timeString);
        var result = mapper.writeValueAsString(time);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"" + timeString + "\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeOffsetTime(ObjectMapper mapper) throws JsonProcessingException {
        var offsetTimeString = "12:55:10.775607+01:00";
        var time = OffsetTime.parse(offsetTimeString);
        var result = mapper.writeValueAsString(time);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"" + offsetTimeString + "\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeLocalDateTime(ObjectMapper mapper) throws JsonProcessingException {
        var localDateTimeString = "2020-08-25T12:57:36.069665";
        var dateTime = LocalDateTime.parse(localDateTimeString);
        var result = mapper.writeValueAsString(dateTime);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"" + localDateTimeString + "\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeZonedDateTime(ObjectMapper mapper) throws JsonProcessingException {
        var zonedDateTimeString = "2020-08-25T13:03:39.11733+01:00[Europe/London]";
        var dateTime = ZonedDateTime.parse(zonedDateTimeString);
        var result = mapper.writeValueAsString(dateTime);
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"T\":\"" + zonedDateTimeString + "\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeLongArray(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(new Long[] {0L, 1L, 2L});
        assertValidJSON(result);
        assertThat(result).isEqualTo("[{\"Z\":\"0\"},{\"Z\":\"1\"},{\"Z\":\"2\"}]");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeByteArray(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(new byte[] {0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"#\":\"0001020304050608090A0B0C0D0E0F10\"}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeArrays(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(new String[] {"A", "B"});
        assertValidJSON(result);
        assertThat(result).isEqualTo("[{\"U\":\"A\"},{\"U\":\"B\"}]");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeHomogenousList(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(List.of(1, 2, 3));
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"[]\":[{\"Z\":\"1\"},{\"Z\":\"2\"},{\"Z\":\"3\"}]}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeHeterogeneousList(ObjectMapper mapper) throws JsonProcessingException {
        var result = mapper.writeValueAsString(List.of("A", 21, 42.3));
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"[]\":[{\"U\":\"A\"},{\"Z\":\"21\"},{\"R\":\"42.3\"}]}");
    }

    @ParameterizedTest
    @MethodSource("strictMappers")
    void shouldSerializeMap(ObjectMapper mapper) throws JsonProcessingException {
        // Treemap only created to have a stable iterator for a non flaky test ;)
        var result = mapper.writeValueAsString(new TreeMap<>(Map.of("name", "Alice", "age", 33)));
        assertValidJSON(result);
        assertThat(result).isEqualTo("{\"{}\":{\"age\":{\"Z\":\"33\"},\"name\":{\"U\":\"Alice\"}}}");
    }

    public static void assertValidJSON(final String json) {
        try {
            om.readTree(json);
        } catch (JsonProcessingException e) {
            fail("Invalid JSON: ", json);
        }
    }
}
