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
package org.neo4j.server.http.cypher.format.input.json;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.string.UTF8;

class StatementDeserializerTest {
    private final JsonFactory jsonFactory = new JsonFactory().setCodec(new ObjectMapper());

    @Test
    void shouldDeserializeSingleStatement() {
        // Given
        String json = createJsonFrom(
                map("statements", singletonList(map("statement", "Blah blah", "parameters", map("one", 12)))));

        // When
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(UTF8.encode(json)));

        // Then
        InputStatement stmt = de.read();
        assertNotNull(stmt);

        assertThat(stmt.statement()).isEqualTo("Blah blah");
        assertThat(stmt.parameters()).isEqualTo(map("one", 12));

        assertNull(de.read());
    }

    @Test
    void shouldRejectMapWithADifferentFieldBeforeStatement() {
        // NOTE: We don't really want this behaviour, but it's a symptom of keeping
        // streaming behaviour while moving the statement list into a map.

        String json = "{ \"timeout\" : 200, \"statements\" : [ { \"statement\" : \"ignored\", \"parameters\" : {}} ] }";

        assertYieldsErrors(
                json, "Unable to deserialize request. Expected first field to be 'statements', but was 'timeout'.");
    }

    @Test
    void shouldTotallyIgnoreInvalidJsonAfterStatementArrayHasFinished() {
        // NOTE: We don't really want this behaviour, but it's a symptom of keeping
        // streaming behaviour while moving the statement list into a map.

        // Given
        String json = "{ \"statements\" : [ { \"statement\" : \"Blah blah\", \"parameters\" : {\"one\" : 12}} ] "
                + "totally invalid json is totally ignored";

        // When
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(UTF8.encode(json)));

        // Then
        InputStatement stmt = de.read();
        assertNotNull(stmt);

        assertThat(stmt.statement()).isEqualTo("Blah blah");

        assertNull(de.read());
    }

    @Test
    void shouldIgnoreUnknownFields() {
        // Given
        String json = "{ \"statements\" : [ { \"a\" : \"\", \"b\" : { \"k\":1 }, \"statement\" : \"blah\" } ] }";

        // When
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(UTF8.encode(json)));

        // Then
        InputStatement stmt = de.read();
        assertNotNull(stmt);

        assertThat(stmt.statement()).isEqualTo("blah");

        assertNull(de.read());
    }

    @Test
    void shouldTakeParametersBeforeStatement() {
        // Given
        String json = "{ \"statements\" : [ { \"a\" : \"\", \"parameters\" : { \"k\":1 }, \"statement\" : \"blah\"}]}";

        // When
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(UTF8.encode(json)));

        // Then
        InputStatement stmt = de.read();
        assertNotNull(stmt);
        assertThat(stmt.statement()).isEqualTo("blah");
        assertThat(stmt.parameters()).isEqualTo(map("k", 1));

        assertNull(de.read());
    }

    @Test
    void shouldTreatEmptyInputStreamAsEmptyStatementList() {
        // When
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(EMPTY_BYTE_ARRAY));

        // Then
        assertNull(de.read());
    }

    @Test
    void shouldDeserializeMultipleStatements() {
        // Given
        String json = createJsonFrom(map(
                "statements",
                asList(
                        map("statement", "Blah blah", "parameters", map("one", 12)),
                        map("statement", "Blah bluh", "parameters", map("asd", singletonList("one, two"))))));

        // When
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(UTF8.encode(json)));

        // Then
        InputStatement stmt = de.read();
        assertNotNull(stmt);

        assertThat(stmt.statement()).isEqualTo("Blah blah");
        assertThat(stmt.parameters()).isEqualTo(map("one", 12));

        InputStatement stmt2 = de.read();
        assertNotNull(stmt2);

        assertThat(stmt2.statement()).isEqualTo("Blah bluh");
        assertThat(stmt2.parameters()).isEqualTo(map("asd", singletonList("one, two")));

        assertNull(de.read());
    }

    @Test
    void shouldNotThrowButReportErrorOnInvalidInput() {
        assertYieldsErrors(
                "{}",
                "Unable to " + "deserialize request. "
                        + "Expected [START_OBJECT, FIELD_NAME, START_ARRAY], "
                        + "found [START_OBJECT, END_OBJECT, null].");

        assertYieldsErrors(
                "{ \"statements\":\"WAIT WAT A STRING NOO11!\" }",
                "Unable to "
                        + "deserialize request. Expected [START_OBJECT, FIELD_NAME, START_ARRAY], found [START_OBJECT, "
                        + "FIELD_NAME, VALUE_STRING].");

        assertYieldsErrors(
                "[{]}",
                "Could not parse the incoming JSON",
                "Unexpected close marker ']': " + "expected '}' "
                        + "(for Object starting at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 2])\n "
                        + "at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 3]");

        assertYieldsErrors(
                "{ \"statements\" : \"ITS A STRING\" }",
                "Unable to deserialize request. " + "Expected [START_OBJECT, FIELD_NAME, START_ARRAY], "
                        + "found [START_OBJECT, FIELD_NAME, VALUE_STRING].");

        assertYieldsErrors(
                "{ \"statements\" : [ { \"statement\" : [\"dd\"] } ] }",
                "Could not map the incoming JSON",
                "Cannot deserialize value of type"
                        + " `java.lang.String` from Array value (token `JsonToken.START_ARRAY`)\n at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, "
                        + "column: 36]");

        assertYieldsErrors(
                "{ \"statements\" : [ { \"statement\" : \"stmt\", \"parameters\" : [\"AN ARRAY!!\"] } ] }",
                "Could not map the incoming JSON",
                "Cannot deserialize value of type"
                        + " `java.util.LinkedHashMap<java.lang.Object,java.lang.Object>` from Array value (token `JsonToken.START_ARRAY`)\n "
                        + "at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 59]");
    }

    private void assertYieldsErrors(String json, String... expectedErrorMessages) {
        StatementDeserializer de = new StatementDeserializer(jsonFactory, new ByteArrayInputStream(UTF8.encode(json)));

        try {
            while (de.read() != null) {}
            fail("An exception should have been thrown");
        } catch (InputFormatException e) {
            List<String> errorMessages = new ArrayList<>();
            errorMessages.add(e.getMessage());

            Throwable t = e;
            while (true) {
                t = t.getCause();

                if (t == null) {
                    break;
                }
                errorMessages.add(t.getMessage());
            }

            assertThat(errorMessages).isEqualTo(Arrays.asList(expectedErrorMessages));
        }
    }
}
