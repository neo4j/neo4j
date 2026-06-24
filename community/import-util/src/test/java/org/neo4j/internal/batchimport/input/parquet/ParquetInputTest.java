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
package org.neo4j.internal.batchimport.input.parquet;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.batchimport.api.input.Collector.EMPTY;
import static org.neo4j.batchimport.api.input.IdType.ACTUAL;
import static org.neo4j.batchimport.api.input.IdType.INTEGER;
import static org.neo4j.batchimport.api.input.IdType.STRING;
import static org.neo4j.internal.helpers.ArrayUtil.union;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

import blue.strategic.parquet.ParquetWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.opentest4j.AssertionFailedError;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class ParquetInputTest {

    @Inject
    private TestDirectory directory;

    private final InputEntity visitor = new InputEntity();
    private Groups groups = new Groups();
    private InputChunk chunk;
    private InputIterator referenceData;
    private AtomicInteger parquetCounter = new AtomicInteger();
    private AtomicInteger headerCounter = new AtomicInteger();

    private static final ParquetMonitor MONITOR = new ParquetMonitor(System.out);

    @AfterEach
    void cleanup() throws IOException {
        parquetCounter.set(0);
        headerCounter.set(0);
        directory.cleanup();
    }

    @BeforeEach
    void resetGroups() {
        groups = new Groups();
        groups.getOrCreate(null);
    }

    @Test
    void shouldHandleParquetFileWithEmptyRowGroup() throws Exception {
        var fileUrl = Objects.requireNonNull(getClass().getResource("/parquet/empty_row_group.parquet"));
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldHandleParquetFilesIncludingEmptyRowGroup() throws Exception {
        var fileUrl = Objects.requireNonNull(getClass().getResource("/parquet/empty_row_group.parquet"));
        var nodeFileWithEmptyRowGroup = Path.of(fileUrl.toURI());

        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson"}));
        Input input = createParquetInput(
                Map.of(Set.of(), List.<Path[]>of(new Path[] {nodeFileWithEmptyRowGroup, nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("groupNames")
    void shouldProvideNodesFromParquetInput(String groupName) throws Exception {
        final var group = groupName == null ? Set.<String>of() : Set.of("");
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "HACKER"}));
        Input input = createParquetInput(
                Map.of(group, List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideNodesFromParquetInputWithHeaderFile() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path headerFile = createHeaderFile(
                List.of(":ID", "name", ":Label"),
                List.of("ignored-column-id", "ignored-column-name", "ignored-column-label"));

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void dontFailOnMultipleFilesWithHeadersForSameTypes() throws Exception {
        // GIVEN
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path headerFile1 = createHeaderFile(
                List.of(":ID", "name", ":Label"),
                List.of("ignored-column-id", "ignored-column-name", "ignored-column-label"));
        Path headerFile2 = createHeaderFile(
                List.of(":ID", "notaname", ":Label"),
                List.of("ignored-column-id", "ignored-column-name", "ignored-column-label"));

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {headerFile1, nodeFile1, headerFile2, nodeFile2})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertNextNode(nodes, 123L, properties("notaname", "Mattias Persson"), labels("HACKER", "USER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideNodesFromMultipleParquetInputsWithHeaderFile() throws Exception {
        // GIVEN
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {456L, "SomeoneElse", "USER"}));
        Path headerFile = createHeaderFile(
                List.of(":ID", "name", ":Label"),
                List.of("ignored-column-id", "ignored-column-name", "ignored-column-label"));

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {headerFile, nodeFile1, nodeFile2})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertNextNode(nodes, 456L, properties("name", "SomeoneElse"), labels("HACKER", "USER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideNodesFromMultipleParquetInputsAndDifferentColumnOrderingWithHeaderFile() throws Exception {
        // GIVEN
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id")),
                List.<Object[]>of(new Object[] {"USER", "SomeoneElse", 456L}));
        Path headerFile = createHeaderFile(
                List.of(":ID", "name", ":Label"),
                List.of("ignored-column-id", "ignored-column-name", "ignored-column-label"));

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {headerFile, nodeFile1, nodeFile2})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertNextNode(nodes, 456L, properties("name", "SomeoneElse"), labels("HACKER", "USER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideNodesFromParquetInputWithHeaderFileReducedColumns() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path headerFile =
                createHeaderFile(List.of(":ID", ":Label"), List.of("ignored-column-id", "ignored-column-label"));

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties(), labels("HACKER", "USER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldOnlyApplyHeadersInTheSameNodeGroup() throws Exception {
        // GIVEN
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":Label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "HACKER"}));

        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":Label")),
                List.<Object[]>of(new Object[] {456L, "SomeoneElse", "HACKER"}));

        Path headerFile = createHeaderFile(List.of(":ID", "new_name", ":Label"), List.of(":ID", "name", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(),
                        List.<Path[]>of(new Path[] {nodeFile1}),
                        Set.of("somethingElse"),
                        List.<Path[]>of(new Path[] {headerFile, nodeFile2})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            List<List<Object>> allProperties = new ArrayList<>();
            readNext(nodes);
            allProperties.add(List.copyOf(visitor.properties));
            readNext(nodes);
            allProperties.add(List.copyOf(visitor.properties));

            assertThat(allProperties)
                    .satisfiesExactlyInAnyOrder(
                            node1 -> {
                                assertThat(node1).containsExactly("name", "Mattias Persson");
                            },
                            node2 -> {
                                assertThat(node2).containsExactly("new_name", "SomeoneElse");
                            });
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void applyNodeHeaderFileAfterOccurrence() throws Exception {
        // GIVEN
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name")),
                List.<Object[]>of(new Object[] {456L, "Mattias Persson"}));
        Path headerFile = createHeaderFile(List.of(":ID", "name"), List.of(":ID", "ignored-column-name"));

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {nodeFile1, headerFile, nodeFile2})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("ignored-column-name", "Mattias Persson"), labels("HACKER"));
            assertNextNode(nodes, 456L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void failIfHeaderHasMoreThanTwoRows() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "HACKER"}));
        Path headerFile = directory.file("header.csv");
        try (var writer = new BufferedWriter(new FileWriter(headerFile.toFile()))) {
            writer.write(":ID,name,:Label");
            writer.newLine();
            writer.write("ignored-column-id,ignored-column-name,ignored-column-label");
            writer.newLine();
            writer.write("idkid,idkname,idklabel");
            writer.newLine();
        }

        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                        Map.of(),
                        INTEGER,
                        groups,
                        MONITOR))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The header is expected to have one or two lines");
    }

    @Test
    void failIfHeaderIsEmptyOrBlank() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":Label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "HACKER"}));
        Path headerFile = directory.file("header.csv");
        try (var writer = new BufferedWriter(new FileWriter(headerFile.toFile()))) {
            writer.newLine();
        }

        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                        Map.of(),
                        INTEGER,
                        groups,
                        MONITOR))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The header definition is empty");
    }

    @Test
    void failIfHeaderContainsUnknownColumns() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":Label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "HACKER"}));
        Path headerFile =
                createHeaderFile(List.of(":ID", "name", "lol", ":Label"), List.of(":ID", "name", "lol", ":Label"));

        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                        Map.of(),
                        INTEGER,
                        groups,
                        MONITOR))
                .isInstanceOf(InputException.class)
                .hasMessageStartingWith("Target column(s) '[lol]' from header cannot be found in");
    }

    @Test
    void shouldProvideNodesFromParquetInputWithSingleLineHeaderFile() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ignored-column-id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("ignored-column-label")),
                List.<Object[]>of(new Object[] {123L, "Mattias Persson", "USER"}));
        Path headerFile = createHeaderFile(List.of(":ID", "name", ":Label"), List.of());

        Input input = createParquetInput(
                Map.of(Set.of("HACKER"), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldStoreIdAsPropertyInSpecificValueTypeWithHeader() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("notid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notprop")),
                List.<Object[]>of(new Object[] {123, "val"}));
        Path headerFile = createHeaderFile(List.of("id:ID(new-group){id-type:int}", "prop"), List.of());
        try (var input = createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // then
            assertNextNode(nodes, groups.get("new-group"), 123, properties("id", 123, "prop", "val"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldStoreIdAsPropertyInSpecificValueTypeWithHeaderConverted() throws Exception {
        // Given a node file with an int column
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("notid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notprop")),
                List.<Object[]>of(new Object[] {123, "val"}));
        // And a header file that maps the int column to an ID property with string type
        Path headerFile = createHeaderFile(List.of("id:ID(new-group){id-type:string}", "prop"), List.of());

        // When processing the parquet file
        try (var input = createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // Then the id field is converted to String and stored as a property
            assertNextNode(nodes, groups.get("new-group"), "123", properties("id", "123", "prop", "val"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @MethodSource("listTypes")
    void shouldReadListTypes(String fileName, List<?> expectedList) throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/" + fileName);
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aList", expectedList, "name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @MethodSource("listTypes")
    void shouldReadListTypesWithHeader(String fileName, List<?> expectedList, String listMappedAs) throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/" + fileName);
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "aList:%s".formatted(listMappedAs), ":Label"),
                List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {headerFile, nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aList", expectedList, "name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadListTypesWithSingleEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/list_single.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aList", List.of("a"), "name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @MethodSource("emptyListTypes")
    void shouldReadListTypesWithEmptyList(String fileName, ArrayValue expectedEmptyArray) throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/empty_list/" + fileName);
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes, 123L, properties("aList", expectedEmptyArray, "name", "Dhru Devalia"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadListTypesWithNullList() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/list_null.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 456L, properties("name", "Dhru"), labels("REKCAH"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadMapTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    123L,
                    properties("aMap.a", "aa", "aMap.b", "bb", "name", "Mattias Persson"),
                    labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadNumericMapTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_numeric.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes, 123L, properties("aMap.a", 1L, "aMap.b", 23L, "name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadMultipleMapTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_multiple.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    123L,
                    properties(
                            "aMap.a",
                            "aa",
                            "aMap.b",
                            "bb",
                            "bMap.x",
                            "xx",
                            "bMap.y",
                            "yy",
                            "cMap.c",
                            "cc",
                            "name",
                            "Mattias Persson"),
                    labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadMapTypesWithNoEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_empty.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadMapTypesWithNullEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_null.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadMapTypesWithSingleEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_single.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aMap.x", "abcd", "name", "Mattias Persson"), labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldFailOnDuplicatedNamePrefix() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_duplicate_names.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        // WHEN/THEN
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR))
                .isInstanceOf(DuplicatedColumnException.class)
                .hasMessageContaining("map_duplicate_names.parquet");
    }

    @Test
    void shouldReadStructTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/struct.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        System.out.println(nodeFile.toAbsolutePath());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    123L,
                    properties("aStruct.a", "aa", "aStruct.b", "bb", "name", "Mattias Persson"),
                    labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldReadMultipleStructTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/struct_multiple.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        System.out.println(nodeFile.toAbsolutePath());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    123L,
                    properties(
                            "aStruct.a",
                            "aa",
                            "aStruct.b",
                            "bb",
                            "name",
                            "Dhru Devalia",
                            "bStruct.x",
                            "xx",
                            "bStruct.y",
                            12,
                            "cStruct.items",
                            List.of("foo", "bar", "baz")),
                    labels("HACKER"));
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @MethodSource("groupNames")
    void shouldProvideRelationshipsFromParquetInput(String groupName) throws Exception {
        // GIVEN
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("since")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));
        Input input = createParquetInput(
                Map.of(),
                Maps.mutable.of(groupName, List.<Path[]>of(new Path[] {relationshipFile})),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldProvideRelationshipsFromParquetInputWithHeaderFile() throws Exception {
        // GIVEN
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notstartid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notendid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("nottype"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("notsince")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));

        Path headerFile = createHeaderFile(
                List.of(":START_ID", ":END_ID", ":Type", "since"),
                List.of("notstartid", "notendid", "nottype", "notsince"));
        Input input = createParquetInput(
                Map.of(),
                Map.of("", List.<Path[]>of(new Path[] {headerFile, relationshipFile})),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldProvideRelationshipsFromParquetInputWithHeaderFileReducedColumns() throws Exception {
        // GIVEN
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notstartid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notendid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("nottype"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("notsince")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));

        Path headerFile = createHeaderFile(
                List.of(":START_ID", ":END_ID", ":Type"), List.of("notstartid", "notendid", "nottype"));
        Input input = createParquetInput(
                Map.of(),
                Map.of("", List.<Path[]>of(new Path[] {headerFile, relationshipFile})),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties());
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldOnlyApplyHeadersInTheSameRelationshipGroup() throws Exception {
        // GIVEN
        Path relationshipFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("notsince")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));
        Path relationshipFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notstartid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("notendid"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("nottype"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("notsince")),
                List.of(
                        new Object[] {"node3", "node33", "KNOWS", 1234567L},
                        new Object[] {"node4", "node44", "HACKS", 987654L}));

        Path headerFile = createHeaderFile(
                List.of(":START_ID", ":END_ID", ":Type", "since"),
                List.of("notstartid", "notendid", "nottype", "notsince"));
        Input input = createParquetInput(
                Map.of(),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile1}), "ignore_me", List.<Path[]>of(new Path[] {
                    headerFile, relationshipFile2
                })),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(
                    relationships,
                    List.of("node1", "node2", "node3", "node4"),
                    List.of("node2", "node10", "node33", "node44"),
                    List.of("KNOWS", "HACKS", "KNOWS", "HACKS"),
                    List.of(
                            properties("notsince", 1234567L),
                            properties("notsince", 987654L),
                            properties("since", 1234567L),
                            properties("since", 987654L)));
            assertNextRelationship(
                    relationships,
                    List.of("node1", "node2", "node3", "node4"),
                    List.of("node2", "node10", "node33", "node44"),
                    List.of("KNOWS", "HACKS", "KNOWS", "HACKS"),
                    List.of(
                            properties("notsince", 1234567L),
                            properties("notsince", 987654L),
                            properties("since", 1234567L),
                            properties("since", 987654L)));
            assertNextRelationship(
                    relationships,
                    List.of("node1", "node2", "node3", "node4"),
                    List.of("node2", "node10", "node33", "node44"),
                    List.of("KNOWS", "HACKS", "KNOWS", "HACKS"),
                    List.of(
                            properties("notsince", 1234567L),
                            properties("notsince", 987654L),
                            properties("since", 1234567L),
                            properties("since", 987654L)));
            assertNextRelationship(
                    relationships,
                    List.of("node1", "node2", "node3", "node4"),
                    List.of("node2", "node10", "node33", "node44"),
                    List.of("KNOWS", "HACKS", "KNOWS", "HACKS"),
                    List.of(
                            properties("notsince", 1234567L),
                            properties("notsince", 987654L),
                            properties("since", 1234567L),
                            properties("since", 987654L)));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void applyRelationshipHeaderAfterOccurrence() throws Exception {
        // GIVEN
        Path relationshipFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("notsince")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));
        Path relationshipFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("notsince")),
                List.of(
                        new Object[] {"node1", "node2", "KNOWS", 1234567L},
                        new Object[] {"node2", "node10", "HACKS", 987654L}));

        Path headerFile = createHeaderFile(
                List.of(":START_ID", ":END_ID", ":Type", "since"),
                List.of(":START_ID", ":END_ID", ":TYPE", "notsince"));
        Input input = createParquetInput(
                Map.of(),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile1, headerFile, relationshipFile2})),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("notsince", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("notsince", 987654L));
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldHandleMultipleInputGroups() throws Exception {
        // GIVEN multiple input groups, each with their own, specific, header
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("kills"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("health")),
                List.of(new Object[] {"1", "Jim", 10, 100}, new Object[] {"2", "Abathur", 0, 200}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("type")),
                List.of(new Object[] {"3", "zergling"}, new Object[] {"4", "csv"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of(),
                STRING,
                groups,
                MONITOR);
        // WHEN iterating over them, THEN the expected data should come out
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, "1", properties("name", "Jim", "kills", 10, "health", 100), labels());
            assertNextNode(nodes, "2", properties("name", "Abathur", "kills", 0, "health", 200), labels());
            assertNextNode(nodes, "3", properties("type", "zergling"), labels());
            assertNextNode(nodes, "4", properties("type", "csv"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideAdditiveLabels() throws Exception {
        // GIVEN
        String[] addedLabels = {"Two", "AddTwo"};
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {0, "First", ""}, new Object[] {1, "Second", "One"}, new Object[] {
                    2, "Third", "One;Two"
                }));
        Input input = createParquetInput(
                Map.of(Set.of(addedLabels), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 0L, properties("name", "First"), labels(addedLabels));
            assertNextNode(nodes, 1L, properties("name", "Second"), labels(union(new String[] {"One"}, addedLabels)));
            assertNextNode(nodes, 2L, properties("name", "Third"), labels(union(new String[] {"One"}, addedLabels)));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldProvideDefaultRelationshipType() throws Exception {
        // GIVEN
        String defaultType = "DEFAULT";
        String customType = "CUSTOM";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE")),
                List.of(new Object[] {0, 1, ""}, new Object[] {1, 2, customType}, new Object[] {2, 1, defaultType}));
        Input input = createParquetInput(
                Map.of(),
                Map.of(defaultType, List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                groups,
                MONITOR);

        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 0L, 1L, defaultType, emptyMap());
            assertNextRelationship(relationships, 1L, 2L, customType, emptyMap());
            assertNextRelationship(relationships, 2L, 1L, defaultType, emptyMap());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldAllowNodesWithoutIdHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {"Mattias", 1}, new Object[] {"Johan", 2}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, null, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldAllowSomeNodesToBeAnonymous() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {"abc", "Mattias", 1}, new Object[] {null, "Johan", 2}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldAllowNodesToBeAnonymousEvenIfIdHeaderIsNamed() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {"abc", "Mattias", 1}, new Object[] {null, "Johan", 2}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, groups, MONITOR);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("id", "abc", "name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotHaveIdSetAsPropertyIfIdHeaderEntryIsNamedForActualIds() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("level")),
                List.of(new Object[] {0, "Mattias", 1}, new Object[] {1, "Johan", 2}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), ACTUAL, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, 0L, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, 1L, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldIgnoreNullPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("extra")),
                List.of(new Object[] {0, "Mattias", null}, new Object[] {1, "Johan", "Additional"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldIgnoreEmptyPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("extra")),
                List.of(new Object[] {0, "Mattias", ""}, new Object[] {1, "Johan", "Additional"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParsePointPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point")),
                List.of(
                        new Object[] {0, "Mattias", "{x: 2.7, y:3.2 }"},
                        new Object[] {1, "Johan", " { height :0.01 ,longitude:5, latitude : -4.2 } "}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "point",
                            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.7, 3.2)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "name",
                            "Johan",
                            "point",
                            Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 5, -4.2, 0.01)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotParsePointPropertyValuesWithDuplicateKeys() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point")),
                List.<Object[]>of(
                        new Object[] {0, "Johan", " { height :0.01 ,longitude:5, latitude : -4.2, latitude : 4.2 } "}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);

        InputIterator nodes = input.nodes(EMPTY).iterator();
        try {
            assertThatThrownBy(() -> readNext(nodes)).isInstanceOf(InputException.class);
        } finally {
            assertFalse(readNext(nodes));
            nodes.close();
        }
    }

    @Test
    void shouldParsePointPropertyValuesWithCRSInHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point{crs:WGS-84-3D}")),
                List.<Object[]>of(new Object[] {0, "Johan", " { height :0.01 ,longitude:5, latitude : -4.2 } "}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Johan",
                            "point",
                            Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 5, -4.2, 0.01)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldUseHeaderInformationToParsePoint() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("point:Point{crs:WGS-84}")),
                List.<Object[]>of(new Object[] {0, "Johan", " { x :1 ,y:2 } "}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Johan", "point", Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseSimpleTypesDouble() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("someDouble")),
                List.of(new Object[] {0, "Mattias", 1.1d}, new Object[] {1, "Johan", 2.2d}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "someDouble", 1.1d), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "someDouble", 2.2d), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseSimpleTypesFloat() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("someDouble")),
                List.of(new Object[] {0, "Mattias", 1.1f}, new Object[] {1, "Johan", 2.2f}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "someDouble", 1.1f), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "someDouble", 2.2f), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDatePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("date:Date")),
                List.of(new Object[] {0, "Mattias", "2018-02-27"}, new Object[] {1, "Johan", "2018-03-01"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2018, 2, 27)), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "date", DateValue.date(2018, 3, 1)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDatePropertyIntegerValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("date:Date")),
                List.<Object[]>of(new Object[] {0, "Mattias", 13193}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2006, 2, 14)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealDatePropertyValue() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.dateType())
                                .named("date:Date")),
                List.<Object[]>of(new Object[] {0, "Mattias", 13193}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2006, 2, 14)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseFilesWithMixedTimeValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.dateType())
                                .named("date:Date")),
                List.<Object[]>of(new Object[] {0, "Mattias", 13193}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2006, 2, 14)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDateTimePropertyLongValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("date:LocalDateTime")),
                List.<Object[]>of(new Object[] {0, "Mattias", 1116975273000000L}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            // 2005-05-24 22:54:33 1116975273000000
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "date", LocalDateTimeValue.localDateTime(2005, 5, 24, 22, 54, 33, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealTimestampPropertyValuesNanos() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                                .named("date:DateTime")),
                List.<Object[]>of(new Object[] {0, "Mattias", 1752844932961528000L}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            // 2005-05-24 22:54:33 1116975273000000
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "date",
                            DateTimeValue.datetime(
                                    2025, 7, 18, 13, 22, 12, 961528000, ZoneId.of(ZoneOffset.UTC.getId()))),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealTimestampPropertyValuesMicros() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("date:LocalDateTime")),
                List.<Object[]>of(new Object[] {0, "Mattias", 1752844932961528L}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            // 2005-05-24 22:54:33 1116975273000000
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "date",
                            LocalDateTimeValue.localDateTime(2025, 7, 18, 13, 22, 12, 961528000)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealTimestampPropertyValuesMillis() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named("date:LocalDateTime")),
                List.<Object[]>of(new Object[] {0, "Mattias", 1752844932961L}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            // 2005-05-24 22:54:33 1116975273000000
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "date",
                            LocalDateTimeValue.localDateTime(2025, 7, 18, 13, 22, 12, 961000000)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", "13:37"}, new Object[] {1, "Johan", "16:20:01"}, new Object[] {
                    2, "Bob", "07:30-05:00"
                }));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", TimeValue.time(13, 37, 0, 0, "+00:00")), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(16, 20, 1, 0, "+00:00")), labels());
            assertNextNode(
                    nodes, 2L, properties("name", "Bob", "time", TimeValue.time(7, 30, 0, 0, "-05:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseNumericTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144072000L}, new Object[] {1, "Johan", 52397000000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144072000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealUTCTimePropertyValuesNanos() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144072000L}, new Object[] {1, "Johan", 52397000000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144072000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealUTCTimePropertyValuesMillis() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144}, new Object[] {1, "Johan", 52397000}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144000000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseRealUTCTimePropertyValuesMicros() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144072L}, new Object[] {1, "Johan", 52397000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144072000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValuesNanos() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144072000L}, new Object[] {1, "Johan", 52397000000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144072000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValuesMillis() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144}, new Object[] {1, "Johan", 52397000}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144000000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValuesMicros() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("time:Time")),
                List.of(new Object[] {0, "Mattias", 52397144072L}, new Object[] {1, "Johan", 52397000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", TimeValue.time(14, 33, 17, 144072000, "+00:00")),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(14, 33, 17, 0, "+00:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseLocalDateTimeWithoutUTCAdjustmentInMicros() throws Exception {
        var nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("datetime")),
                List.of(new Object[] {0, 52397144072L}, new Object[] {1, 52397000000L}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("datetime", LocalDateTimeValue.localDateTime(1970, 1, 1, 14, 33, 17, 144072000)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("datetime", LocalDateTimeValue.localDateTime(1970, 1, 1, 14, 33, 17, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseUUIDasString() throws Exception {
        var nodeFile = Path.of(getClass().getResource("/parquet/uuid.parquet").toURI());

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("myUUID", "ba576658-d01d-4858-94ff-a97f18be9608"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseLocalDateTimeWithoutUTCAdjustmentInMillis() throws Exception {
        var nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named("datetime")),
                List.of(new Object[] {0, 52397144L}, new Object[] {1, 52397000L}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("datetime", LocalDateTimeValue.localDateTime(1970, 1, 1, 14, 33, 17, 144000000)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("datetime", LocalDateTimeValue.localDateTime(1970, 1, 1, 14, 33, 17, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseSourceContainingDateAndTimeCorrectly() throws Exception {

        var headerPath =
                Path.of(getClass().getResource("/parquet/datetime_header.csv").toURI());
        var nodePath =
                Path.of(getClass().getResource("/parquet/datetime_data.parquet").toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {headerPath, nodePath})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNodeWithoutGroupAndIdCheck(
                    nodes,
                    properties(
                            "date",
                            DateValue.date(2025, 7, 15),
                            "datetime",
                            DateTimeValue.datetime(2025, 7, 15, 12, 0, 0, 0, "Z"),
                            "address",
                            "123 Main St",
                            "string",
                            "Alpha",
                            "epoch",
                            LocalDateTimeValue.localDateTime(2025, 7, 15, 6, 30, 0, 0),
                            "integer",
                            101),
                    labels());
            assertNextNodeWithoutGroupAndIdCheck(
                    nodes,
                    properties(
                            "date",
                            DateValue.date(2025, 7, 15),
                            "datetime",
                            DateTimeValue.datetime(2025, 7, 15, 12, 1, 0, 0, "Z"),
                            "address",
                            "456 Market Ave",
                            "string",
                            "Beta",
                            "epoch",
                            LocalDateTimeValue.localDateTime(2025, 7, 15, 7, 30, 0, 0),
                            "integer",
                            102),
                    labels());
            assertNextNodeWithoutGroupAndIdCheck(
                    nodes,
                    properties(
                            "date",
                            DateValue.date(2025, 7, 15),
                            "datetime",
                            DateTimeValue.datetime(2025, 7, 15, 12, 2, 0, 0, "Z"),
                            "address",
                            "789 Broadway Blvd",
                            "string",
                            "Gamma",
                            "epoch",
                            LocalDateTimeValue.localDateTime(2025, 7, 15, 8, 30, 0, 0),
                            "integer",
                            103),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDateRelatedTypesLikeDataImporter() throws Exception {

        var nodePath = Path.of(
                getClass().getResource("/parquet/temporal_types.parquet").toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodePath})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNodeWithoutGroupAndIdCheck(
                    nodes,
                    properties(
                            "c_time_millis",
                            LocalTimeValue.localTime(23, 59, 59, 999_000_000),
                            "c_time_millis_utc",
                            TimeValue.time(23, 59, 59, 999_000_000, ZoneOffset.UTC),
                            "c_time_micros",
                            LocalTimeValue.localTime(23, 59, 59, 999_999_000),
                            "c_time_micros_utc",
                            TimeValue.time(23, 59, 59, 999_999_000, ZoneOffset.UTC),
                            "c_time_nanos",
                            LocalTimeValue.localTime(23, 59, 59, 999_999_999),
                            "c_time_nanos_utc",
                            TimeValue.time(23, 59, 59, 999_999_999, ZoneOffset.UTC),
                            "c_timestamp_millis",
                            LocalDateTimeValue.localDateTime(1999, 1, 5, 22, 59, 59, 999_000_000),
                            "c_timestamp_millis_utc",
                            DateTimeValue.datetime(
                                    1999, 1, 5, 23, 59, 59, 999_000_000, ZoneId.of(ZoneOffset.UTC.getId())),
                            "c_timestamp_micros",
                            LocalDateTimeValue.localDateTime(1999, 1, 5, 22, 59, 59, 999_999_000),
                            "c_timestamp_micros_utc",
                            DateTimeValue.datetime(
                                    1999, 1, 5, 23, 59, 59, 999_999_000, ZoneId.of(ZoneOffset.UTC.getId())),
                            "c_timestamp_nanos",
                            LocalDateTimeValue.localDateTime(1999, 1, 5, 22, 59, 59, 999_999_999),
                            "c_timestamp_nanos_utc",
                            DateTimeValue.datetime(
                                    1999, 1, 5, 23, 59, 59, 999_999_999, ZoneId.of(ZoneOffset.UTC.getId())),
                            "c_date",
                            DateValue.date(1999, 1, 5)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseNullTemporalTypes() throws Exception {

        // Given a parquet file which contains null value column c_timestamp_millis, c_timestamp_micros and c_date
        var nodePath = Path.of(getClass()
                .getResource("/parquet/temporal_nullable_types.parquet")
                .toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodePath})), Map.of(), INTEGER, groups, MONITOR);

        // When processing the file, should not fail and should ignore the null value columns
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNodeWithoutGroupAndIdCheck(
                    nodes,
                    properties(
                            "c_time_millis_utc",
                            TimeValue.time(23, 59, 59, 999_000_000, ZoneOffset.UTC),
                            "c_time_micros",
                            LocalTimeValue.localTime(23, 59, 59, 999_999_000),
                            "c_time_micros_utc",
                            TimeValue.time(23, 59, 59, 999_999_000, ZoneOffset.UTC),
                            "c_timestamp_millis_utc",
                            DateTimeValue.datetime(
                                    1999, 1, 5, 23, 59, 59, 999_000_000, ZoneId.of(ZoneOffset.UTC.getId())),
                            "c_timestamp_micros",
                            LocalDateTimeValue.localDateTime(1999, 1, 5, 22, 59, 59, 999_999_000),
                            "c_timestamp_micros_utc",
                            DateTimeValue.datetime(
                                    1999, 1, 5, 23, 59, 59, 999_999_000, ZoneId.of(ZoneOffset.UTC.getId()))),
                    labels());

            assertThat(IntStream.range(0, visitor.properties.size())
                            .filter(i -> i % 2 == 1)
                            .mapToObj(visitor.properties::get)
                            .toList())
                    .doesNotContain("c_time_millis")
                    .doesNotContain("c_timestamp_millis")
                    .doesNotContain("c_date");

            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseNumbersLikeDataImporter() throws Exception {

        var nodePath =
                Path.of(getClass().getResource("/parquet/numeric_types.parquet").toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodePath})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNodeWithoutGroupAndIdCheck(
                    nodes,
                    properties(
                            "c_boolean",
                            true,
                            "c_byte",
                            127,
                            "c_short",
                            32767,
                            "c_int",
                            2147483647,
                            "c_long",
                            9223372036854775807L,
                            "c_float16",
                            "5.016327E-4", // cannot read correctly
                            "c_float32",
                            3.4028235E38F,
                            "c_double",
                            1.7976931348623157e+308,
                            "c_decimal_int32",
                            2147483647,
                            "c_decimal_int64",
                            9223372036854775807L,
                            "c_decimal_binary",
                            "1234567890",
                            "c_decimal_bytes",
                            "1234567890",
                            "c_ubyte",
                            255,
                            "c_ushort",
                            65535,
                            "c_uint",
                            -1, // ignore
                            "c_ulong",
                            -1L), // ignore
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseTimePropertyValuesWithTimezoneInHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:Time{timezone:+02:00}")),
                List.of(new Object[] {0, "Mattias", "13:37"}, new Object[] {1, "Johan", "16:20:01"}, new Object[] {
                    2, "Bob", "07:30-05:00"
                }));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", TimeValue.time(13, 37, 0, 0, "+02:00")), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(16, 20, 1, 0, "+02:00")), labels());
            assertNextNode(
                    nodes, 2L, properties("name", "Bob", "time", TimeValue.time(7, 30, 0, 0, "-05:00")), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDateTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:DateTime")),
                List.of(
                        new Object[] {0, "Mattias", "2018-02-27T13:37"},
                        new Object[] {1, "Johan", "2018-03-01T16:20:01"},
                        new Object[] {2, "Bob", "1981-05-11T07:30-05:00"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", DateTimeValue.datetime(2018, 2, 27, 13, 37, 0, 0, "+00:00")),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("name", "Johan", "time", DateTimeValue.datetime(2018, 3, 1, 16, 20, 1, 0, "+00:00")),
                    labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("name", "Bob", "time", DateTimeValue.datetime(1981, 5, 11, 7, 30, 0, 0, "-05:00")),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDateTimePropertyValuesWithTimezoneInHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:DateTime{timezone:Europe/Stockholm}")),
                List.of(
                        new Object[] {0, "Mattias", "2018-02-27T13:37"},
                        new Object[] {1, "Johan", "2018-03-01T16:20:01"},
                        new Object[] {2, "Bob", "1981-05-11T07:30-05:00"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties(
                            "name",
                            "Mattias",
                            "time",
                            DateTimeValue.datetime(2018, 2, 27, 13, 37, 0, 0, "Europe/Stockholm")),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "name",
                            "Johan",
                            "time",
                            DateTimeValue.datetime(2018, 3, 1, 16, 20, 1, 0, "Europe/Stockholm")),
                    labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("name", "Bob", "time", DateTimeValue.datetime(1981, 5, 11, 7, 30, 0, 0, "-05:00")),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseLocalTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:LocalTime")),
                List.of(new Object[] {0, "Mattias", "13:37"}, new Object[] {1, "Johan", "16:20:01"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", LocalTimeValue.localTime(13, 37, 0, 0)), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", LocalTimeValue.localTime(16, 20, 1, 0)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseLocalDateTimePropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("time:LocalDateTime")),
                List.of(new Object[] {0, "Mattias", "2018-02-27T13:37"}, new Object[] {1, "Johan", "2018-03-01T16:20:01"
                }));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", LocalDateTimeValue.localDateTime(2018, 2, 27, 13, 37, 0, 0)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("name", "Johan", "time", LocalDateTimeValue.localDateTime(2018, 3, 1, 16, 20, 1, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseDurationPropertyValues() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("duration:Duration")),
                List.of(new Object[] {0, "Mattias", "P3MT13H37M"}, new Object[] {1, "Johan", "P-1YT4H20M"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "duration", DurationValue.duration(3, 0, 13 * 3600 + 37 * 60, 0)),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties("name", "Johan", "duration", DurationValue.duration(-12, 0, 4 * 3600 + 20 * 60, 0)),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldHaveNodesBelongToGroupSpecifiedInHeader() throws Exception {
        // GIVEN
        Group group = groups.getOrCreate("MyGroup");
        String idHeader = ":ID(%s)".formatted(group.name());
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(idHeader),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of(new Object[] {123, "one"}, new Object[] {456, "two"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, group, 123L, properties("name", "one"), labels());
            assertNextNode(nodes, group, 456L, properties("name", "two"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void multipleIdColumnsRequireStringIdType() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part1:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part2:ID")),
                List.of(new Object[] {123, 456}, new Object[] {3, 6}));
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Having multiple :ID columns requires idType: STRING");
    }

    @Test
    void shouldHandleMultipleNodeIdColumnsWithSameExplicitGroup() throws Exception {
        Group group = groups.getOrCreate("MyGroup");
        String idHeader = ":ID(%s)".formatted(group.name());
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part1%s".formatted(idHeader)),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part2%s".formatted(idHeader))),
                List.of(new Object[] {123, 456}, new Object[] {3, 6}));
        var input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, groups, MONITOR);
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    group,
                    "123%s456".formatted(ParquetInput.DELIMITER),
                    properties("part1", 123, "part2", 456),
                    labels());
            assertNextNode(
                    nodes,
                    group,
                    "3%s6".formatted(ParquetInput.DELIMITER),
                    properties("part1", 3, "part2", 6),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotFailWithDifferentIdsCombinedToVirtuallyTheSameId() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part1:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part2:ID")),
                List.of(new Object[] {123, 456}, new Object[] {1234, 56}));
        var input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, groups, MONITOR);
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    "123%s456".formatted(ParquetInput.DELIMITER),
                    properties("part1", 123, "part2", 456),
                    labels());
            assertNextNode(
                    nodes,
                    "1234%s56".formatted(ParquetInput.DELIMITER),
                    properties("part1", 1234, "part2", 56),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void multipleNodeIdColumnsRequireSameGroup() throws Exception {
        Group group1 = groups.getOrCreate("MyGroup1");
        Group group2 = groups.getOrCreate("MyGroup2");
        String idHeader1 = ":ID(%s)".formatted(group1.name());
        String idHeader2 = ":ID(%s)".formatted(group2.name());
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part1%s".formatted(idHeader1)),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("part2%s".formatted(idHeader2))),
                List.of(new Object[] {123, 456}, new Object[] {3, 6}));
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), STRING, groups, MONITOR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("There are multiple :ID columns, but they are referring to different groups");
    }

    @Test
    void shouldHaveRelationshipsSpecifyStartEndNodeIdGroupsInHeader() throws Exception {
        var startGroupName = "StartGroup";
        var endGroupName = "EndGroup";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named(":START_ID(%s)".formatted(startGroupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named(":END_ID(%s)".formatted(endGroupName))),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .named(":ID(%s)".formatted(startGroupName))),
                List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(%s)".formatted(endGroupName))),
                List.of());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                groups,
                MONITOR);
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(relationships, startGroupName, 123L, endGroupName, 234L, "TYPE", properties());
            assertRelationship(relationships, startGroupName, 345L, endGroupName, 456L, "TYPE", properties());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldCorrectlyReferenceStartAndEndIdFromGroups() throws Exception {
        var startGroupName = "StartGroup";
        var endGroupName = "EndGroup";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named(":START_ID(%s)".formatted(startGroupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named(":END_ID(%s)".formatted(endGroupName))),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                        .named(":ID(%s)".formatted(startGroupName))),
                List.<Object[]>of(new Object[] {123}, new Object[] {345}));
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(%s)".formatted(endGroupName))),
                List.<Object[]>of(new Object[] {234}, new Object[] {456}));
        Input input = createParquetInput(
                Map.of(
                        Set.of("STARTTHING"),
                        List.<Path[]>of(new Path[] {nodeFile1}),
                        Set.of("ENDTHING"),
                        List.<Path[]>of(new Path[] {nodeFile2})),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                groups,
                MONITOR);
        var nodesFromIterator = new ArrayList<VisitedNode>();
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertTrue(readNext(nodes));
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertTrue(readNext(nodes));
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertTrue(readNext(nodes));
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertTrue(readNext(nodes));
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertFalse(readNext(nodes));
        }
        assertNextVisitedNode(nodesFromIterator, 234L, groups.get(endGroupName), Set.of("ENDTHING"));
        assertNextVisitedNode(nodesFromIterator, 456L, groups.get(endGroupName), Set.of("ENDTHING"));
        assertNextVisitedNode(nodesFromIterator, 123L, groups.get(startGroupName), Set.of("STARTTHING"));
        assertNextVisitedNode(nodesFromIterator, 345L, groups.get(startGroupName), Set.of("STARTTHING"));
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(relationships, startGroupName, 123L, endGroupName, 234L, "TYPE", properties());
            assertRelationship(relationships, startGroupName, 345L, endGroupName, 456L, "TYPE", properties());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldCorrectlyAssignCombinedIdsFromNodesToRelationships() throws Exception {
        var groupName = "aGroup";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("id1:START_ID(%s)".formatted(groupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("id2:START_ID(%s)".formatted(groupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("id3:END_ID(%s)".formatted(groupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("id4:END_ID(%s)".formatted(groupName))),
                List.of(new Object[] {123, 333, "TYPE", 234, 444}, new Object[] {345, 555, "TYPE", 456, 666}));
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id1:ID(%s)".formatted(groupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id2:ID(%s)".formatted(groupName))),
                List.of(new Object[] {123, 333}, new Object[] {345, 555}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id3:ID(%s)".formatted(groupName)),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id4:ID(%s)".formatted(groupName))),
                List.of(new Object[] {234, 444}, new Object[] {456, 666}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                STRING,
                groups,
                MONITOR);
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(
                    relationships,
                    groupName,
                    "123%c333".formatted(ParquetInput.DELIMITER),
                    groupName,
                    "234%c444".formatted(ParquetInput.DELIMITER),
                    "TYPE",
                    properties());
            assertRelationship(
                    relationships,
                    groupName,
                    "345%c555".formatted(ParquetInput.DELIMITER),
                    groupName,
                    "456%c666".formatted(ParquetInput.DELIMITER),
                    "TYPE",
                    properties());
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldDoWithoutRelationshipTypeHeaderIfDefaultSupplied() throws Exception {
        // GIVEN relationship data w/o :TYPE column
        String defaultType = "HERE";
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of(new Object[] {0, 1, "First"}, new Object[] {2, 3, "Second"}));
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")), List.of());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(defaultType, List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            // THEN
            assertNextRelationship(relationships, 0L, 1L, defaultType, properties("name", "First"));
            assertNextRelationship(relationships, 2L, 3L, defaultType, properties("name", "Second"));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldIgnoreNodeEntriesMarkedIgnoreUsingHeader() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:IGNORE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("other:int"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(
                        new Object[] {1, "Mattias", "10", "Person"},
                        new Object[] {2, "Johan", "111", "Person"},
                        new Object[] {3, "Emil", "12", "Person"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("other", 10), labels("Person"));
            assertNextNode(nodes, 2L, properties("other", 111), labels("Person"));
            assertNextNode(nodes, 3L, properties("other", 12), labels("Person"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldIgnoreRelationshipEntriesMarkedIgnoreUsingHeader() throws Exception {
        // GIVEN
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("prop:IGNORE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("other:int")),
                List.of(
                        new Object[] {1, "KNOWS", 2, "Mattias", "10"},
                        new Object[] {2, "KNOWS", 3, "Johan", "111"},
                        new Object[] {3, "KNOWS", 4, "Emil", "12"}));
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")), List.of());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                new Groups(),
                MONITOR);

        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 1L, 2L, "KNOWS", properties("other", 10));
            assertNextRelationship(relationships, 2L, 3L, "KNOWS", properties("other", 111));
            assertNextRelationship(relationships, 3L, 4L, "KNOWS", properties("other", 12));
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldUseOverriddenArrayDelimiterWithSpecialCharacter() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("prop:int[]")),
                Collections.singletonList(new Object[] {1, "1?23"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter('?').build());

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("prop", Values.intArray(new int[] {1, 23})), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldUseOverriddenArrayDelimiterWithSpecialCharacterForMultipleLabels() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                Collections.singletonList(new Object[] {1, "Foo?Bar"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter('?').build());

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties(), labels("Foo", "Bar"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldNotIncludeEmptyArraysInEntities() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("sprop:String[]"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lprop:long[]")),
                List.of(new Object[] {1, "", ""}, new Object[] {2, "a;b", "10;20"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("sprop", Values.stringArray("a", "b"), "lprop", Values.longArray(new long[] {10, 20})),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldParseColumnNamesWithArrayDefinition() throws Exception {
        var commentHeader =
                Path.of(getClass().getResource("/parquet/complex_header1.csv").toURI());
        var commentFile = Path.of(
                getClass().getResource("/parquet/complex_comment.parquet").toURI());
        var personHeader =
                Path.of(getClass().getResource("/parquet/complex_header2.csv").toURI());
        var personFile = Path.of(
                getClass().getResource("/parquet/complex_person.parquet").toURI());
        var relationshipHeader =
                Path.of(getClass().getResource("/parquet/complex_header3.csv").toURI());
        var relationshipFile = Path.of(getClass()
                .getResource("/parquet/complex_comment_hasCreator_person.parquet")
                .toURI());
        Input input = createParquetInput(
                Map.of(
                        Set.of("Comment"), List.<Path[]>of(new Path[] {commentHeader, commentFile}),
                        Set.of("Person"), List.<Path[]>of(new Path[] {personHeader, personFile})),
                Map.of("HAS_CREATOR", List.<Path[]>of(new Path[] {relationshipHeader, relationshipFile})),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            var readNodes = 0;
            while (readNext(nodes)) {
                readNodes++;
            }
            assertThat(readNodes).isEqualTo(8);
            assertFalse(readNext(nodes));
        }
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            var readRelationships = 0;
            while (readNext(relationships)) {
                readRelationships++;
            }
            assertThat(readRelationships).isEqualTo(4);
            assertFalse(readNext(relationships));
        }
    }

    @Test
    void shouldNotIncludeNullArraysInEntities() throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("sprop:String[]"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lprop:long[]")),
                List.of(new Object[] {1, null, null}, new Object[] {2, "a;b", "10;20"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("sprop", Values.stringArray("a", "b"), "lprop", Values.longArray(new long[] {10, 20})),
                    labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {":SOMETHING", "abcde#rtg:123", "", ":START_ID", ":END_ID", ":TYPE"})
    void shouldFailOnUnparsableNodeColumn(String unparsableColumnNames) throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(unparsableColumnNames)),
                List.<Object[]>of(new Object[] {1, "test"}));
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, MONITOR))
                .isInstanceOf(InputException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {":SOMETHING", "abcde#rtg:123", ":ID", ":LABEL"})
    void shouldFailOnUnparsableRelationshipHeader(String unparsableColumnName) throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(unparsableColumnName)),
                List.<Object[]>of(new Object[] {1, 2, "TYPE", "test"}));
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.of()),
                        Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                        INTEGER,
                        groups,
                        MONITOR))
                .isInstanceOf(InputException.class);
    }

    @Test
    void shouldFailOnUndefinedGroupInRelationshipHeader() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID(left)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID(rite)")),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(left)")), List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(right)")), List.of());
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                        Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                        INTEGER,
                        groups,
                        MONITOR))
                .isInstanceOf(InputException.class);
    }

    @Test
    void shouldFailOnGlobalGroupInRelationshipHeaderIfNoGlobalGroupInNodeHeader() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID(left)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID")),
                List.of(new Object[] {123, "TYPE", 234}, new Object[] {345, "TYPE", 456}));
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(left)")), List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID(right)")), List.of());
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                        Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                        INTEGER,
                        new Groups(),
                        MONITOR)) // new Groups() instead of field groups important here to not have the global id space
                .isInstanceOf(InputException.class);
    }

    @Test
    void shouldNormalizeTypes() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("byteProp:byte"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("longProp:long")),
                List.<Object[]>of(new Object[] {123, 234, 8, 123L}));
        Path nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("shortProp:short"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("intProp:int")),
                List.<Object[]>of(new Object[] {1, 234, 1024}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("floatProp:float"),
                        Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("doubleProp")),
                List.<Object[]>of(new Object[] {2, 43f, 37d}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        createParquetInput(
                Map.of(Set.of("someLabel"), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of("someType", List.<Path[]>of(new Path[] {relationshipFile})),
                INTEGER,
                groups,
                monitor);

        // then
        verify(monitor, times(1)).typeNormalized("test1.parquet", "intProp", "INT", "LONG");
        verify(monitor, times(1)).typeNormalized("test1.parquet", "shortProp", "SHORT", "LONG");
        verify(monitor, times(1)).typeNormalized("test2.parquet", "floatProp", "FLOAT", "DOUBLE");
        verify(monitor, times(1)).typeNormalized("test0.parquet", "byteProp", "BYTE", "LONG");
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldReportNoNodeLabels() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")),
                List.<Object[]>of(new Object[] {1}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, monitor);
        // then
        verify(monitor).noNodeLabelsSpecified("test0.parquet");
    }

    @Test
    void shouldNotReportNoNodeLabelsIfDecorated() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID")),
                List.<Object[]>of(new Object[] {1}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        createParquetInput(
                Map.of(Set.of("test"), List.<Path[]>of(new Path[] {nodeFile})), Map.of(), INTEGER, groups, monitor);

        // then
        verify(monitor, never()).noNodeLabelsSpecified("test0.parquet");
    }

    @Test
    void shouldReportNoRelationshipType() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID")),
                List.<Object[]>of(new Object[] {1, 2}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        createParquetInput(
                Map.of(), Map.of("", List.<Path[]>of(new Path[] {relationshipFile})), INTEGER, groups, monitor);

        // then
        verify(monitor).noRelationshipTypeSpecified("test0.parquet");
    }

    @Test
    void shouldNotReportNoRelationshipTypeIfDecorated() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID")),
                List.<Object[]>of(new Object[] {1, 2}));
        ParquetMonitor monitor = mock(ParquetMonitor.class);

        // when
        createParquetInput(
                Map.of(), Map.of("someType", List.<Path[]>of(new Path[] {relationshipFile})), INTEGER, groups, monitor);
        // then
        verify(monitor, never()).noRelationshipTypeSpecified("test0.parquet");
    }

    @Test
    void shouldReportDuplicateNodeHeader() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:string"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of());
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        INTEGER,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(DuplicatedColumnException.class)
                .hasMessageContaining("test0.parquet");
    }

    @Test
    void shouldReportDuplicateRelationshipHeader() throws Exception {
        // given
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.of());
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(),
                        Map.of("", List.<Path[]>of(new Path[] {relationshipFile})),
                        INTEGER,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(DuplicatedColumnException.class)
                .hasMessageContaining("test0.parquet");
    }

    @Test
    void shouldThrowOnReferencedNodeSchemaWithoutExplicitLabelOptionData() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("my:ID(Person)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:string"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of());
        try (var input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                STRING,
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));

            // then
            assertThatThrownBy(() -> input.referencedNodeSchema(tokenHolders))
                    .hasMessageContaining("No label was specified");
        }
    }

    @Test
    void shouldHandleMultipleEqualReferencedSchemaForSameGroup() throws Exception {
        // given
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Person}")),
                List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Person}")),
                List.of());
        try (var input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of(),
                STRING,
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));

            // then
            var referencedNodeSchema = input.referencedNodeSchema(tokenHolders);
            assertThat(referencedNodeSchema)
                    .containsEntry(
                            "MyGroup",
                            SchemaDescriptors.forLabel(
                                    tokenHolders.labelTokens().getIdByName("Person"),
                                    tokenHolders.propertyKeyTokens().getIdByName("myId")));
        }
    }

    @Test
    void shouldFailMultipleNonEqualReferencedSchemaForSameGroup() throws Exception {
        // given
        Path nodeFile1 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Person}")),
                List.of());
        Path nodeFile2 = createParquetFile(
                List.of(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(MyGroup){label:Company}")),
                List.of());
        try (var input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile1, nodeFile2})),
                Map.of(),
                STRING,
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)),
                    tokenHolder(Map.of("Person", 2, "Company", 3)),
                    tokenHolder(Map.of()));

            // then
            assertThatThrownBy(() -> input.referencedNodeSchema(tokenHolders))
                    .hasMessageContaining("Multiple different indexes for group");
        }
    }

    @Test
    void shouldParseReferencedNodeSchemaWithExplicitLabelOptionData() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("myId:ID(My Group){label:Person}"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name:string"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of());
        try (var input = createParquetInput(
                Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                Map.of(),
                STRING,
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));
            var schema = input.referencedNodeSchema(tokenHolders);

            // then
            Assertions.assertThat(schema).isEqualTo(Map.of("My Group", SchemaDescriptors.forLabel(2, 4)));
        }
    }

    @Test
    void shouldStoreIdAsPropertyInSpecificValueType() throws Exception {
        // given nodes w/ IDs as ints
        // when using string id-type in the input
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id:ID(new-group){id-type:int}"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("prop")),
                List.<Object[]>of(new Object[] {123, "val"}));
        try (var input = createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // then
            assertNextNode(nodes, groups.get("new-group"), 123, properties("id", 123, "prop", "val"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldHandleMultipleNodeIdColumns() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id1:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id2:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"ABC", "123", "First", "Person"}, new Object[] {"ABC", "456", "Second", "Person"
                }));
        try (var input = createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(Collector.STRICT).iterator()) {
            assertNextNode(
                    nodes,
                    "ABC%s123".formatted(ParquetInput.DELIMITER),
                    properties("id1", "ABC", "id2", "123", "name", "First"),
                    Set.of("Person"));
            assertNextNode(
                    nodes,
                    "ABC%s456".formatted(ParquetInput.DELIMITER),
                    properties("id1", "ABC", "id2", "456", "name", "Second"),
                    Set.of("Person"));
            assertFalse(readNext(nodes));
        }
    }

    @Test
    void shouldFailOnStoringMultipleCompositeIdColumnsInSameProperty() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("id:ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"ABC", "123", "First", "Person"}, new Object[] {"ABC", "456", "Second", "Person"
                }));
        // when/then
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(InputException.class)
                .hasMessageContaining("Cannot store composite IDs");
    }

    @Test
    void shouldFailOnCompositeIdColumnsForDifferentGroups() throws Exception {
        // given
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID(group1)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":ID(group2)"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"ABC", "123", "First", "Person"}, new Object[] {"ABC", "456", "Second", "Person"
                }));
        // when/then
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        INTEGER,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("referring to different groups");
    }

    @Test
    void shouldFailOnNonParquetFile() throws Exception {
        Path nodeFile = createNonParquetFile();
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.<Path[]>of(new Path[] {nodeFile})),
                        Map.of(),
                        INTEGER,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Could not read parquet file %s".formatted(nodeFile));
    }

    @Test
    void shouldProvideRelationshipReusingSameDataFileAsNode() throws Exception {
        Path nodeHeaderFile1 =
                createHeaderFile(List.of("id:ID(n@3<p@3_3>){id-type:long}", "name", ":IGNORE"), List.of());
        var personId = 42L;
        var bandId = 76L;
        Path commonFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("band_id")),
                List.<Object[]>of(new Object[] {personId, "Jane Doe", bandId}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.<Object[]>of(new Object[] {bandId, "the band wagon"}));
        Path nodeHeaderFile2 = createHeaderFile(List.of("id:ID(n@2<p@2_2>){id-type:long}", "name"), List.of());
        Path relHeaderFile1 = createHeaderFile(
                List.of("id:START_ID(n@3<p@3_3>)", ":IGNORE", "band_id:END_ID(n@2<p@2_2>)"), List.of());

        try (ParquetInput input = createParquetInput(
                        Map.of(
                                Set.of("Person"), List.<Path[]>of(new Path[] {nodeHeaderFile1, commonFile}),
                                Set.of("Band"), List.<Path[]>of(new Path[] {nodeHeaderFile2, nodeFile2})),
                        Map.of("MEMBER_OF", List.<Path[]>of(new Path[] {relHeaderFile1, commonFile})),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var rels = input.relationships(EMPTY).iterator()) {

            assertRelationship(
                    rels,
                    groups.get("n@3<p@3_3>"),
                    personId,
                    groups.get("n@2<p@2_2>"),
                    bandId,
                    "MEMBER_OF",
                    properties());
            assertFalse(readNext(rels));
        }
    }

    @Test
    void shouldProvideTwoRelationshipsWithSameTypeReusingSameDataFileAsNode() throws Exception {
        Path nodeHeaderFile1 =
                createHeaderFile(List.of("id:ID(n@3<p@3_3>){id-type:long}", "name", ":IGNORE", ":IGNORE"), List.of());
        var personId = 42L;
        var bandId = 76L;
        long groupId = 123L;
        Path commonFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("band_id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("group_id")),
                List.<Object[]>of(new Object[] {personId, "Jane Doe", bandId, groupId}));
        Path nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.<Object[]>of(new Object[] {bandId, "the band wagon"}));
        Path nodeHeaderFile2 = createHeaderFile(List.of("id:ID(n@2<p@2_2>){id-type:long}", "name"), List.of());
        Path nodeHeaderFile3 = createHeaderFile(
                List.of(":IGNORE", ":IGNORE", ":IGNORE", "id:ID(n@1<p@1_1>){id-type:long}"), List.of());
        Path relHeaderFile1 = createHeaderFile(
                List.of("id:START_ID(n@3<p@3_3>)", ":IGNORE", "band_id:END_ID(n@2<p@2_2>)", ":IGNORE"), List.of());
        Path relHeaderFile2 = createHeaderFile(
                List.of("id:START_ID(n@3<p@3_3>)", ":IGNORE", ":IGNORE", "group_id:END_ID(n@1<p@1_1>)"), List.of());

        try (ParquetInput input = createParquetInput(
                        Map.of(
                                Set.of("Person"), List.<Path[]>of(new Path[] {nodeHeaderFile1, commonFile}),
                                Set.of("Band"), List.<Path[]>of(new Path[] {nodeHeaderFile2, nodeFile2}),
                                Set.of("Group"), List.<Path[]>of(new Path[] {nodeHeaderFile3, commonFile})),
                        Map.of(
                                "MEMBER_OF",
                                List.<Path[]>of(new Path[] {relHeaderFile1, commonFile, relHeaderFile2, commonFile})),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var rels = input.relationships(EMPTY).iterator()) {

            assertRelationship(
                    rels,
                    groups.get("n@3<p@3_3>"),
                    personId,
                    groups.get("n@2<p@2_2>"),
                    bandId,
                    "MEMBER_OF",
                    properties());
            assertRelationship(
                    rels,
                    groups.get("n@3<p@3_3>"),
                    personId,
                    groups.get("n@1<p@1_1>"),
                    groupId,
                    "MEMBER_OF",
                    properties());
            assertFalse(readNext(rels));
        }
    }

    private static ParquetInput createParquetInput(
            Map<Set<String>, List<Path[]>> nodeFiles,
            Map<String, List<Path[]>> relationshipFiles,
            IdType idType,
            Groups idGroups,
            ParquetMonitor parquetMonitor) {
        return createParquetInput(
                nodeFiles,
                relationshipFiles,
                idType,
                idGroups,
                parquetMonitor,
                Configuration.newBuilder().build());
    }

    private static ParquetInput createParquetInput(
            Map<Set<String>, List<Path[]>> nodeFiles,
            Map<String, List<Path[]>> relationshipFiles,
            IdType idType,
            Groups idGroups,
            ParquetMonitor parquetMonitor,
            Configuration csvConfig) {
        return new ParquetInput(nodeFiles, relationshipFiles, List.of(), idType, csvConfig, idGroups, parquetMonitor);
    }

    private Path createNonParquetFile() throws Exception {
        Path path = directory.file("test-non.parquet");
        try (var writer = new FileWriter(path.toFile())) {
            writer.write("some data for sure not parquet");
        }
        return path;
    }

    private Path createParquetFile(List<org.apache.parquet.schema.Type> types, List<Object[]> data) throws Exception {
        Path path = directory.file("test%d.parquet".formatted(parquetCounter.getAndIncrement()));
        try (var writer =
                ParquetWriter.writeFile(new MessageType("something", types), path.toFile(), (record, valueWriter) -> {
                    var recordData = (Object[]) record;
                    for (int i = 0; i < types.size(); i++) {
                        org.apache.parquet.schema.Type type = types.get(i);
                        Object value = recordData[i];
                        if (value != null) {
                            valueWriter.write(type.getName(), value);
                        }
                    }
                })) {
            for (Object[] datum : data) {
                writer.write(datum);
            }
        }

        return path;
    }

    private Path createHeaderFile(List<String> columnNames, List<String> originalColumnNames) throws Exception {
        return createHeaderFile(columnNames, originalColumnNames, ",");
    }

    private Path createHeaderFile(List<String> columnNames, List<String> originalColumnNames, String delimiter)
            throws Exception {
        Path path = directory.file("header" + headerCounter.getAndIncrement() + ".csv");
        createHeaderFile(path, columnNames, originalColumnNames, delimiter);
        return path;
    }

    private static void createHeaderFile(
            Path path, List<String> columnNames, List<String> originalColumnNames, String delimiter) throws Exception {
        try (var writer = new BufferedWriter(new FileWriter(path.toFile()))) {
            writer.write(String.join(delimiter, columnNames));
            writer.newLine();
            if (!originalColumnNames.isEmpty()) {
                writer.write(String.join(delimiter, originalColumnNames));
                writer.newLine();
            }
        }
    }

    private TokenHolder tokenHolder(Map<String, Integer> tokens) {
        var tokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, "type");
        tokenHolder.setInitialTokens(tokens.entrySet().stream()
                .map(e -> new NamedToken(e.getKey(), e.getValue()))
                .toList());
        return tokenHolder;
    }

    private void assertNextRelationship(
            InputIterator relationship, Object startNode, Object endNode, String type, Map<String, Object> properties)
            throws IOException {
        assertRelationship(relationship, groups.get(null), startNode, groups.get(null), endNode, type, properties);
    }

    // testing arbitrary order of relationships
    private void assertNextRelationship(
            InputIterator relationship,
            List<Object> startNodes,
            List<Object> endNodes,
            List<String> types,
            List<Map<String, Object>> propertiess)
            throws IOException {
        var success = false;
        Throwable lastError = null;
        assertTrue(readNext(relationship));
        for (int i = 0; i < startNodes.size(); i++) {
            var startNode = startNodes.get(i);
            var endNode = endNodes.get(i);
            var type = types.get(i);
            var properties = propertiess.get(i);
            try {
                assertEquals(groups.get(null), visitor.startIdGroup);
                assertEquals(startNode, visitor.startId());
                assertEquals(groups.get(null), visitor.endIdGroup);
                assertEquals(endNode, visitor.endId());
                assertEquals(type, visitor.stringType);
                assertPropertiesEquals(properties, visitor.propertiesAsMap());
                success = true;
            } catch (AssertionFailedError e) {
                lastError = e;
            }
        }
        if (!success) {
            fail(lastError);
        }
    }

    void assertRelationship(
            InputIterator data,
            Group startNodeGroup,
            Object startNode,
            Group endNodeGroup,
            Object endNode,
            String type,
            Map<String, Object> properties)
            throws IOException {
        assertTrue(readNext(data));
        assertEquals(startNodeGroup, visitor.startIdGroup);
        assertEquals(startNode, visitor.startId());
        assertEquals(endNodeGroup, visitor.endIdGroup);
        assertEquals(endNode, visitor.endId());
        assertEquals(type, visitor.stringType);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertRelationship(
            InputIterator data,
            String startNodeGroupName,
            Object startNode,
            String endNodeGroupName,
            Object endNode,
            String type,
            Map<String, Object> properties)
            throws IOException {
        assertTrue(readNext(data));
        assertEquals(startNodeGroupName, visitor.startIdGroup.name());
        assertEquals(startNode, visitor.startId());
        assertEquals(endNodeGroupName, visitor.endIdGroup.name());
        assertEquals(endNode, visitor.endId());
        assertEquals(type, visitor.stringType);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private record VisitedNode(Object id, String groupName, List<String> labels) {
        private static VisitedNode from(InputEntity inputEntity) {
            return new VisitedNode(
                    inputEntity.id(),
                    inputEntity.idGroup.name(),
                    Arrays.stream(inputEntity.labels()).toList());
        }
    }

    private void assertNextVisitedNode(List<VisitedNode> visitedNode, Object id, Group group, Set<String> labels) {
        assertThat(visitedNode).haveAtLeastOne(new Condition<>() {
            @Override
            public boolean matches(VisitedNode value) {
                return id.equals(value.id())
                        && group.name().equals(value.groupName())
                        && labels.containsAll(value.labels());
            }
        });
    }

    private void assertNextNode(InputIterator data, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertNextNode(data, groups.get(null), id, properties, labels);
    }

    private void assertNextNode(
            InputIterator data, Group group, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertTrue(readNext(data));
        assertEquals(group, visitor.idGroup);
        assertEquals(id, visitor.id());
        assertEquals(labels, asSet(visitor.labels()));
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertNextNodeWithoutGroupAndIdCheck(
            InputIterator data, Map<String, Object> properties, Set<String> labels) throws IOException {
        assertTrue(readNext(data));
        assertEquals(labels, asSet(visitor.labels()));
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertPropertiesEquals(Map<String, Object> expected, Map<String, Object> actual) {
        // Do this more complicated assert to handle primitive array equality
        assertEquals(primitiveArraysAsLists(expected), primitiveArraysAsLists(actual));
    }

    private Map<String, Object> primitiveArraysAsLists(Map<String, Object> map) {
        var result = new HashMap<String, Object>();
        for (var entry : map.entrySet()) {
            result.put(entry.getKey(), convertToList(entry.getValue()));
        }
        return result;
    }

    private Object convertToList(Object value) {
        if (value.getClass().isArray()) {
            return convertPrimitiveArrayToList(value);
        }
        if (value instanceof ArrayValue arrayValue) {
            return convertArrayValueToList(arrayValue);
        }
        return value;
    }

    private List<Object> convertPrimitiveArrayToList(Object array) {
        var length = Array.getLength(array);
        var result = new ArrayList<>(length);
        for (var i = 0; i < length; i++) {
            result.add(Array.get(array, i));
        }
        return result;
    }

    private List<Object> convertArrayValueToList(ArrayValue arrayValue) {
        var size = arrayValue.intSize();
        var result = new ArrayList<>(size);
        for (var i = 0; i < size; i++) {
            var v = arrayValue.value(i);
            result.add(v instanceof Value value ? value.asObject() : v);
        }
        return result;
    }

    private boolean readNext(InputIterator data) throws IOException {
        if (referenceData != data) {
            chunk = null;
            referenceData = data;
        }

        if (chunk == null) {
            chunk = data.newChunk();
            if (!data.next(chunk)) {
                return false;
            }
        }

        if (chunk.next(visitor)) {
            return true;
        }
        if (!data.next(chunk)) {
            return false;
        }
        return chunk.next(visitor);
    }

    private static Map<String, Object> properties(Object... keysAndValues) {
        return MapUtil.map(keysAndValues);
    }

    private static Set<String> labels(String... labels) {
        return asSet(labels);
    }

    private static Stream<String> groupNames() {
        return Stream.of("", null);
    }

    private static Stream<Arguments> listTypes() {
        return Stream.of(
                Arguments.of("list.parquet", List.of("a", "b", "c"), "string[]"),
                Arguments.of("list_int32.parquet", List.of(123, 234, 345), "int[]"),
                Arguments.of("list_int64.parquet", List.of(123L, 234L, 345L), "long[]"),
                Arguments.of("list_int128.parquet", List.of(123d, 234d, 345d), "double[]"),
                Arguments.of("list_float.parquet", List.of(1.01f, 2.21f, 3.23f), "float[]"),
                Arguments.of("list_double.parquet", List.of(1.01d, 2.21d, 3.23d), "double[]"),
                Arguments.of("list_boolean.parquet", List.of(true, false, true), "boolean[]"));
    }

    private static Stream<Arguments> emptyListTypes() {
        return Stream.of(
                Arguments.of("list_empty.parquet", Values.EMPTY_TEXT_ARRAY),
                Arguments.of("list_empty_int32.parquet", Values.EMPTY_INT_ARRAY),
                Arguments.of("list_empty_int64.parquet", Values.EMPTY_LONG_ARRAY),
                Arguments.of("list_empty_float.parquet", Values.EMPTY_FLOAT_ARRAY),
                Arguments.of("list_empty_double.parquet", Values.EMPTY_DOUBLE_ARRAY),
                Arguments.of("list_empty_boolean.parquet", Values.EMPTY_BOOLEAN_ARRAY),
                Arguments.of("list_empty_date.parquet", Values.dateArray(new java.time.LocalDate[0])),
                Arguments.of("list_empty_time.parquet", Values.timeArray(new java.time.OffsetTime[0])),
                Arguments.of("list_empty_timestamp.parquet", Values.dateTimeArray(new java.time.ZonedDateTime[0])),
                Arguments.of("list_empty_localtime.parquet", Values.localTimeArray(new java.time.LocalTime[0])),
                Arguments.of(
                        "list_empty_localdatetime.parquet", Values.localDateTimeArray(new java.time.LocalDateTime[0])));
    }
}
