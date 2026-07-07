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
import static org.assertj.core.api.Assertions.fail;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Condition;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
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
import org.neo4j.values.storable.VectorValue;
import org.opentest4j.AssertionFailedError;

@TestDirectoryExtension
@RandomSupportExtension
class ParquetInputTest {

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

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
                Map.of(Set.of(), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
                Map.of(
                        Set.of(),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(0, nodeFileWithEmptyRowGroup),
                                new FileGroup.NumberedFile(1, nodeFile)))),
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
                Map.of(group, List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile1),
                                new FileGroup.NumberedFile(-1, nodeFile1),
                                new FileGroup.NumberedFile(-1, headerFile2),
                                new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertNextNode(nodes, 123L, properties("notaname", "Mattias Persson"), labels("HACKER", "USER"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, nodeFile1),
                                new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertNextNode(nodes, 456L, properties("name", "SomeoneElse"), labels("HACKER", "USER"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, nodeFile1),
                                new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertNextNode(nodes, 456L, properties("name", "SomeoneElse"), labels("HACKER", "USER"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties(), labels("HACKER", "USER"));
            assertThat(readNext(nodes)).isFalse();
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
                        List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile1))),
                        Set.of("somethingElse"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            List<InputEntity.Property> allProperties = new ArrayList<>();
            readNext(nodes);
            allProperties.addAll(visitor.properties);
            readNext(nodes);
            allProperties.addAll(visitor.properties);

            assertThat(allProperties)
                    .satisfiesExactlyInAnyOrder(
                            propertyNode1 -> {
                                assertThat(propertyNode1.asValue()).isEqualTo(Values.stringValue("Mattias Persson"));
                                assertThat(propertyNode1.keyName()).isEqualTo("name");
                            },
                            propertyNode2 -> {
                                assertThat(propertyNode2.asValue()).isEqualTo(Values.stringValue("SomeoneElse"));
                                assertThat(propertyNode2.keyName()).isEqualTo("new_name");
                            });
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1),
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("ignored-column-name", "Mattias Persson"), labels("HACKER"));
            assertNextNode(nodes, 456L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(
                                Set.of(),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, headerFile),
                                        new FileGroup.NumberedFile(-1, nodeFile)))),
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
                        Map.of(
                                Set.of(),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, headerFile),
                                        new FileGroup.NumberedFile(-1, nodeFile)))),
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
                        Map.of(
                                Set.of(),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, headerFile),
                                        new FileGroup.NumberedFile(-1, nodeFile)))),
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
                Map.of(
                        Set.of("HACKER"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER", "USER"));
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(
                                Set.of(""),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, headerFile),
                                        new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // then
            assertNextNode(nodes, groups.get("new-group"), 123, properties("id", 123, "prop", "val"), labels());
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(
                                Set.of(""),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, headerFile),
                                        new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // Then the id field is converted to String and stored as a property
            assertNextNode(nodes, groups.get("new-group"), "123", properties("id", "123", "prop", "val"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldConvertIntegerIdsToStringWhenGlobalIdTypeIsString() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.<Object[]>of(new Object[] {6597069807267L, "Mattias Persson"}));
        Input input = createParquetInput(
                Map.of(Set.of("Person"), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                MONITOR);
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, "6597069807267", properties("name", "Mattias Persson"), labels("Person"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldConvertIntegerRelationshipIdsToStringWhenGlobalIdTypeIsString() throws Exception {
        Path relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":START_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":END_ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":TYPE")),
                List.<Object[]>of(
                        new Object[] {9345850217180L, 6597069807267L, "COMMENT_HAS_CREATOR"},
                        new Object[] {1L, 2L, "KNOWS"}));
        Input input = createParquetInput(
                Map.of(),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                STRING,
                groups,
                MONITOR);
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(
                    relationships, "9345850217180", "6597069807267", "COMMENT_HAS_CREATOR", properties());
            assertNextRelationship(relationships, "1", "2", "KNOWS", properties());
            assertThat(readNext(relationships)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("listTypes")
    void shouldReadListTypes(String fileName, List<?> expectedList) throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/" + fileName);
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aList", expectedList, "name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aList", expectedList, "name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListTypesWithSingleEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/list_single.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aList", List.of("a"), "name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfTemporalTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/list_temporal.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(
                        ":ID",
                        "c_list_date",
                        "c_list_local_time",
                        "c_list_zoned_time",
                        "c_list_local_timestamp",
                        "c_list_offset_timestamp",
                        "c_list_zoned_timestamp"),
                List.of(
                        "id",
                        "c_list_date",
                        "c_list_local_time",
                        "c_list_zoned_time",
                        "c_list_local_timestamp",
                        "c_list_offset_timestamp",
                        "c_list_zoned_timestamp"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(0, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "c_list_date",
                                    List.of(
                                            LocalDate.of(2020, 12, 13),
                                            LocalDate.of(2021, 1, 15),
                                            LocalDate.of(2022, 3, 10)),
                            "c_list_local_time",
                                    List.of(
                                            LocalTime.of(22, 56, 57, 997000000),
                                            LocalTime.of(10, 30, 0),
                                            LocalTime.of(8, 45, 30)),
                            "c_list_zoned_time",
                                    List.of(
                                            OffsetTime.of(22, 56, 57, 997000000, ZoneOffset.UTC),
                                            OffsetTime.of(10, 30, 0, 0, ZoneOffset.UTC),
                                            OffsetTime.of(8, 45, 30, 0, ZoneOffset.UTC)),
                            "c_list_local_timestamp",
                                    List.of(
                                            LocalDateTime.of(2020, 12, 14, 23, 57, 58, 998000000),
                                            LocalDateTime.of(2021, 6, 15, 12, 30, 0),
                                            LocalDateTime.of(2022, 3, 10, 8, 45, 30)),
                            "c_list_offset_timestamp",
                                    List.of(
                                            ZonedDateTime.of(2020, 12, 14, 23, 58, 59, 999000000, ZoneOffset.UTC),
                                            ZonedDateTime.of(2021, 12, 15, 11, 30, 0, 0, ZoneOffset.UTC),
                                            ZonedDateTime.of(2022, 12, 10, 13, 45, 30, 0, ZoneOffset.UTC)),
                            "c_list_zoned_timestamp",
                                    List.of(
                                            ZonedDateTime.of(2020, 12, 14, 23, 58, 59, 999000000, ZoneOffset.UTC),
                                            ZonedDateTime.of(2021, 12, 15, 11, 30, 0, 0, ZoneOffset.UTC),
                                            ZonedDateTime.of(2022, 12, 10, 13, 45, 30, 0, ZoneOffset.UTC))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfTemporalTypesWithExplicitHeaders() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/list_temporal.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(
                        ":ID",
                        "c_list_date:date[]",
                        "c_list_local_time:localtime[]",
                        "c_list_zoned_time:time[]",
                        "c_list_local_timestamp:localdatetime[]",
                        "c_list_offset_timestamp:datetime[]",
                        "c_list_zoned_timestamp:datetime[]"),
                List.of(
                        "id",
                        "c_list_date",
                        "c_list_local_time",
                        "c_list_zoned_time",
                        "c_list_local_timestamp",
                        "c_list_offset_timestamp",
                        "c_list_zoned_timestamp"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(0, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "c_list_date",
                            List.of(LocalDate.of(2020, 12, 13), LocalDate.of(2021, 1, 15), LocalDate.of(2022, 3, 10)),
                            "c_list_local_time",
                            List.of(
                                    LocalTime.of(22, 56, 57, 997000000),
                                    LocalTime.of(10, 30, 0),
                                    LocalTime.of(8, 45, 30)),
                            "c_list_zoned_time",
                            List.of(
                                    OffsetTime.of(22, 56, 57, 997000000, ZoneOffset.UTC),
                                    OffsetTime.of(10, 30, 0, 0, ZoneOffset.UTC),
                                    OffsetTime.of(8, 45, 30, 0, ZoneOffset.UTC)),
                            "c_list_local_timestamp",
                            List.of(
                                    LocalDateTime.of(2020, 12, 14, 23, 57, 58, 998000000),
                                    LocalDateTime.of(2021, 6, 15, 12, 30, 0),
                                    LocalDateTime.of(2022, 3, 10, 8, 45, 30)),
                            "c_list_offset_timestamp",
                            List.of(
                                    ZonedDateTime.of(2020, 12, 14, 23, 58, 59, 999000000, ZoneOffset.UTC),
                                    ZonedDateTime.of(2021, 12, 15, 11, 30, 0, 0, ZoneOffset.UTC),
                                    ZonedDateTime.of(2022, 12, 10, 13, 45, 30, 0, ZoneOffset.UTC)),
                            "c_list_zoned_timestamp",
                            List.of(
                                    ZonedDateTime.of(2020, 12, 14, 23, 58, 59, 999000000, ZoneOffset.UTC),
                                    ZonedDateTime.of(2021, 12, 15, 11, 30, 0, 0, ZoneOffset.UTC),
                                    ZonedDateTime.of(2022, 12, 10, 13, 45, 30, 0, ZoneOffset.UTC))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("numericListTemporalTypes")
    void shouldReadListsOfTemporalTypesFromNumericLists(String mappedType, List<Long> rawValues, List<?> expected)
            throws Exception {
        // GIVEN a native LIST<int64> column (no temporal logical annotation) whose raw numeric elements are mapped to
        // a temporal array through the header. Each element flows through the Number branches of convertType, i.e. the
        // list-reading counterpart of the scalar shouldParseNumeric*PropertyValues tests.
        Path nodeFile = createLongListParquetFile("values", rawValues);
        Path headerFile = createHeaderFile(List.of(":ID", "values:" + mappedType), List.of(":ID", "values"));
        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("values", expected), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfPointsFromDelimitedStringColumn() throws Exception {
        // GIVEN
        // Parquet has no native point type, so point arrays can arrive as a delimited string column mapped via the
        // header. The element values become PointValues, which must be unwrapped before going into a point array.
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("points:point[]")),
                Collections.singletonList(new Object[] {1, "{x: 1.0, y: 2.0};{x: 3.0, y: 4.0}"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter(';').build());
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "points",
                            List.of(
                                    Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 2.0),
                                    Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 3.0, 4.0))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfDurationsFromDelimitedStringColumn() throws Exception {
        // GIVEN
        // Parquet has no native duration type, so duration arrays can arrive as a delimited string column mapped via
        // the header. The element values become DurationValues, which must be unwrapped before going into the array.
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("durations:duration[]")),
                Collections.singletonList(new Object[] {1, "P3MT13H37M;P-1YT4H20M"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter(';').build());
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "durations",
                            List.of(
                                    DurationValue.duration(3, 0, 13 * 3600 + 37 * 60, 0),
                                    DurationValue.duration(-12, 0, 4 * 3600 + 20 * 60, 0))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfPointsFromStringListColumn() throws Exception {
        // GIVEN
        // A native LIST<string> column whose elements are mapped to points through the header. This exercises the
        // list-reading branch (as opposed to splitting a delimited string), with each element parsed to a PointValue.
        Path nodeFile = createStringListParquetFile("points", List.of("{x: 1.0, y: 2.0}", "{x: 3.0, y: 4.0}"));
        Path headerFile = createHeaderFile(List.of(":ID", "points:point[]"), List.of(":ID", "points"));
        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "points",
                            List.of(
                                    Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 2.0),
                                    Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 3.0, 4.0))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfDurationsFromStringListColumn() throws Exception {
        // GIVEN
        // A native LIST<string> column whose elements are mapped to durations through the header, exercising the
        // list-reading branch with each element parsed to a DurationValue.
        Path nodeFile = createStringListParquetFile("durations", List.of("P3MT13H37M", "P-1YT4H20M"));
        Path headerFile = createHeaderFile(List.of(":ID", "durations:duration[]"), List.of(":ID", "durations"));
        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "durations",
                            List.of(
                                    DurationValue.duration(3, 0, 13 * 3600 + 37 * 60, 0),
                                    DurationValue.duration(-12, 0, 4 * 3600 + 20 * 60, 0))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadIntervalColumnAsDuration() throws Exception {
        // GIVEN
        // A native parquet INTERVAL column (12-byte months/days/millis) is read as a Neo4j duration without any header.
        Path nodeFile = createIntervalParquetFile("d", 14, 3, 90_500);
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("d", DurationValue.duration(14, 3, 90, 500_000_000)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadIntervalColumnAsDurationWithExplicitHeader() throws Exception {
        // GIVEN
        // The same INTERVAL column mapped explicitly as a duration via the header file.
        Path nodeFile = createIntervalParquetFile("d", 14, 3, 90_500);
        Path headerFile = createHeaderFile(List.of(":ID", "d:duration"), List.of(":ID", "d"));
        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("d", DurationValue.duration(14, 3, 90, 500_000_000)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListOfIntervalsAsDurations() throws Exception {
        // GIVEN
        // A native LIST<interval> column is read as a Neo4j duration array.
        Path nodeFile = createIntervalListParquetFile("d", List.of(new int[] {14, 3, 90_500}, new int[] {1, 0, 1_000}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "d",
                            List.of(
                                    DurationValue.duration(14, 3, 90, 500_000_000),
                                    DurationValue.duration(1, 0, 1, 0))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadEmptyListOfIntervalsAsEmptyDurationArray() throws Exception {
        // GIVEN
        // An empty LIST<interval> must be inferred as an (empty) duration array rather than falling back to the
        // FIXED_LEN_BYTE_ARRAY primitive type (which would wrongly produce an empty byte array).
        Path nodeFile = createIntervalListParquetFile("d", List.of());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("d", Values.durationArray(new DurationValue[0])), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("emptyListTypes")
    void shouldReadListTypesWithEmptyList(String fileName, ArrayValue expectedEmptyArray) throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/empty_list/" + fileName);
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes, 123L, properties("aList", expectedEmptyArray, "name", "Dhru Devalia"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadListTypesWithNullList() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/list_null.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 456L, properties("name", "Dhru"), labels("REKCAH"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadMapTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    123L,
                    properties("aMap.a", "aa", "aMap.b", "bb", "name", "Mattias Persson"),
                    labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadNumericMapTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_numeric.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes, 123L, properties("aMap.a", 1L, "aMap.b", 23L, "name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadMultipleMapTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_multiple.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
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
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadMapTypesWithNoEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_empty.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadMapTypesWithNullEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_null.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadMapTypesWithSingleEntry() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_single.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("aMap.x", "abcd", "name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailOnDuplicatedNamePrefix() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/map_duplicate_names.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        // WHEN/THEN
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        INTEGER,
                        groups,
                        MONITOR))
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes,
                    123L,
                    properties("aStruct.a", "aa", "aStruct.b", "bb", "name", "Mattias Persson"),
                    labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReadMultipleStructTypes() throws Exception {
        // GIVEN
        var fileUrl = getClass().getResource("/parquet/struct_multiple.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        System.out.println(nodeFile.toAbsolutePath());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
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
            assertThat(readNext(nodes)).isFalse();
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
                Maps.mutable.of(groupName, List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(
                        "",
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, relationshipFile)))),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(
                        "",
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, relationshipFile)))),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties());
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties());
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(
                        "",
                        List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile1))),
                        "ignore_me",
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, relationshipFile2)))),
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
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(
                        "",
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, relationshipFile1),
                                new FileGroup.NumberedFile(-1, headerFile),
                                new FileGroup.NumberedFile(-1, relationshipFile2)))),
                STRING,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("notsince", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("notsince", 987654L));
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1), new FileGroup.NumberedFile(-1, nodeFile2)))),
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(addedLabels), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 0L, properties("name", "First"), labels(addedLabels));
            assertNextNode(nodes, 1L, properties("name", "Second"), labels(union(new String[] {"One"}, addedLabels)));
            assertNextNode(nodes, 2L, properties("name", "Third"), labels(union(new String[] {"One"}, addedLabels)));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(defaultType, List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                MONITOR);

        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 0L, 1L, defaultType, emptyMap());
            assertNextRelationship(relationships, 1L, 2L, customType, emptyMap());
            assertNextRelationship(relationships, 2L, 1L, defaultType, emptyMap());
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, null, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                MONITOR);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("id", "abc", "name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                ACTUAL,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, 0L, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, 1L, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        InputIterator nodes = input.nodes(EMPTY).iterator();
        try {
            assertThatThrownBy(() -> readNext(nodes)).isInstanceOf(InputException.class);
        } finally {
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Johan", "point", Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2)),
                    labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "someDouble", 1.1d), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "someDouble", 2.2d), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "someDouble", 1.1f), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "someDouble", 2.2f), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2018, 2, 27)), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "date", DateValue.date(2018, 3, 1)), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2006, 2, 14)), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2006, 2, 14)), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2006, 2, 14)), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            // 2005-05-24 22:54:33 == 1116975273000000 epoch micros
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "date", LocalDateTimeValue.localDateTime(2005, 5, 24, 22, 54, 33, 0)),
                    labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", TimeValue.time(13, 37, 0, 0, "+00:00")), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(16, 20, 1, 0, "+00:00")), labels());
            assertNextNode(
                    nodes, 2L, properties("name", "Bob", "time", TimeValue.time(7, 30, 0, 0, "-05:00")), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseUUIDasString() throws Exception {
        var nodeFile = Path.of(getClass().getResource("/parquet/uuid.parquet").toURI());

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("myUUID", "ba576658-d01d-4858-94ff-a97f18be9608"), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseSourceContainingDateAndTimeCorrectly() throws Exception {

        var headerPath =
                Path.of(getClass().getResource("/parquet/datetime_header.csv").toURI());
        var nodePath =
                Path.of(getClass().getResource("/parquet/datetime_data.parquet").toURI());
        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerPath), new FileGroup.NumberedFile(-1, nodePath)))),
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
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseDateRelatedTypesLikeDataImporter() throws Exception {

        var nodePath = Path.of(
                getClass().getResource("/parquet/temporal_types.parquet").toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodePath)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseNullTemporalTypes() throws Exception {

        // Given a parquet file which contains null value column c_timestamp_millis, c_timestamp_micros and c_date
        var nodePath = Path.of(getClass()
                .getResource("/parquet/temporal_nullable_types.parquet")
                .toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodePath)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

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

            assertThat(visitor.properties.stream()
                            .map(InputEntity.Property::keyName)
                            .toList())
                    .doesNotContain("c_time_millis")
                    .doesNotContain("c_timestamp_millis")
                    .doesNotContain("c_date");

            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseNumbersLikeDataImporter() throws Exception {

        var nodePath =
                Path.of(getClass().getResource("/parquet/numeric_types.parquet").toURI());
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodePath)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", TimeValue.time(13, 37, 0, 0, "+02:00")), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", TimeValue.time(16, 20, 1, 0, "+02:00")), labels());
            assertNextNode(
                    nodes, 2L, properties("name", "Bob", "time", TimeValue.time(7, 30, 0, 0, "-05:00")), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes, 0L, properties("name", "Mattias", "time", LocalTimeValue.localTime(13, 37, 0, 0)), labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", LocalTimeValue.localTime(16, 20, 1, 0)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseNumericLocalTimePropertyValues() throws Exception {
        // GIVEN a raw numeric column (no temporal logical type) explicitly mapped to localtime through the header,
        // the number is interpreted as nanoseconds of day
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("time:LocalTime")),
                List.of(new Object[] {0, "Mattias", 52397144072000L}, new Object[] {1, "Johan", 52397000000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(
                    nodes,
                    0L,
                    properties("name", "Mattias", "time", LocalTimeValue.localTime(14, 33, 17, 144072000)),
                    labels());
            assertNextNode(
                    nodes, 1L, properties("name", "Johan", "time", LocalTimeValue.localTime(14, 33, 17, 0)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldParseNumericDateTimePropertyValues() throws Exception {
        // GIVEN a raw numeric column (no temporal logical type) explicitly mapped to datetime through the header,
        // the number is interpreted as epoch microseconds at UTC (sub-second fraction is preserved)
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("time:DateTime")),
                List.of(new Object[] {0, "Mattias", 1116975273123456L}, new Object[] {1, "Johan", 1116975273000000L}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
                            DateTimeValue.datetime(
                                    2005, 5, 24, 22, 54, 33, 123456000, ZoneId.of(ZoneOffset.UTC.getId()))),
                    labels());
            assertNextNode(
                    nodes,
                    1L,
                    properties(
                            "name",
                            "Johan",
                            "time",
                            DateTimeValue.datetime(2005, 5, 24, 22, 54, 33, 0, ZoneId.of(ZoneOffset.UTC.getId()))),
                    labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isFalse();
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, group, 123L, properties("name", "one"), labels());
            assertNextNode(nodes, group, 456L, properties("name", "two"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IdType.class,
            names = {"INTEGER", "STRING"})
    void multipleIdColumns(IdType idType) throws Exception {
        var nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("part1:ID" + (random.nextBoolean() ? "{id-type=int}" : "")),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                .named("part2:ID" + (random.nextBoolean() ? "{id-type=int}" : ""))),
                List.of(new Object[] {123, 456}, new Object[] {3, 6}));
        try (var input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                idType,
                groups,
                MONITOR)) {
            var nodes = input.nodes(EMPTY).iterator();
            assertNextNode(
                    nodes,
                    groups.get(null),
                    "123%s456".formatted(ParquetInput.DELIMITER),
                    properties("part1", 123, "part2", 456),
                    labels());
            assertNextNode(
                    nodes,
                    groups.get(null),
                    "3%s6".formatted(ParquetInput.DELIMITER),
                    properties("part1", 3, "part2", 6),
                    labels());
            assertThat(readNext(nodes)).isFalse();
        }
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                MONITOR);
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
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        STRING,
                        groups,
                        MONITOR))
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
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1), new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                MONITOR);
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(relationships, startGroupName, 123L, endGroupName, 234L, "TYPE", properties());
            assertRelationship(relationships, startGroupName, 345L, endGroupName, 456L, "TYPE", properties());
            assertThat(readNext(relationships)).isFalse();
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
                        List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile1))),
                        Set.of("ENDTHING"),
                        List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                MONITOR);
        var nodesFromIterator = new ArrayList<VisitedNode>();
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThat(readNext(nodes)).isTrue();
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertThat(readNext(nodes)).isTrue();
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertThat(readNext(nodes)).isTrue();
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertThat(readNext(nodes)).isTrue();
            nodesFromIterator.add(VisitedNode.from(visitor));
            assertThat(readNext(nodes)).isFalse();
        }
        assertNextVisitedNode(nodesFromIterator, 234L, groups.get(endGroupName), Set.of("ENDTHING"));
        assertNextVisitedNode(nodesFromIterator, 456L, groups.get(endGroupName), Set.of("ENDTHING"));
        assertNextVisitedNode(nodesFromIterator, 123L, groups.get(startGroupName), Set.of("STARTTHING"));
        assertNextVisitedNode(nodesFromIterator, 345L, groups.get(startGroupName), Set.of("STARTTHING"));
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(relationships, startGroupName, 123L, endGroupName, 234L, "TYPE", properties());
            assertRelationship(relationships, startGroupName, 345L, endGroupName, 456L, "TYPE", properties());
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1), new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
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
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(defaultType, List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                MONITOR);
        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            // THEN
            assertNextRelationship(relationships, 0L, 1L, defaultType, properties("name", "First"));
            assertNextRelationship(relationships, 2L, 3L, defaultType, properties("name", "Second"));
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("other", 10), labels("Person"));
            assertNextNode(nodes, 2L, properties("other", 111), labels("Person"));
            assertNextNode(nodes, 3L, properties("other", 12), labels("Person"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                new Groups(),
                MONITOR);

        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 1L, 2L, "KNOWS", properties("other", 10));
            assertNextRelationship(relationships, 2L, 3L, "KNOWS", properties("other", 111));
            assertNextRelationship(relationships, 3L, 4L, "KNOWS", properties("other", 12));
            assertThat(readNext(relationships)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter('?').build());

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("prop", Values.intArray(new int[] {1, 23})), labels());
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter('?').build());

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties(), labels("Foo", "Bar"));
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("sprop", Values.stringArray("a", "b"), "lprop", Values.longArray(new long[] {10, 20})),
                    labels());
            assertThat(readNext(nodes)).isFalse();
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
                        Set.of("Comment"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, commentHeader),
                                new FileGroup.NumberedFile(-1, commentFile))),
                        Set.of("Person"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, personHeader),
                                new FileGroup.NumberedFile(-1, personFile)))),
                Map.of(
                        "HAS_CREATOR",
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, relationshipHeader),
                                new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            var readNodes = 0;
            while (readNext(nodes)) {
                readNodes++;
            }
            assertThat(readNodes).isEqualTo(8);
            assertThat(readNext(nodes)).isFalse();
        }
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            var readRelationships = 0;
            while (readNext(relationships)) {
                readRelationships++;
            }
            assertThat(readRelationships).isEqualTo(4);
            assertThat(readNext(relationships)).isFalse();
        }
    }

    static Stream<Arguments> shouldImportVectors() {
        return Stream.of(
                Arguments.of(
                        "vector{coordinateType:byte,dimensions:2}", "1;23", Values.int8Vector((byte) 1, (byte) 23)),
                Arguments.of(
                        "vector{coordinateType:short,dimensions:2}", "1;23", Values.int16Vector((short) 1, (short) 23)),
                Arguments.of("vector{coordinateType:int,dimensions:2}", "1;23", Values.int32Vector(1, 23)),
                Arguments.of("vector{coordinateType:long,dimensions:2}", "1;23", Values.int64Vector(1, 23)),
                Arguments.of("vector{coordinateType:float,dimensions:2}", "1;23", Values.float32Vector(1, 23)),
                Arguments.of("vector{coordinateType:double,dimensions:2}", "1;23", Values.float64Vector(1, 23)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldImportVectors(String header, String stringValue, VectorValue expectedValue) throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:%s".formatted(header))),
                Collections.singletonList(new Object[] {1, stringValue}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("vprop", expectedValue), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("shouldImportVectors")
    void shouldImportVectorsWithHeaderFiles(String header, String stringValue, VectorValue expectedValue)
            throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop")),
                Collections.singletonList(new Object[] {1, stringValue}));
        Path headerFile = createHeaderFile(List.of(":ID", "vprop:" + header), List.of(":ID", "vprop"), ";");
        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withDelimiter(';').build());
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("vprop", expectedValue), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldNotUseOverriddenArrayDelimiterForVectors() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{coordinateType:int,dimensions:2}")),
                Collections.singletonList(new Object[] {1, "1;23"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withArrayDelimiter('§').build());
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("vprop", Values.int32Vector(1, 23)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldUseOverriddenVectorDelimiter() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{coordinateType:int,dimensions:2}")),
                Collections.singletonList(new Object[] {1, "1§23"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR,
                Configuration.newBuilder().withVectorDelimiter('§').build());
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("vprop", Values.int32Vector(1, 23)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    static Stream<Arguments> shouldImportVectorsFromListColumn() {
        return Stream.of(
                Arguments.of(
                        "list_int32.parquet",
                        "vector{coordinateType:short,dimensions:3}",
                        Values.int16Vector((short) 123, (short) 234, (short) 345)),
                Arguments.of(
                        "list_int32.parquet",
                        "vector{coordinateType:int,dimensions:3}",
                        Values.int32Vector(123, 234, 345)),
                Arguments.of(
                        "list_int32.parquet",
                        "vector{coordinateType:long,dimensions:3}",
                        Values.int64Vector(123L, 234L, 345L)),
                Arguments.of(
                        "list_int32.parquet",
                        "vector{coordinateType:float,dimensions:3}",
                        Values.float32Vector(123f, 234f, 345f)),
                Arguments.of(
                        "list_int32.parquet",
                        "vector{coordinateType:double,dimensions:3}",
                        Values.float64Vector(123d, 234d, 345d)),
                Arguments.of(
                        "list_int64.parquet",
                        "vector{coordinateType:long,dimensions:3}",
                        Values.int64Vector(123L, 234L, 345L)),
                Arguments.of(
                        "list_float.parquet",
                        "vector{coordinateType:float,dimensions:3}",
                        Values.float32Vector(1.01f, 2.21f, 3.23f)),
                Arguments.of(
                        "list_double.parquet",
                        "vector{coordinateType:double,dimensions:3}",
                        Values.float64Vector(1.01d, 2.21d, 3.23d)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldImportVectorsFromListColumn(String fileName, String header, VectorValue expectedValue) throws Exception {
        var fileUrl = getClass().getResource("/parquet/" + fileName);
        var nodeFile = Path.of(fileUrl.toURI());
        // The vector header contains commas (which clash with the CSV delimiter), so the field is quoted.
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "\"aList:%s\"".formatted(header), ":Label"),
                List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(
                    nodes, 123L, properties("aList", expectedValue, "name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorDataDimensionMismatchFromListColumn() throws Exception {
        // list_int32.parquet has 3 elements; the header asserts 5 dimensions.
        var fileUrl = getClass().getResource("/parquet/list_int32.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "\"aList:vector{coordinateType:int,dimensions:5}\"", ":Label"),
                List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Header specified 5 dimensions, but vector has 3 dimensions");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldConvertInRangeIntegerColumnToByteProperty() throws Exception {
        // A native parquet INT32 column with an explicit ':byte' header forces convertType through
        // the BYTE switch arm with a real Number, exercising the safeCastLongToByte happy path.
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("byteProp:byte")),
                Collections.singletonList(new Object[] {1, 42}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("byteProp", (byte) 42), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    static Stream<Arguments> shouldConvertNativeNumericColumnViaNumberPath() {
        return Stream.of(
                Arguments.of(PrimitiveType.PrimitiveTypeName.INT32, "int", 42, 42),
                Arguments.of(PrimitiveType.PrimitiveTypeName.INT64, "int", 42L, 42),
                Arguments.of(PrimitiveType.PrimitiveTypeName.INT32, "short", 42, (short) 42),
                Arguments.of(PrimitiveType.PrimitiveTypeName.INT32, "long", 42, 42L),
                Arguments.of(PrimitiveType.PrimitiveTypeName.INT64, "long", 42L, 42L),
                Arguments.of(PrimitiveType.PrimitiveTypeName.FLOAT, "float", 1.5f, 1.5f),
                Arguments.of(PrimitiveType.PrimitiveTypeName.DOUBLE, "double", 1.5d, 1.5d));
    }

    @ParameterizedTest
    @MethodSource
    void shouldConvertNativeNumericColumnViaNumberPath(
            PrimitiveType.PrimitiveTypeName parquetType, String headerType, Object sourceValue, Object expectedValue)
            throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(parquetType).named("prop:" + headerType)),
                Collections.singletonList(new Object[] {1, sourceValue}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("prop", expectedValue), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    static Stream<Arguments> shouldReportOverflowWhenNarrowingNativeNumericColumn() {
        return Stream.of(
                Arguments.of("int", Long.MAX_VALUE, "Value " + Long.MAX_VALUE + " is too big to be represented as int"),
                Arguments.of("short", 100_000L, "Value 100000 is too big to be represented as short"));
    }

    @ParameterizedTest
    @MethodSource
    void shouldReportOverflowWhenNarrowingNativeNumericColumn(
            String headerType, long sourceValue, String expectedMessageFragment) throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("prop:" + headerType)),
                Collections.singletonList(new Object[] {1, sourceValue}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining(expectedMessageFragment);
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldReportOverflowWhenNarrowingListColumnToByteVector() throws Exception {
        // list_int32.parquet contains Integer 234, which does not fit a signed byte.
        // The Number short-circuit in convertType must surface a clear overflow error.
        var fileUrl = getClass().getResource("/parquet/list_int32.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "\"aList:vector{coordinateType:byte,dimensions:3}\"", ":Label"),
                List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Value 234 is too big to be represented as byte");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithNonNumericValueFromListColumn() throws Exception {
        // list.parquet stores a List<String> ["a", "b", "c"]; mapping it to a numeric vector must fail.
        var fileUrl = getClass().getResource("/parquet/list.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "\"aList:vector{coordinateType:int,dimensions:3}\"", ":Label"),
                List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("could not convert")
                    .hasMessageContaining("VECTOR");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnNestedListColumn() throws Exception {
        // list_nested_int32.parquet stores a List<List<Integer>>; Neo4j properties cannot hold arrays of
        // arrays, so the reader must reject it rather than silently flatten it into a single list.
        var fileUrl = getClass().getResource("/parquet/list_nested_int32.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "aList:int[]", ":Label"), List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Nested list columns are not supported");
        }
    }

    @Test
    void shouldFailImportOnVectorFromNestedListColumn() throws Exception {
        // A nested list mapped to a vector must also be rejected rather than silently flattened.
        var fileUrl = getClass().getResource("/parquet/list_nested_int32.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "\"aList:vector{coordinateType:int,dimensions:3}\"", ":Label"),
                List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Nested list columns are not supported");
        }
    }

    @Test
    void shouldIgnoreNestedListColumnWhenNotMapped() throws Exception {
        // A nested list column that is not imported (mapped to IGNORE) must not break the import, even
        // though Neo4j properties cannot hold arrays of arrays - it is read and discarded, never stored.
        var fileUrl = getClass().getResource("/parquet/list_nested_int32.parquet");
        var nodeFile = Path.of(fileUrl.toURI());
        Path headerFile = createHeaderFile(
                List.of(":ID", "name:string", "aList:IGNORE", ":Label"), List.of(":ID", "name", "aList", ":Label"));

        Input input = createParquetInput(
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, headerFile), new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    private static Stream<Arguments> dimensionMismatchedVectors() {
        return Stream.of(
                Arguments.of("vector{coordinateType:byte,dimensions:3}", "123;-2"),
                Arguments.of("vector{coordinateType:short,dimensions:3}", "123;-2"),
                Arguments.of("vector{coordinateType:int,dimensions:3}", "123;-2"),
                Arguments.of("vector{coordinateType:long,dimensions:3}", "123;-2"),
                Arguments.of("vector{coordinateType:float,dimensions:3}", "123;-2"),
                Arguments.of("vector{coordinateType:double,dimensions:3}", "123;-2"));
    }

    @ParameterizedTest
    @MethodSource("dimensionMismatchedVectors")
    void shouldFailImportOnVectorDataDimensionMismatch(String header, String stringValue) throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:%s".formatted(header))),
                Collections.singletonList(new Object[] {1, stringValue}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Header specified 3 dimensions, but vector has 2 dimensions");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorDataDimensionMissing() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{coordinateType:int}")),
                Collections.singletonList(new Object[] {1, "1;23"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("vector must specify dimensions");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorDataCoordinateTypeMissing() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions:2}")),
                Collections.singletonList(new Object[] {1, "1;23"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("vector must specify coordinate type");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorDataDimensionAndCoordinateTypeMissing() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector")),
                Collections.singletonList(new Object[] {1, "1;23"}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("vector must specify");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorDataCoordinateTypeSpecifiedTwice() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{coordinateType:byte, coordinateType:int}")),
                Collections.singletonList(new Object[] {1, "1;23"}));
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        INTEGER,
                        groups,
                        MONITOR))
                .hasMessageContaining("Duplicate field 'coordinateType'");
    }

    @Test
    void shouldFailImportOnVectorDataCoordinateTypeSpecifiedTwiceWithDifferentCasing() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{coordinateType:byte, coordinatetype:int}")),
                Collections.singletonList(new Object[] {1, "1;23"}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Duplicate field 'coordinateType'");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "vector{coordinateType:byte,dimensions:3}",
                "vector{coordinateType:short,dimensions:3}",
                "vector{coordinateType:int,dimensions:3}",
                "vector{coordinateType:long,dimensions:3}",
                "vector{coordinateType:float,dimensions:3}",
                "vector{coordinateType:double,dimensions:3}"
            })
    void shouldWriteNullForVectorsWhenInputIsEmpty(String header) throws Exception {
        // GIVEN
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:%s".formatted(header))),
                Collections.singletonList(new Object[] {1, ""}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties(), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithMissingValue() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions: 3, coordinateType:int}")),
                Collections.singletonList(new Object[] {1, "1;;23"}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("could not convert 1;;23 to VECTOR");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithMissingValueLast() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions: 3, coordinateType:int}")),
                Collections.singletonList(new Object[] {1, "1;23;"}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("could not convert 1;23; to VECTOR");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithNonNumericValue() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions: 3, coordinateType:int}")),
                Collections.singletonList(new Object[] {1, "1;abc;23"}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("could not convert 1;abc;23 to VECTOR");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    static Stream<Arguments> shouldAllowDifferentCapitalizationOfVectorInfo() {
        return Stream.of(
                Arguments.of(
                        "vEcToR{coordinateType:byte,dimensions:2}", "1;23", Values.int8Vector((byte) 1, (byte) 23)),
                Arguments.of(
                        "vector{Coordinatetype:sHort,dimensions:2}", "1;23", Values.int16Vector((short) 1, (short) 23)),
                Arguments.of("vector{coordinateType:inT,DIMENSIONS:2}", "1;23", Values.int32Vector(1, 23)),
                Arguments.of("vector{coordinateType:lOng,dImensions:2}", "1;23", Values.int64Vector(1, 23)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldAllowDifferentCapitalizationOfVectorInfo(String header, String stringValue, VectorValue expectedValue)
            throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:%s".formatted(header))),
                Collections.singletonList(new Object[] {1, stringValue}));
        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("vprop", expectedValue), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithUnknownPropertyType() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions: 3, coordinateType:pyte}")),
                Collections.singletonList(new Object[] {1, "1;2;23"}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("pyte is not a valid coordinate type.");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithNonIntegerDimension() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions: three, coordinateType:byte}")),
                Collections.singletonList(new Object[] {1, "1;2;23"}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("three is not a valid value for dimensions.");
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @Test
    void shouldFailImportOnVectorWithTooLargeDimension() throws Exception {
        Path nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(":ID"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("vprop:vector{dimensions: 5000, coordinateType:byte}")),
                Collections.singletonList(
                        new Object[] {1, Stream.generate(() -> "1").limit(5000).collect(Collectors.joining(";"))}));

        Input input = createParquetInput(
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);
        assertThat(input.containsVectorData()).isTrue();

        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertThatThrownBy(() -> readNext(nodes))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("Invalid vector dimensions: 5000");
            assertThat(readNext(nodes)).isFalse();
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                MONITOR);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes,
                    2L,
                    properties("sprop", Values.stringArray("a", "b"), "lprop", Values.longArray(new long[] {10, 20})),
                    labels());
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        INTEGER,
                        groups,
                        MONITOR))
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
                        Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
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
                        Map.of(
                                Set.of(""),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile1),
                                        new FileGroup.NumberedFile(-1, nodeFile2)))),
                        Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
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
                        Map.of(
                                Set.of(""),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile1),
                                        new FileGroup.NumberedFile(-1, nodeFile2)))),
                        Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
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
                Map.of(
                        Set.of("someLabel"),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1), new FileGroup.NumberedFile(-1, nodeFile2)))),
                Map.of("someType", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                monitor);
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
                Map.of(Set.of("test"), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                INTEGER,
                groups,
                monitor);

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
                Map.of(),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                monitor);

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
                Map.of(),
                Map.of("someType", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
                INTEGER,
                groups,
                monitor);
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
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
                        Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(-1, relationshipFile)))),
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
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
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1), new FileGroup.NumberedFile(-1, nodeFile2)))),
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
                Map.of(
                        Set.of(""),
                        List.of(new FileGroup(
                                new FileGroup.NumberedFile(-1, nodeFile1), new FileGroup.NumberedFile(-1, nodeFile2)))),
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
                Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                Map.of(),
                STRING,
                groups,
                new ParquetMonitor(System.out))) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));
            var schema = input.referencedNodeSchema(tokenHolders);

            // then
            assertThat(schema).containsExactlyInAnyOrderEntriesOf(Map.of("My Group", SchemaDescriptors.forLabel(2, 4)));
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(EMPTY).iterator()) {
            // then
            assertNextNode(nodes, groups.get("new-group"), 123, properties("id", 123, "prop", "val"), labels());
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
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
            assertThat(readNext(nodes)).isFalse();
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        STRING,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(InputException.class)
                .hasMessageContaining("Multiple :ID columns share the same property name");
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
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
                        Map.of(),
                        INTEGER,
                        groups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("referring to different groups");
    }

    @Test
    void shouldHandleCompositeKeySupportInRelationships() throws Exception {
        // given
        List<Object[]> data = List.of(
                new Object[] {"John", "Smith", "London", "Person"},
                new Object[] {"Bob", "Smith", "New York", "Person"});

        var nodeFileHeader = createHeaderFile(
                List.of(
                        "firstName:ID(startGroup){id-type:string}",
                        "lastName:ID(startGroup){id-type:string}",
                        ":IGNORE",
                        ":LABEL"),
                List.of("firstName", "lastName", "city", ":LABEL"));
        var nodeFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("firstName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lastName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("city"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                data);

        var nodeFile2Header = createHeaderFile(List.of(":ID(endGroup)", ":LABEL"), List.of("name", ":LABEL"));
        var nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.of(new Object[] {"London", "City"}, new Object[] {"New York", "City"}));

        var relationshipHeader = createHeaderFile(
                List.of(":START_ID(startGroup)", ":START_ID(startGroup)", ":END_ID(endGroup)", ":IGNORE"),
                List.of("firstName", "lastName", "city", ":LABEL"));
        var relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("firstName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lastName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("city"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":IGNORE")),
                data);

        var idGroups = new Groups();
        idGroups.getOrCreate("startGroup");
        idGroups.getOrCreate("endGroup");

        // when
        var nodeFiles = new LinkedHashMap<Set<String>, List<FileGroup>>();
        nodeFiles.put(
                Set.of("Person"),
                List.of(new FileGroup(
                        new FileGroup.NumberedFile(-1, nodeFileHeader), new FileGroup.NumberedFile(-1, nodeFile))));
        nodeFiles.put(
                Set.of("City"),
                List.of(new FileGroup(
                        new FileGroup.NumberedFile(-1, nodeFile2Header), new FileGroup.NumberedFile(-1, nodeFile2))));
        try (var input = createParquetInput(
                        nodeFiles,
                        Map.of(
                                "LIVES_IN",
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, relationshipHeader),
                                        new FileGroup.NumberedFile(-1, relationshipFile)))),
                        STRING,
                        idGroups,
                        new ParquetMonitor(System.out));
                var nodes = input.nodes(Collector.STRICT).iterator();
                var relationships = input.relationships(Collector.STRICT).iterator(); ) {
            // then
            assertNextNode(
                    nodes,
                    idGroups.get("startGroup"),
                    "John%sSmith".formatted(ParquetInput.DELIMITER),
                    properties("firstName", "John", "lastName", "Smith"),
                    Set.of("Person"));
            assertNextNode(
                    nodes,
                    idGroups.get("startGroup"),
                    "Bob%sSmith".formatted(ParquetInput.DELIMITER),
                    properties("firstName", "Bob", "lastName", "Smith"),
                    Set.of("Person"));
            assertNextNode(nodes, idGroups.get("endGroup"), "London", properties(), Set.of("City"));
            assertNextNode(nodes, idGroups.get("endGroup"), "New York", properties(), Set.of("City"));
            assertThat(readNext(nodes)).isFalse();

            assertRelationship(
                    relationships,
                    idGroups.get("startGroup"),
                    "John%sSmith".formatted(ParquetInput.DELIMITER),
                    idGroups.get("endGroup"),
                    "London",
                    "LIVES_IN",
                    properties());
            assertRelationship(
                    relationships,
                    idGroups.get("startGroup"),
                    "Bob%sSmith".formatted(ParquetInput.DELIMITER),
                    idGroups.get("endGroup"),
                    "New York",
                    "LIVES_IN",
                    properties());
            assertThat(readNext(relationships)).isFalse();
        }
    }

    @Test
    void shouldFailWhenStartIdColumnsReferToDifferentGroups() throws Exception {
        // given
        var nodeFile1Header = createHeaderFile(
                List.of("firstName:ID(groupA){id-type:string}", "lastName:ID(groupA){id-type:string}", ":LABEL"),
                List.of("firstName", "lastName", ":LABEL"));
        var nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("firstName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lastName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"John", "Smith", "Person"}));

        var nodeFile2Header = createHeaderFile(List.of(":ID(groupB)", ":LABEL"), List.of("name", ":LABEL"));
        var nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"London", "City"}));

        var relationshipHeader = createHeaderFile(
                List.of(":START_ID(groupA)", ":START_ID(groupB)", ":END_ID(groupB)"),
                List.of("firstName", "lastName", "name"));
        var relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("firstName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lastName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.<Object[]>of(new Object[] {"John", "Smith", "London"}));

        var idGroups = new Groups();
        idGroups.getOrCreate("groupA");
        idGroups.getOrCreate("groupB");

        // when/then
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(
                                Set.of("Person"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile1Header),
                                        new FileGroup.NumberedFile(-1, nodeFile1))),
                                Set.of("City"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile2Header),
                                        new FileGroup.NumberedFile(-1, nodeFile2)))),
                        Map.of(
                                "LIVES_IN",
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, relationshipHeader),
                                        new FileGroup.NumberedFile(-1, relationshipFile)))),
                        STRING,
                        idGroups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(":START_ID columns")
                .hasMessageContaining("referring to different groups");
    }

    @Test
    void shouldFailWhenEndIdColumnsReferToDifferentGroups() throws Exception {
        // given
        var nodeFile1Header = createHeaderFile(List.of(":ID(groupA)", ":LABEL"), List.of("name", ":LABEL"));
        var nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"John", "Person"}));

        var nodeFile2Header = createHeaderFile(List.of(":ID(groupB)", ":LABEL"), List.of("name", ":LABEL"));
        var nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"London", "City"}));

        var nodeFile3Header = createHeaderFile(List.of(":ID(groupC)", ":LABEL"), List.of("name", ":LABEL"));
        var nodeFile3 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"Paris", "Town"}));

        var relationshipHeader = createHeaderFile(
                List.of(":START_ID(groupA)", ":END_ID(groupB)", ":END_ID(groupC)"),
                List.of("startName", "endName1", "endName2"));
        var relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("startName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("endName1"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("endName2")),
                List.<Object[]>of(new Object[] {"John", "London", "Paris"}));

        var idGroups = new Groups();
        idGroups.getOrCreate("groupA");
        idGroups.getOrCreate("groupB");
        idGroups.getOrCreate("groupC");

        // when/then
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(
                                Set.of("Person"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile1Header),
                                        new FileGroup.NumberedFile(-1, nodeFile1))),
                                Set.of("City"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile2Header),
                                        new FileGroup.NumberedFile(-1, nodeFile2))),
                                Set.of("Town"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile3Header),
                                        new FileGroup.NumberedFile(-1, nodeFile3)))),
                        Map.of(
                                "VISITS",
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, relationshipHeader),
                                        new FileGroup.NumberedFile(-1, relationshipFile)))),
                        STRING,
                        idGroups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(":END_ID columns")
                .hasMessageContaining("referring to different groups");
    }

    @Test
    void shouldFailWhenStartIdColumnCountDoesNotMatchNodeGroupArity() throws Exception {
        // given: Person node group has 2 ID columns (firstName + lastName)
        var nodeFile1Header = createHeaderFile(
                List.of("firstName:ID(groupA){id-type:string}", "lastName:ID(groupA){id-type:string}", ":LABEL"),
                List.of("firstName", "lastName", ":LABEL"));
        var nodeFile1 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("firstName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lastName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"John", "Smith", "Person"}));

        var nodeFile2Header = createHeaderFile(List.of(":ID(groupB)", ":LABEL"), List.of("name", ":LABEL"));
        var nodeFile2 = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named(":LABEL")),
                List.<Object[]>of(new Object[] {"London", "City"}));

        // Relationship has 3 START_ID columns for groupA, but groupA only has 2 ID columns
        var relationshipHeader = createHeaderFile(
                List.of(":START_ID(groupA)", ":START_ID(groupA)", ":START_ID(groupA)", ":END_ID(groupB)"),
                List.of("firstName", "lastName", "extra", "name"));
        var relationshipFile = createParquetFile(
                List.of(
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("firstName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("lastName"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("extra"),
                        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                .as(LogicalTypeAnnotation.stringType())
                                .named("name")),
                List.<Object[]>of(new Object[] {"John", "Smith", "extra", "London"}));

        var idGroups = new Groups();
        idGroups.getOrCreate("groupA");
        idGroups.getOrCreate("groupB");

        // when/then
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(
                                Set.of("Person"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile1Header),
                                        new FileGroup.NumberedFile(-1, nodeFile1))),
                                Set.of("City"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeFile2Header),
                                        new FileGroup.NumberedFile(-1, nodeFile2)))),
                        Map.of(
                                "LIVES_IN",
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, relationshipHeader),
                                        new FileGroup.NumberedFile(-1, relationshipFile)))),
                        STRING,
                        idGroups,
                        new ParquetMonitor(System.out)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Number of :START_ID columns")
                .hasMessageContaining("does not match");
    }

    @Test
    void shouldFailOnNonParquetFile() throws Exception {
        Path nodeFile = createNonParquetFile();
        assertThatThrownBy(() -> createParquetInput(
                        Map.of(Set.of(""), List.of(new FileGroup(new FileGroup.NumberedFile(-1, nodeFile)))),
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
                                Set.of("Person"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeHeaderFile1),
                                        new FileGroup.NumberedFile(-1, commonFile))),
                                Set.of("Band"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeHeaderFile2),
                                        new FileGroup.NumberedFile(-1, nodeFile2)))),
                        Map.of(
                                "MEMBER_OF",
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, relHeaderFile1),
                                        new FileGroup.NumberedFile(-1, commonFile)))),
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
            assertThat(readNext(rels)).isFalse();
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
                                Set.of("Person"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeHeaderFile1),
                                        new FileGroup.NumberedFile(-1, commonFile))),
                                Set.of("Band"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeHeaderFile2),
                                        new FileGroup.NumberedFile(-1, nodeFile2))),
                                Set.of("Group"),
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, nodeHeaderFile3),
                                        new FileGroup.NumberedFile(-1, commonFile)))),
                        Map.of(
                                "MEMBER_OF",
                                List.of(new FileGroup(
                                        new FileGroup.NumberedFile(-1, relHeaderFile1),
                                        new FileGroup.NumberedFile(-1, commonFile),
                                        new FileGroup.NumberedFile(-1, relHeaderFile2),
                                        new FileGroup.NumberedFile(-1, commonFile)))),
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
            assertThat(readNext(rels)).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("idMapperTypeCases")
    void idMapperType(String name, List<ParquetIdMapperTypeFile> files, IdType globalIdType, IdType expectedIdType)
            throws Exception {
        var nodeFiles = new LinkedHashMap<Set<String>, List<FileGroup>>();
        for (int i = 0; i < files.size(); i++) {
            var spec = files.get(i);
            var path = createParquetFile(spec.columns(), List.<Object[]>of(spec.row()));
            nodeFiles.put(Set.of("L" + i), List.of(new FileGroup(new FileGroup.NumberedFile(-1, path))));
        }
        try (var input = createParquetInput(nodeFiles, Map.of(), globalIdType, groups, MONITOR)) {
            assertThat(input.idType()).isEqualTo(expectedIdType);
        }
    }

    private static Stream<Arguments> idMapperTypeCases() {
        return Stream.of(
                Arguments.of(
                        "integer column with string id-type override falls back to string",
                        List.of(new ParquetIdMapperTypeFile(
                                List.of(stringColumn("id:ID(g1){id-type:string}"), stringColumn("prop")),
                                new Object[] {"two", "val"})),
                        INTEGER,
                        STRING),
                Arguments.of(
                        "string column with int id-type override yields integer",
                        List.of(new ParquetIdMapperTypeFile(
                                List.of(intColumn("id:ID(g1){id-type:int}"), stringColumn("prop")),
                                new Object[] {123, "val"})),
                        STRING,
                        INTEGER),
                Arguments.of(
                        "single int id column with global integer id-type",
                        List.of(new ParquetIdMapperTypeFile(
                                List.of(longColumn("id:ID(g1)"), stringColumn("prop")), new Object[] {123L, "val"})),
                        INTEGER,
                        INTEGER),
                Arguments.of(
                        "single string id column with global string id-type",
                        List.of(new ParquetIdMapperTypeFile(
                                List.of(stringColumn("id:ID(g1)"), stringColumn("prop")), new Object[] {"abc", "val"})),
                        STRING,
                        STRING),
                Arguments.of(
                        "composite id columns yield string",
                        List.of(new ParquetIdMapperTypeFile(
                                List.of(
                                        intColumn("id1:ID(g1){id-type:int}"),
                                        intColumn("id2:ID(g1){id-type:int}"),
                                        stringColumn("prop")),
                                new Object[] {1, 2, "val"})),
                        INTEGER,
                        STRING),
                Arguments.of(
                        "ACTUAL global id-type stays ACTUAL",
                        List.of(new ParquetIdMapperTypeFile(
                                List.of(longColumn("id:ID(g1)"), stringColumn("prop")), new Object[] {1L, "val"})),
                        ACTUAL,
                        ACTUAL),
                Arguments.of(
                        "any node file with non-long id column yields string",
                        List.of(
                                new ParquetIdMapperTypeFile(
                                        List.of(intColumn("id:ID(g1){id-type:int}"), stringColumn("prop")),
                                        new Object[] {123, "val"}),
                                new ParquetIdMapperTypeFile(
                                        List.of(stringColumn("id:ID(g2){id-type:string}"), stringColumn("prop")),
                                        new Object[] {"abc", "val"})),
                        INTEGER,
                        STRING),
                Arguments.of(
                        "all node files with single long id column yield integer",
                        List.of(
                                new ParquetIdMapperTypeFile(
                                        List.of(longColumn("id:ID(g1)"), stringColumn("prop")),
                                        new Object[] {1L, "val"}),
                                new ParquetIdMapperTypeFile(
                                        List.of(longColumn("id:ID(g2){id-type:long}"), stringColumn("prop")),
                                        new Object[] {2L, "val"})),
                        INTEGER,
                        INTEGER));
    }

    private record ParquetIdMapperTypeFile(List<org.apache.parquet.schema.Type> columns, Object[] row) {}

    private static org.apache.parquet.schema.Type stringColumn(String name) {
        return Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(name);
    }

    private static org.apache.parquet.schema.Type intColumn(String name) {
        return Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(name);
    }

    private static org.apache.parquet.schema.Type longColumn(String name) {
        return Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(name);
    }

    private static ParquetInput createParquetInput(
            Map<Set<String>, List<FileGroup>> nodeFiles,
            Map<String, List<FileGroup>> relationshipFiles,
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
            Map<Set<String>, List<FileGroup>> nodeFiles,
            Map<String, List<FileGroup>> relationshipFiles,
            IdType idType,
            Groups idGroups,
            ParquetMonitor parquetMonitor,
            Configuration csvConfig) {
        return new ParquetInput(
                nodeFiles, relationshipFiles, ResolvedSchemaCommands.of(), idType, csvConfig, idGroups, parquetMonitor);
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

    /**
     * Writes a parquet file with a required INT32 {@code :ID} column and a native 3-level {@code LIST<string>} column,
     * used to exercise reading list-typed columns whose elements need explicit header type mapping (e.g. point/duration
     * which have no native parquet representation). {@code parquet-floor}'s writer only handles primitive columns, so
     * the parquet-mr example writer is used here instead.
     */
    private Path createStringListParquetFile(String listColumnName, List<String> listValues) throws Exception {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named(":ID")
                .optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(listColumnName)
                .named("something");
        var listGroupType = schema.getType(listColumnName).asGroupType();
        var repeatedName = listGroupType.getType(0).getName();
        var elementName = listGroupType.getType(0).asGroupType().getType(0).getName();

        Path path = directory.file("test%d.parquet".formatted(parquetCounter.getAndIncrement()));
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
                .withType(schema)
                .build()) {
            var record = new SimpleGroup(schema);
            record.add(":ID", 1);
            var listGroup = record.addGroup(listColumnName);
            for (String value : listValues) {
                listGroup.addGroup(repeatedName).add(elementName, value);
            }
            writer.write(record);
        }
        return path;
    }

    /**
     * Writes a parquet file with a required INT32 {@code :ID} column and a native 3-level {@code LIST<int64>} column
     * with no temporal logical annotation, used to exercise reading a list of raw numeric elements that are mapped to
     * a temporal array through the header (so each element flows through the {@code Number} branches of convertType).
     */
    private Path createLongListParquetFile(String listColumnName, List<Long> listValues) throws Exception {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named(":ID")
                .optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.INT64)
                .named(listColumnName)
                .named("something");
        var listGroupType = schema.getType(listColumnName).asGroupType();
        var repeatedName = listGroupType.getType(0).getName();
        var elementName = listGroupType.getType(0).asGroupType().getType(0).getName();

        Path path = directory.file("test%d.parquet".formatted(parquetCounter.getAndIncrement()));
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
                .withType(schema)
                .build()) {
            var record = new SimpleGroup(schema);
            record.add(":ID", 1);
            var listGroup = record.addGroup(listColumnName);
            for (Long value : listValues) {
                listGroup.addGroup(repeatedName).add(elementName, value);
            }
            writer.write(record);
        }
        return path;
    }

    /**
     * Writes a parquet file with a required INT32 {@code :ID} column and a single {@code INTERVAL} column
     * (a 12-byte FIXED_LEN_BYTE_ARRAY), used to verify that Parquet intervals are read as Neo4j durations.
     */
    private Path createIntervalParquetFile(String columnName, int months, int days, int millis) throws Exception {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named(":ID")
                .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(12)
                .as(LogicalTypeAnnotation.intervalType())
                .named(columnName)
                .named("something");
        Path path = directory.file("test%d.parquet".formatted(parquetCounter.getAndIncrement()));
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
                .withType(schema)
                .build()) {
            var record = new SimpleGroup(schema);
            record.add(":ID", 1);
            record.add(columnName, Binary.fromConstantByteArray(intervalBytes(months, days, millis)));
            writer.write(record);
        }
        return path;
    }

    /**
     * Writes a parquet file with a required INT32 {@code :ID} column and a native {@code LIST} of {@code INTERVAL}
     * elements, used to verify that lists of Parquet intervals are read as Neo4j duration arrays.
     */
    private Path createIntervalListParquetFile(String columnName, List<int[]> intervals) throws Exception {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named(":ID")
                .optionalList()
                .optionalElement(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(12)
                .as(LogicalTypeAnnotation.intervalType())
                .named(columnName)
                .named("something");
        var listGroupType = schema.getType(columnName).asGroupType();
        var repeatedName = listGroupType.getType(0).getName();
        var elementName = listGroupType.getType(0).asGroupType().getType(0).getName();

        Path path = directory.file("test%d.parquet".formatted(parquetCounter.getAndIncrement()));
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
                .withType(schema)
                .build()) {
            var record = new SimpleGroup(schema);
            record.add(":ID", 1);
            var listGroup = record.addGroup(columnName);
            for (int[] interval : intervals) {
                listGroup
                        .addGroup(repeatedName)
                        .add(
                                elementName,
                                Binary.fromConstantByteArray(intervalBytes(interval[0], interval[1], interval[2])));
            }
            writer.write(record);
        }
        return path;
    }

    private static byte[] intervalBytes(int months, int days, int millis) {
        return ByteBuffer.allocate(12)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(months)
                .putInt(days)
                .putInt(millis)
                .array();
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
        assertThat(readNext(relationship)).isTrue();
        for (int i = 0; i < startNodes.size(); i++) {
            var startNode = startNodes.get(i);
            var endNode = endNodes.get(i);
            var type = types.get(i);
            var properties = propertiess.get(i);
            try {
                assertThat(visitor.startIdGroup).isEqualTo(groups.get(null));
                assertThat(visitor.startId()).isEqualTo(startNode);
                assertThat(visitor.endIdGroup).isEqualTo(groups.get(null));
                assertThat(visitor.endId()).isEqualTo(endNode);
                assertThat(visitor.stringType).isEqualTo(type);
                assertPropertiesEquals(properties, visitor.propertiesAsMap());
                success = true;
            } catch (AssertionFailedError e) {
                lastError = e;
            }
        }
        if (!success) {
            fail("", lastError);
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
        assertThat(readNext(data)).isTrue();
        assertThat(visitor.startIdGroup).isEqualTo(startNodeGroup);
        assertThat(visitor.startId()).isEqualTo(startNode);
        assertThat(visitor.endIdGroup).isEqualTo(endNodeGroup);
        assertThat(visitor.endId()).isEqualTo(endNode);
        assertThat(visitor.stringType).isEqualTo(type);
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
        assertThat(readNext(data)).isTrue();
        assertThat(visitor.startIdGroup.name()).isEqualTo(startNodeGroupName);
        assertThat(visitor.startId()).isEqualTo(startNode);
        assertThat(visitor.endIdGroup.name()).isEqualTo(endNodeGroupName);
        assertThat(visitor.endId()).isEqualTo(endNode);
        assertThat(visitor.stringType).isEqualTo(type);
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
        assertThat(readNext(data)).isTrue();
        assertThat(visitor.idGroup).isEqualTo(group);
        assertThat(visitor.id()).isEqualTo(id);
        assertThat(asSet(visitor.labels())).hasSameElementsAs(labels);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertNextNodeWithoutGroupAndIdCheck(
            InputIterator data, Map<String, Object> properties, Set<String> labels) throws IOException {
        assertThat(readNext(data)).isTrue();
        assertThat(asSet(visitor.labels())).hasSameElementsAs(labels);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
    }

    private void assertPropertiesEquals(Map<String, Object> expected, Map<String, Object> actual) {
        // Do this more complicated assert to handle primitive array equality
        assertThat(primitiveArraysAsLists(actual)).containsExactlyInAnyOrderEntriesOf(primitiveArraysAsLists(expected));
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

    private static Stream<Arguments> numericListTemporalTypes() {
        return Stream.of(
                Arguments.of(
                        "date[]",
                        List.of(18262L, 18793L),
                        List.of(LocalDate.of(2020, 1, 1), LocalDate.of(2021, 6, 15))),
                Arguments.of(
                        "time[]",
                        List.of(52397144072000L, 0L),
                        List.of(
                                OffsetTime.of(14, 33, 17, 144072000, ZoneOffset.UTC),
                                OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC))),
                Arguments.of(
                        "localtime[]",
                        List.of(52397144072000L, 0L),
                        List.of(LocalTime.of(14, 33, 17, 144072000), LocalTime.of(0, 0))),
                Arguments.of(
                        "datetime[]",
                        List.of(1116975273123456L, 0L),
                        List.of(
                                ZonedDateTime.of(2005, 5, 24, 22, 54, 33, 123456000, ZoneOffset.UTC),
                                ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))),
                Arguments.of(
                        "localdatetime[]",
                        List.of(1116975273123456L, 0L),
                        List.of(
                                LocalDateTime.of(2005, 5, 24, 22, 54, 33, 123456000),
                                LocalDateTime.of(1970, 1, 1, 0, 0))));
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
