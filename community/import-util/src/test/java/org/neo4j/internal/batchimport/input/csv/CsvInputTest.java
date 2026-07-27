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
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.batchimport.api.input.ApplicationMode.CREATE;
import static org.neo4j.batchimport.api.input.ApplicationMode.DELETE;
import static org.neo4j.batchimport.api.input.ApplicationMode.UPDATE;
import static org.neo4j.batchimport.api.input.Collector.EMPTY;
import static org.neo4j.batchimport.api.input.IdType.ACTUAL;
import static org.neo4j.batchimport.api.input.IdType.INTEGER;
import static org.neo4j.batchimport.api.input.IdType.STRING;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.csv.reader.Configuration.TABS;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.internal.batchimport.input.csv.CsvInput.NO_MONITOR;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.internal.helpers.ArrayUtil.union;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.function.Predicates;
import org.neo4j.internal.batchimport.input.DuplicateHeaderException;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.HeaderException;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.csv.Header.Monitor;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.collection.Pair;
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
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
@RandomSupportExtension
class CsvInputTest {
    private static final int NUMBER_OF_ESTIMATE_THREADS = 4;
    private static final PropertySizeCalculator PROPERTY_SIZE_CALCULATOR = (values, cursorContext, memoryTracker) -> 0;

    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory directory;

    private final Extractors extractors = new Extractors(',', ',');

    private final InputEntity visitor = new InputEntity();
    private final Groups groups = new Groups();
    private final Group globalGroup = groups.getOrCreate(null);
    private InputChunk chunk;
    private InputIterator referenceData;

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldProvideNodesFromCsvInput(MultilineSetting setting) throws Exception {
        // GIVEN
        IdType idType = INTEGER;
        Iterable<DataFactory> data = dataIterable(data("123,Mattias Persson,HACKER"));
        Input input = new CsvInput(
                data,
                header(
                        entry(null, Type.ID, CsvInput.idExtractor(idType, extractors)),
                        entry("name", Type.PROPERTY, extractors.string()),
                        entry("labels", Type.LABEL, extractors.string())),
                datas(),
                defaultFormatRelationshipFileHeader(),
                idType,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertThat(chunk.next(visitor)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldProvideRelationshipsFromCsvInput(MultilineSetting setting) throws Exception {
        // GIVEN
        IdType idType = IdType.STRING;
        Iterable<DataFactory> data = dataIterable(data("node1,node2,KNOWS,1234567\n" + "node2,node10,HACKS,987654"));
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                data,
                header(
                        entry("from", Type.START_ID, CsvInput.idExtractor(idType, extractors)),
                        entry("to", Type.END_ID, CsvInput.idExtractor(idType, extractors)),
                        entry("type", Type.TYPE, extractors.string()),
                        entry("since", Type.PROPERTY, extractors.long_())),
                idType,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldCopeWithLinesThatHasTooFewValuesButStillValidates(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = dataIterable(data("""
                        1,ultralisk,ZERG,10
                        2,corruptor,ZERG
                        3,mutalisk,ZERG,3"""));
        Input input = new CsvInput(
                data,
                header(
                        entry(null, Type.ID, extractors.long_()),
                        entry("unit", Type.PROPERTY, extractors.string()),
                        entry("type", Type.LABEL, extractors.string()),
                        entry("kills", Type.PROPERTY, extractors.int_())),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("unit", "ultralisk", "kills", 10), labels("ZERG"));
            assertNextNode(nodes, 2L, properties("unit", "corruptor"), labels("ZERG"));
            assertNextNode(nodes, 3L, properties("unit", "mutalisk", "kills", 3), labels("ZERG"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldIgnoreValuesAfterHeaderEntries(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = dataIterable(data("1,zergling,bubble,bobble\n" + "2,scv,pun,intended"));
        Input input = new CsvInput(
                data,
                header(entry(null, Type.ID, extractors.long_()), entry("name", Type.PROPERTY, extractors.string())),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("name", "zergling"), labels());
            assertNextNode(nodes, 2L, properties("name", "scv"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldHandleMultipleInputGroups(MultilineSetting setting) throws Exception {
        // GIVEN multiple input groups, each with their own, specific, header
        DataFactory group1 = data("""
                :ID,name,kills:int,health:int
                1,Jim,10,100
                2,Abathur,0,200
                """);
        DataFactory group2 = data("""
                :ID,type
                3,zergling
                4,csv
                """);
        Iterable<DataFactory> data = dataIterable(group1, group2);
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN iterating over them, THEN the expected data should come out
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, "1", properties("name", "Jim", "kills", 10, "health", 100), labels());
            assertNextNode(nodes, "2", properties("name", "Abathur", "kills", 0, "health", 200), labels());
            assertNextNode(nodes, "3", properties("type", "zergling"), labels());
            assertNextNode(nodes, "4", properties("type", "csv"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldProvideAdditiveLabels(MultilineSetting setting) throws Exception {
        // GIVEN
        String[] addedLabels = {"Two", "AddTwo"};
        DataFactory data = data("""
                        :ID,name,:LABEL
                        0,First,
                        1,Second,One
                        2,Third,One;Two""", InputEntityDecorators.additiveLabels(addedLabels));
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 0L, properties("name", "First"), labels(addedLabels));
            assertNextNode(nodes, 1L, properties("name", "Second"), labels(union(new String[] {"One"}, addedLabels)));
            assertNextNode(nodes, 2L, properties("name", "Third"), labels(union(new String[] {"One"}, addedLabels)));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldProvideDefaultRelationshipType(MultilineSetting setting) throws Exception {
        // GIVEN
        String defaultType = "DEFAULT";
        String customType = "CUSTOM";
        DataFactory data = data(
                ":START_ID,:END_ID,:TYPE\n" + "0,1,\n" + "1,2," + customType + "\n" + "2,1," + defaultType,
                InputEntityDecorators.defaultRelationshipType(defaultType));
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                dataIterable,
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 0L, 1L, defaultType, emptyMap());
            assertNextRelationship(relationships, 1L, 2L, customType, emptyMap());
            assertNextRelationship(relationships, 2L, 1L, defaultType, emptyMap());
            assertThat(readNext(relationships)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldAllowNodesWithoutIdHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                name:string,level:int
                Mattias,1
                Johan,2
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, null, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldAllowSomeNodesToBeAnonymous(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                        :ID,name:string,level:int
                        abc,Mattias,1
                        ,Johan,2
                        """); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldAllowNodesToBeAnonymousEvenIfIdHeaderIsNamed(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                        id:ID,name:string,level:int
                        abc,Mattias,1
                        ,Johan,2
                        """); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("id", "abc", "name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldNotHaveIdSetAsPropertyIfIdHeaderEntryIsNamedForActualIds(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                        myId:ID,name:string,level:int
                        0,Mattias,1
                        1,Johan,2
                        """); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                ACTUAL,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, 0L, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, 1L, properties("name", "Johan", "level", 2), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldIgnoreEmptyPropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        // here we leave out "extra" property
        DataFactory data = data("""
                :ID,name,extra
                0,Mattias,
                1,Johan,Additional
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldIgnoreEmptyIntPropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        // here we leave out "extra" property
        DataFactory data = data("""
                :ID,name,extra:int
                0,Mattias,
                1,Johan,10
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", 10), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParsePointPropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,point:Point
                0,Mattias,"{x: 2.7, y:3.2 }"
                1,Johan," { height :0.01 ,longitude:5, latitude : -4.2 } "
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldNotParsePointPropertyValuesWithDuplicateKeys(MultilineSetting setting) {
        // GIVEN
        DataFactory data = data("""
                :ID,name,point:Point
                1,Johan," { height :0.01 ,longitude:5, latitude : -4.2, latitude : 4.2 } "
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(InputException.class);
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParsePointPropertyValuesWithCRSInHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,point:Point{crs:WGS-84-3D}
                0,Johan," { height :0.01 ,longitude:5, latitude : -4.2 } "
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldUseHeaderInformationToParsePoint(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,point:Point{crs:WGS-84}
                0,Johan," { x :1 ,y:2 } "
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseDatePropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,date:Date
                0,Mattias,2018-02-27
                1,Johan,2018-03-01
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2018, 2, 27)), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "date", DateValue.date(2018, 3, 1)), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseTimePropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                        :ID,name,time:Time
                        0,Mattias,13:37
                        1,Johan,"16:20:01"
                        2,Bob,07:30-05:00
                        """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseTimePropertyValuesWithTimezoneInHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,time:Time{timezone:+02:00}
                0,Mattias,13:37
                1,Johan,"16:20:01"
                2,Bob,07:30-05:00
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseDateTimePropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,time:DateTime
                0,Mattias,2018-02-27T13:37
                1,Johan,"2018-03-01T16:20:01"
                2,Bob,1981-05-11T07:30-05:00
                """);

        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseDateTimePropertyValuesWithTimezoneInHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,time:DateTime{timezone:Europe/Stockholm}
                0,Mattias,2018-02-27T13:37
                1,Johan,"2018-03-01T16:20:01"
                2,Bob,1981-05-11T07:30-05:00
                """);

        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseLocalTimePropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,time:LocalTime
                0,Mattias,13:37
                1,Johan,"16:20:01"
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseLocalDateTimePropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                        :ID,name,time:LocalDateTime
                        0,Mattias,2018-02-27T13:37
                        1,Johan,"2018-03-01T16:20:01"
                        """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldParseDurationPropertyValues(MultilineSetting setting) throws Exception {
        // GIVEN
        DataFactory data = data("""
                :ID,name,duration:Duration
                0,Mattias,P3MT13H37M
                1,Johan,"P-1YT4H20M"
                """);
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
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

    private static Stream<Arguments> shouldFailWhenNewlineIsUsedAsAnyDelimiter() {
        return Arrays.stream(MultilineSetting.values())
                .flatMap(setting -> Stream.of(Pair.of(setting, '\n'), Pair.of(setting, '\r')))
                .flatMap(settings -> Stream.of(
                        invalidNewlineConfig("delimiter", settings, b -> b::withDelimiter),
                        invalidNewlineConfig("array delimiter", settings, b -> b::withArrayDelimiter),
                        invalidNewlineConfig("vector delimiter", settings, b -> b::withVectorDelimiter),
                        invalidNewlineConfig("quotation character", settings, b -> b::withQuotationCharacter)));
    }

    private static Arguments invalidNewlineConfig(
            String characterDescription,
            Pair<MultilineSetting, Character> settings,
            Function<Configuration.Builder, Consumer<Character>> configSetter) {
        var builder = config(settings.first()).toBuilder();
        configSetter.apply(builder).accept(settings.other());
        return Arguments.of(builder.build(), characterDescription);
    }

    @ParameterizedTest
    @MethodSource
    void shouldFailWhenNewlineIsUsedAsAnyDelimiter(Configuration configuration, String characterDescription) {
        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                        null, null, null, null, INTEGER, configuration, false, NO_MONITOR, groups, INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("A newline character must not be used as the ", characterDescription);
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldFailOnArrayDelimiterBeingSameAsDelimiter(MultilineSetting setting) {
        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                        null,
                        null,
                        null,
                        null,
                        INTEGER,
                        config(setting).toBuilder()
                                .withDelimiter(',')
                                .withArrayDelimiter(',')
                                .build(),
                        false,
                        NO_MONITOR,
                        groups,
                        INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("array delimiter");
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldFailOnQuotationCharacterBeingSameAsDelimiter(MultilineSetting setting) {
        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                        null,
                        null,
                        null,
                        null,
                        INTEGER,
                        config(setting).toBuilder()
                                .withDelimiter(',')
                                .withArrayDelimiter(';')
                                .withQuotationCharacter(',')
                                .build(),
                        false,
                        NO_MONITOR,
                        groups,
                        INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("delimiter")
                .hasMessageContaining("quotation");
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldFailOnQuotationCharacterBeingSameAsArrayDelimiter(MultilineSetting setting) {
        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                        null,
                        null,
                        null,
                        null,
                        INTEGER,
                        config(setting).toBuilder().withQuotationCharacter(';').build(),
                        false,
                        NO_MONITOR,
                        groups,
                        INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("array delimiter")
                .hasMessageContaining("quotation");
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldFailOnVectorDelimiterBeingSameAsDelimiter(MultilineSetting setting) {
        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                        null,
                        null,
                        null,
                        null,
                        INTEGER,
                        config(setting).toBuilder()
                                .withDelimiter(',')
                                .withVectorDelimiter(',')
                                .build(),
                        false,
                        NO_MONITOR,
                        groups,
                        INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("vector delimiter")
                .hasMessageContaining("delimiter");
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldFailOnVectorDelimiterBeingSameAsQuotationCharacter(MultilineSetting setting) {
        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                        null,
                        null,
                        null,
                        null,
                        INTEGER,
                        config(setting).toBuilder()
                                .withQuotationCharacter('"')
                                .withVectorDelimiter('"')
                                .build(),
                        false,
                        NO_MONITOR,
                        groups,
                        INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("vector delimiter")
                .hasMessageContaining("quotation");
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldAllowVectorDelimiterToEqualArrayDelimiter(MultilineSetting setting) {
        assertThatCode(() -> new CsvInput(
                        datas(),
                        defaultFormatNodeFileHeader(),
                        datas(),
                        defaultFormatRelationshipFileHeader(),
                        INTEGER,
                        config(setting).toBuilder()
                                .withArrayDelimiter(';')
                                .withVectorDelimiter(';')
                                .build(),
                        false,
                        NO_MONITOR,
                        groups,
                        INSTANCE))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldHaveNodesBelongToGroupSpecifiedInHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        IdType idType = IdType.INTEGER;
        Iterable<DataFactory> data = dataIterable(data("123,one\n" + "456,two"));
        Group group = groups.getOrCreate("MyGroup");
        Input input = new CsvInput(
                data,
                header(
                        entry(null, Type.ID, group.name(), CsvInput.idExtractor(idType, extractors)),
                        entry("name", Type.PROPERTY, extractors.string())),
                datas(),
                defaultFormatRelationshipFileHeader(),
                idType,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, group, 123L, properties("name", "one"), labels());
            assertNextNode(nodes, group, 456L, properties("name", "two"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldHaveRelationshipsSpecifyStartEndNodeIdGroupsInHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        IdType idType = IdType.INTEGER;
        Iterable<DataFactory> data = dataIterable(data("123,TYPE,234\n" + "345,TYPE,456"));
        Group startNodeGroup = groups.getOrCreate("StartGroup");
        Group endNodeGroup = groups.getOrCreate("EndGroup");
        Iterable<DataFactory> nodeHeader =
                dataIterable(data(":ID(" + startNodeGroup.name() + ")"), data(":ID(" + endNodeGroup.name() + ")"));
        Input input = new CsvInput(
                nodeHeader,
                defaultFormatNodeFileHeader(),
                data,
                header(
                        entry(null, Type.START_ID, startNodeGroup.name(), CsvInput.idExtractor(idType, extractors)),
                        entry(null, Type.TYPE, extractors.string()),
                        entry(null, Type.END_ID, endNodeGroup.name(), CsvInput.idExtractor(idType, extractors))),
                idType,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, startNodeGroup, 123L, endNodeGroup, 234L, "TYPE", properties());
            assertNextRelationship(relationships, startNodeGroup, 345L, endNodeGroup, 456L, "TYPE", properties());
            assertThat(readNext(relationships)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldDoWithoutRelationshipTypeHeaderIfDefaultSupplied(MultilineSetting setting) throws Exception {
        // GIVEN relationship data w/o :TYPE header
        String defaultType = "HERE";
        DataFactory data = data("""
                        :START_ID,:END_ID,name
                        0,1,First
                        2,3,Second
                        """, InputEntityDecorators.defaultRelationshipType(defaultType));
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                dataIterable,
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            // THEN
            assertNextRelationship(relationships, 0L, 1L, defaultType, properties("name", "First"));
            assertNextRelationship(relationships, 2L, 3L, defaultType, properties("name", "Second"));
            assertThat(readNext(relationships)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldIgnoreNodeEntriesMarkedIgnoreUsingHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(data("""
                        :ID,name:IGNORE,other:int,:LABEL
                        1,Mattias,10,Person
                        2,Johan,111,Person
                        3,Emil,12,Person"""));
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatNodeFileHeader(),
                IdType.INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("other", 10), labels("Person"));
            assertNextNode(nodes, 2L, properties("other", 111), labels("Person"));
            assertNextNode(nodes, 3L, properties("other", 12), labels("Person"));
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldIgnoreRelationshipEntriesMarkedIgnoreUsingHeader(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(data("""
                        :START_ID,:TYPE,:END_ID,prop:IGNORE,other:int
                        1,KNOWS,2,Mattias,10
                        2,KNOWS,3,Johan,111
                        3,KNOWS,4,Emil,12"""));
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                data,
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 1L, 2L, "KNOWS", properties("other", 10));
            assertNextRelationship(relationships, 2L, 3L, "KNOWS", properties("other", 111));
            assertNextRelationship(relationships, 3L, 4L, "KNOWS", properties("other", 12));
            assertThat(readNext(relationships)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldPropagateExceptionFromFailingDecorator(MultilineSetting setting) {
        // GIVEN
        RuntimeException failure = new RuntimeException("FAILURE");
        Iterable<DataFactory> data = datas(data(":ID,name\n1,Mattias", new FailingNodeDecorator(failure)));
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatNodeFileHeader(),
                IdType.INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN
        assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(InputException.class)
                .cause()
                .isEqualTo(failure);
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldNotIncludeEmptyArraysInEntities(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(data("""
                        :ID,sprop:String[],lprop:long[]
                        1,,
                        2,a;b,10;20"""));
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatNodeFileHeader(),
                IdType.INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes, 2L, properties("sprop", new String[] {"a", "b"}, "lprop", new long[] {10, 20}), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldTreatEmptyQuotedStringsAsNullIfConfiguredTo(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(data(":ID,one,two,three\n" + "1,\"\",,value"));
        Configuration config =
                config(setting).toBuilder().withEmptyQuotedStringsAsNull(true).build();

        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config,
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("three", "value"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldIgnoreEmptyExtraColumns(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(data("""
                :ID,one
                1,test,
                2,test,,additional"""));

        // WHEN
        Collector collector = mock(Collector.class);
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // THEN
        try (InputIterator nodes = input.nodes(collector).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("one", "test"), labels());
            assertNextNode(nodes, 2L, properties("one", "test"), labels());
            assertThat(readNext(nodes)).isFalse();
        }
        verify(collector).collectExtraColumns(anyString(), eq(1L), eq(null));
        verify(collector).collectExtraColumns(anyString(), eq(2L), eq(null));
        verify(collector).collectExtraColumns(anyString(), eq(2L), eq("additional"));
    }

    @ParameterizedTest
    @EnumSource(MultilineSetting.class)
    void shouldSkipRelationshipValidationIfToldTo(MultilineSetting setting) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(data(":START_ID,:END_ID,:TYPE\n" + ",,"));
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                data,
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config(setting),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            readNext(relationships);
            assertThat(visitor.startId()).isNull();
            assertThat(visitor.endId()).isNull();
            assertThat(visitor.stringType).isNull();
        }
    }

    @Test
    void shouldFailOnUnparsableNodeHeader() {
        // given
        Iterable<DataFactory> data = datas(data(":SOMETHING,abcde#rtg:123,"));

        assertThatThrownBy(() -> new CsvInput(
                                data,
                                defaultFormatNodeFileHeader(),
                                datas(),
                                defaultFormatRelationshipFileHeader(),
                                IdType.INTEGER,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(HeaderException.class);
    }

    @Test
    void shouldFailOnUnparsableRelationshipHeader() {
        // given
        Iterable<DataFactory> data = datas(data(":SOMETHING,abcde#rtg:123,"));

        assertThatThrownBy(() -> new CsvInput(
                                datas(),
                                defaultFormatNodeFileHeader(),
                                data,
                                defaultFormatRelationshipFileHeader(),
                                IdType.INTEGER,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(HeaderException.class);
    }

    @Test
    void shouldFailOnUndefinedGroupInRelationshipHeader() {
        // given
        Iterable<DataFactory> nodeData = datas(data(":ID(left)"), data(":ID(right)"));
        Iterable<DataFactory> relationshipData = datas(data(":START_ID(left),:END_ID(rite)"));

        assertThatThrownBy(() -> new CsvInput(
                                nodeData,
                                defaultFormatNodeFileHeader(),
                                relationshipData,
                                defaultFormatRelationshipFileHeader(),
                                IdType.INTEGER,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(HeaderException.class);
    }

    @Test
    void shouldFailOnGlobalGroupInRelationshipHeaderIfNoGLobalGroupInNodeHeader() {
        // given
        Iterable<DataFactory> nodeData = datas(data(":ID(left)"), data(":ID(right)"));
        Iterable<DataFactory> relationshipData = datas(data(":START_ID,:END_ID(rite)"));

        assertThatThrownBy(() -> new CsvInput(
                                nodeData,
                                defaultFormatNodeFileHeader(),
                                relationshipData,
                                defaultFormatRelationshipFileHeader(),
                                IdType.INTEGER,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(HeaderException.class);
    }

    @Test
    void shouldReportDuplicateNodeSourceFiles() throws IOException {
        // given
        Path file = writeFile("node-source", ":ID");
        // The same file is used twice
        Iterable<DataFactory> data = datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file, file));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        data,
                        defaultFormatNodeFileHeader(),
                        datas(),
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        true,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor).duplicateSourceFile(sourceDescription(file));
    }

    @Test
    void shouldReportDuplicateRelationshipSourceFiles() throws IOException {
        // given
        Path file = writeFile("relationship-source", ":START_ID,:END_ID,:TYPE");
        // The same file is used twice
        Iterable<DataFactory> data = datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file, file));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        datas(),
                        defaultFormatNodeFileHeader(),
                        data,
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        true,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor).duplicateSourceFile(sourceDescription(file));
    }

    @Test
    void shouldReportDuplicateSourceFileUsedAsBothNodeAndRelationshipSourceFile() throws IOException {
        // given
        Path nodeHeader = writeFile("node-source", ":ID");
        Path relationshipHeader = writeFile("relationship-source", ":START_ID,:END_ID,:TYPE");
        Path sharedDataFile = writeFile("shared-source", "1,2,3");
        Iterable<DataFactory> nodeData =
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), nodeHeader, sharedDataFile));
        Iterable<DataFactory> relationshipData =
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), relationshipHeader, sharedDataFile));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        nodeData,
                        defaultFormatNodeFileHeader(),
                        relationshipData,
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        false,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor).duplicateSourceFile(sourceDescription(sharedDataFile));
    }

    @Test
    void shouldNormalizeTypes() throws IOException {
        // given
        Path source1 = writeFile("source1", ":ID,shortProp:short,intProp:int");
        Path source2 = writeFile("source2", ":ID,floatProp:float,doubleProp:double");
        Path source3 = writeFile("source3", ":START_ID,:END_ID,byteProp:byte,longProp:long");
        // A non-NO_DECORATOR decorator so that validation doesn't also report missing labels/type
        Iterable<DataFactory> nodeData = datas(
                DataFactories.data(value -> value, defaultCharset(), source1),
                DataFactories.data(value -> value, defaultCharset(), source2));
        Iterable<DataFactory> relationshipData = datas(DataFactories.data(value -> value, defaultCharset(), source3));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        CsvInput input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(true),
                relationshipData,
                defaultFormatRelationshipFileHeader(true),
                IdType.INTEGER,
                COMMAS,
                false,
                monitor,
                groups,
                INSTANCE);
        input.validateAndEstimate((values, NULL, INSTANCE) -> 1 /*doesn't quite matter*/, NUMBER_OF_ESTIMATE_THREADS);

        // then
        String description1 = sourceDescription(source1);
        String description2 = sourceDescription(source2);
        String description3 = sourceDescription(source3);
        verify(monitor, times(1)).typeNormalized(description1, "shortProp", "short", "long");
        verify(monitor, times(1)).typeNormalized(description1, "intProp", "int", "long");
        verify(monitor, times(1)).typeNormalized(description2, "floatProp", "float", "double");
        verify(monitor, times(1)).typeNormalized(description3, "byteProp", "byte", "long");
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldCalculateCorrectEstimatesForZippedInputFile() throws IOException {
        // GIVEN
        IdType idType = STRING;
        Path uncompressedFile = createNodeInputDataFile(mebiBytes(10));
        Path compressedFile = compressWithZip(uncompressedFile);
        assertThat(Files.size(compressedFile)).isLessThan(Files.size(uncompressedFile));

        // WHEN
        Input.Estimates uncompressedEstimates =
                calculateEstimates(idType, NUMBER_OF_ESTIMATE_THREADS, List.of(uncompressedFile), emptyList());
        Input.Estimates compressedEstimates =
                calculateEstimates(idType, NUMBER_OF_ESTIMATE_THREADS, List.of(compressedFile), emptyList());

        // then
        assertEstimatesEquals(uncompressedEstimates, compressedEstimates, 0);
    }

    @Test
    void shouldCalculateCorrectEstimatesForGZippedInputFile() throws IOException {
        // GIVEN
        IdType idType = STRING;
        Path uncompressedFile = createNodeInputDataFile(mebiBytes(10));
        Path compressedFile = compressWithGZip(uncompressedFile);
        assertThat(Files.size(compressedFile)).isLessThan(Files.size(uncompressedFile));

        // WHEN
        Input.Estimates uncompressedEstimates =
                calculateEstimates(idType, NUMBER_OF_ESTIMATE_THREADS, List.of(uncompressedFile), emptyList());
        Input.Estimates compressedEstimates =
                calculateEstimates(idType, NUMBER_OF_ESTIMATE_THREADS, List.of(compressedFile), emptyList());

        // then the compressed and uncompressed should be _roughly_ equal. The thing with GZIP is that there's no
        // reliable way
        // of getting the uncompressed data size w/o decompressing it in its entirety, and this is why the estimator
        // doesn't do this
        // but instead tries to estimate its compression rate after reading a chunk of it
        assertEstimatesEquals(uncompressedEstimates, compressedEstimates, 0.01);
    }

    @Test
    void shouldCalculateSameEstimatesUsingMultipleThreads() throws IOException {
        // given
        List<Path> nodeDataFiles = new ArrayList<>();
        List<Path> relationshipDataFiles = new ArrayList<>();
        nodeDataFiles.add(writeFile("node-header.csv", ":ID,:LABEL,name"));
        relationshipDataFiles.add(writeFile("relationship-header.csv", ":START_ID,:TYPE,:END_ID,name"));
        for (int f = 0, id = 0; f < 10; f++) {
            List<String> nodeLines = new ArrayList<>();
            for (int i = 0; i < 100; i++, id++) {
                nodeLines.add(String.format("%d,Person,name%d", id, id));
            }
            nodeDataFiles.add(writeFile("node-data-" + f + ".csv", nodeLines.toArray(String[]::new)));

            List<String> relationshipLines = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                relationshipLines.add(String.format("%d,KNOWS,%d,name%d", id, id + 1, id + 2));
            }
            relationshipDataFiles.add(
                    writeFile("relationship-data-" + f + ".csv", relationshipLines.toArray(String[]::new)));
        }

        // when
        var singleThreadedEstimates = calculateEstimates(INTEGER, 1, nodeDataFiles, relationshipDataFiles);
        assertThat(singleThreadedEstimates)
                .isEqualTo(new Input.Estimates(1000, 2000, 1000, 2000, 16890, 34200, 1000, false, false));
        for (int i = 2; i < 6; i++) {
            var parallelEstimates = calculateEstimates(INTEGER, i, nodeDataFiles, relationshipDataFiles);
            // then
            assertThat(parallelEstimates).isEqualTo(singleThreadedEstimates);
        }
    }

    @Test
    void shouldReportNoNodeLabels() throws IOException {
        // given
        Path file = writeFile("node-source", ":ID");
        Iterable<DataFactory> data = datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        data,
                        defaultFormatNodeFileHeader(),
                        datas(),
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        false,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor).noNodeLabelsSpecified(sourceDescription(file));
    }

    @Test
    void shouldNotReportNoNodeLabelsIfDecorated() throws IOException {
        // given
        Path file = writeFile("node-source", ":ID");
        Iterable<DataFactory> data = datas(DataFactories.data(additiveLabels("MyLabel"), defaultCharset(), file));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        data,
                        defaultFormatNodeFileHeader(),
                        datas(),
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        false,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor, never()).noNodeLabelsSpecified(sourceDescription(file));
    }

    @Test
    void shouldReportNoRelationshipType() throws IOException {
        // given
        Path file = writeFile("relationship-source", ":START_ID,:END_ID");
        Iterable<DataFactory> data = datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        datas(),
                        defaultFormatNodeFileHeader(),
                        data,
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        false,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor).noRelationshipTypeSpecified(sourceDescription(file));
    }

    @Test
    void shouldNotReportNoRelationshipTypeIfDecorated() throws IOException {
        // given
        Path file = writeFile("relationship-source", ":START_ID,:END_ID");
        Iterable<DataFactory> data =
                datas(DataFactories.data(defaultRelationshipType("MyType"), defaultCharset(), file));
        CsvInput.Monitor monitor = mock(CsvInput.Monitor.class);

        // when
        new CsvInput(
                        datas(),
                        defaultFormatNodeFileHeader(),
                        data,
                        defaultFormatRelationshipFileHeader(),
                        IdType.INTEGER,
                        COMMAS,
                        false,
                        monitor,
                        groups,
                        INSTANCE)
                .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

        // then
        verify(monitor, never()).noRelationshipTypeSpecified(sourceDescription(file));
    }

    @Test
    void shouldReportDuplicateNodeHeader() throws IOException {
        // GIVEN
        Path file = writeFile("node-header", ":ID,name:string,name");

        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                                defaultFormatNodeFileHeader(),
                                datas(),
                                defaultFormatRelationshipFileHeader(),
                                STRING,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(DuplicateHeaderException.class)
                .hasMessageContaining(file.getFileName().toString());
    }

    @Test
    void shouldReportDuplicateRelationshipHeader() throws IOException {
        // GIVEN
        Path file = writeFile("relationship-header", ":START_ID,:TYPE,:END_ID,:TYPE,name:string");

        // WHEN
        assertThatThrownBy(() -> new CsvInput(
                                datas(),
                                defaultFormatNodeFileHeader(),
                                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                                defaultFormatRelationshipFileHeader(),
                                STRING,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(DuplicateHeaderException.class)
                .hasMessageContaining(file.getFileName().toString());
    }

    @Test
    void shouldThrowOnReferencedNodeSchemaWithoutExplicitLabelOptionData() throws FileNotFoundException {
        // given
        Path file = writeFile("relationship-header", "myId:ID(Person)\tname:string\t:LABEL");

        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                TABS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));

            // then
            assertThatThrownBy(() -> input.referencedNodeSchema(tokenHolders))
                    .hasMessageContaining("No label was specified");
        }
    }

    @Test
    void shouldHandleMultipleEqualReferencedSchemaForSameGroup() throws FileNotFoundException {
        // given
        var file1 = writeFile("nodes1", "myId:ID(MyGroup){label:Person}");
        var file2 = writeFile("nodes2", "myId:ID(MyGroup){label:Person}");

        try (var input = new CsvInput(
                datas(
                        DataFactories.data(NO_DECORATOR, defaultCharset(), file1),
                        DataFactories.data(NO_DECORATOR, defaultCharset(), file2)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                TABS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
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
    void shouldFailMultipleNonEqualReferencedSchemaForSameGroup() throws FileNotFoundException {
        // given
        var file1 = writeFile("nodes1", "myId:ID(MyGroup){label:Person}");
        var file2 = writeFile("nodes2", "myId:ID(MyGroup){label:Company}");

        try (var input = new CsvInput(
                datas(
                        DataFactories.data(NO_DECORATOR, defaultCharset(), file1),
                        DataFactories.data(NO_DECORATOR, defaultCharset(), file2)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                TABS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
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
    void shouldParseReferencedNodeSchemaWithExplicitLabelOptionData() throws FileNotFoundException {
        // given
        Path file = writeFile("relationship-header", "myId:ID(My Group){label:Person}\tname:string\t:LABEL");

        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                TABS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));
            var schema = input.referencedNodeSchema(tokenHolders);

            // then
            assertThat(schema).containsExactlyInAnyOrderEntriesOf(Map.of("My Group", SchemaDescriptors.forLabel(2, 4)));
        }
    }

    @Test
    void shouldStoreIdAsPropertyInSpecificValueType() throws IOException {
        // given nodes w/ IDs as ints
        var nodeData = datas(data("id:ID{id-type:int},prop\n123,val"));

        // The variable groups has a global id space created already without id-type:int.
        // Passing that in would fail.
        var testSpecificGroups = new Groups();
        var group = testSpecificGroups.getOrCreate(null);
        testSpecificGroups.bindIdType(group, "id", "int");

        // when using string id-type in the input
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                config(MultilineSetting.DISALLOW),
                false,
                NO_MONITOR,
                testSpecificGroups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(EMPTY).iterator()) {
                // then
                assertNextNode(nodes, group, 123, properties("id", 123, "prop", "val"), labels());
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    @Test
    void shouldHandleMultipleNodeIdColumns() throws IOException {
        // given
        var file = writeFile("nodes", "id1:ID,id2:ID,name,:LABEL", "ABC,123,First,Person", "ABC,456,Second,Person");

        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                assertNextNode(
                        nodes,
                        "ABC%c123".formatted(IdValueBuilder.DELIMITER),
                        properties("id1", "ABC", "id2", "123", "name", "First"),
                        Set.of("Person"));
                assertNextNode(
                        nodes,
                        "ABC%c456".formatted(IdValueBuilder.DELIMITER),
                        properties("id1", "ABC", "id2", "456", "name", "Second"),
                        Set.of("Person"));
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("idMapperTypeCases")
    void idMapperType(String name, List<String[]> files, IdType globalIdType, IdType expectedIdType)
            throws IOException {
        var dataFactories = new ArrayList<DataFactory>();
        for (int i = 0; i < files.size(); i++) {
            var path = writeFile("nodes-" + i, files.get(i));
            dataFactories.add(DataFactories.data(NO_DECORATOR, defaultCharset(), path));
        }
        try (var input = new CsvInput(
                datas(dataFactories.toArray(DataFactory[]::new)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                globalIdType,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            assertThat(input.idType()).isEqualTo(expectedIdType);
        }
    }

    private static Stream<Arguments> idMapperTypeCases() {
        return Stream.of(
                Arguments.of(
                        "integer column with string id-type override falls back to string",
                        List.<String[]>of(new String[] {"id:ID(g1){id-type:string},prop", "two,val"}),
                        INTEGER,
                        STRING),
                Arguments.of(
                        "string column with int id-type override yields integer",
                        List.<String[]>of(new String[] {"id:ID(g1){id-type:int},prop", "123,val"}),
                        STRING,
                        INTEGER),
                Arguments.of(
                        "single int id column with global integer id-type",
                        List.<String[]>of(new String[] {"id:ID(g1),prop", "123,val"}),
                        INTEGER,
                        INTEGER),
                Arguments.of(
                        "single string id column with global string id-type",
                        List.<String[]>of(new String[] {"id:ID(g1),prop", "abc,val"}),
                        STRING,
                        STRING),
                Arguments.of(
                        "composite id columns yield string",
                        List.<String[]>of(
                                new String[] {"id1:ID(g1){id-type:int},id2:ID(g1){id-type:int},prop", "1,2,val"}),
                        INTEGER,
                        STRING),
                Arguments.of(
                        "ACTUAL global id-type stays ACTUAL",
                        List.<String[]>of(new String[] {"id:ID(g1),prop", "1,val"}),
                        ACTUAL,
                        ACTUAL),
                Arguments.of(
                        "any node file with non-long id column yields string",
                        List.of(
                                new String[] {"id:ID(g1){id-type:int},prop", "123,val"},
                                new String[] {"id:ID(g2){id-type:string},prop", "abc,val"}),
                        INTEGER,
                        STRING),
                Arguments.of(
                        "all node files with single long id column yield integer",
                        List.of(
                                new String[] {"id:ID(g1),prop", "1,val"},
                                new String[] {"id:ID(g2){id-type:long},prop", "2,val"}),
                        INTEGER,
                        INTEGER));
    }

    @ParameterizedTest
    @EnumSource(
            value = IdType.class,
            names = {"INTEGER", "STRING"})
    void multipleIdColumns(IdType idType) throws IOException {
        var columnIdTypes = Lists.immutable.of("", "int", "long", "string", "datetime");
        var headers = new StringBuilder();
        for (int i = 0, length = random.nextInt(2, 5); i < length; i++) {
            if (i > 0) {
                headers.append(",");
            }
            headers.append(":ID");

            var columnType = random.among(columnIdTypes);
            if (!columnType.isEmpty()) {
                headers.append("{id-type=").append(columnType).append("}");
            }
        }
        try (var csvInput = new CsvInput(
                datas(data(headers.toString())),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                idType,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThat(csvInput.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isNotNull();
        }
    }

    @Test
    void shouldHandleMultipleNodeIdColumnsWithSameGroup() throws IOException {
        Iterable<DataFactory> nodeData = datas(data("""
                 id1:ID(g1),id2:ID(g1),name
                 foo,bar,ABC
                 """));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                assertNextNode(
                        nodes,
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        properties("id1", "foo", "id2", "bar", "name", "ABC"),
                        Set.of());
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    @Test
    void multipleNodeIdColumnsRequireSameGroup() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g2),name"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("There are multiple :ID columns, but they are referring to different groups");
        }
    }

    @Test
    void shouldHandleMultipleStartAndEndIdColumns() throws IOException {
        // given
        Iterable<DataFactory> nodeData = datas(data("""
                id1:ID,id2:ID,name
                foo,bar,ABC
                """));
        Iterable<DataFactory> relData = datas(data("""
                :START_ID,:START_ID,:END_ID,:END_ID,:TYPE
                a,b,c,d,KNOWS
                b,c,d,e,KNOWS
                """));

        // when
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            // then
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        "a%cb".formatted(IdValueBuilder.DELIMITER),
                        "c%cd".formatted(IdValueBuilder.DELIMITER),
                        "KNOWS",
                        emptyMap());
                assertNextRelationship(
                        relationships,
                        "b%cc".formatted(IdValueBuilder.DELIMITER),
                        "d%ce".formatted(IdValueBuilder.DELIMITER),
                        "KNOWS",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void shouldHandleMultipleStartAndEndIdColumnsWithSameGroup() throws IOException {
        Iterable<DataFactory> nodeData = datas(data("""
                 id1:ID(g1),id2:ID(g1),name
                 foo,bar,ABC
                 """), data("""
                 id1:ID(g2),id2:ID(g2),id3:ID(g2),name
                 foo,bar,baz,DEF
                 """));
        Iterable<DataFactory> relData = datas(data("""
                 :START_ID(g1),:START_ID(g1),:END_ID(g2),:END_ID(g2),:END_ID(g2),:TYPE
                 foo,bar,foo,bar,baz,TTT
                 """));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        groups.getOrCreate("g2"),
                        "foo%cbar%cbaz".formatted(IdValueBuilder.DELIMITER, IdValueBuilder.DELIMITER),
                        "TTT",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void shouldHandleMultipleStartAndEndIdColumnsWithSameGroupMultipleNodeFiles() throws IOException {
        Iterable<DataFactory> nodeData = datas(data("""
                 id1:ID(g1),id2:ID(g1),name
                 foo,bar,ABC
                 """), data("""
                 id1:ID(g1),id2:ID(g1),name
                 foo,baz,DEF
                 """));
        Iterable<DataFactory> relData = datas(data("""
                 :START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE
                 foo,bar,foo,baz,TTT
                 """));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        groups.getOrCreate("g1"),
                        "foo%cbaz".formatted(IdValueBuilder.DELIMITER),
                        "TTT",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void shouldHandleMultipleStartAndEndIdColumnsWithGlobalGroup() throws IOException {
        Iterable<DataFactory> nodeData = datas(data("""
                 id1:ID(g1),id2:ID(g1),name
                 foo,bar,ABC
                 """), data("""
                 id1:ID,id2:ID,id3:ID,name
                 foo,bar,baz,DEF
                 """));
        Iterable<DataFactory> relData = datas(data("""
                 :START_ID(g1),:START_ID(g1),:END_ID,:END_ID,:END_ID,:TYPE
                 foo,bar,foo,bar,baz,TTT
                 """));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        groups.getOrCreate(null),
                        "foo%cbar%cbaz".formatted(IdValueBuilder.DELIMITER, IdValueBuilder.DELIMITER),
                        "TTT",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void multipleStartIdColumnsRequireSameGroup() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"), data("id1:ID(g2),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:START_ID(g2),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are multiple :START_ID columns, but they are referring to different groups");
        }
    }

    @Test
    void multipleEndIdColumnsRequireSameGroup() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"), data("id1:ID(g2),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g2),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are multiple :END_ID columns, but they are referring to different groups");
        }
    }

    @Test
    void singleNodeIdColumnRequiresSingleStartIdColumn() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are 2 :START_ID columns for group 'g1', but 1 :START_ID columns is expected.");
        }
    }

    @Test
    void singleNodeIdColumnRequiresSingleEndIdColumn() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are 2 :END_ID columns for group 'g1', but 1 :END_ID columns is expected.");
        }
    }

    @Test
    void shouldBeAbleToUseSingleIdInOneNodeFileAndCompositeIdInAnother() throws IOException {
        Iterable<DataFactory> nodeData = datas(data("""
                 id1:ID(g1),id2:ID(g1),name
                 foo,bar,ABC
                 """), data("""
                 id1:ID(g1),name
                 foo,DEF
                 """));
        Iterable<DataFactory> relData = datas(data("""
                 :START_ID(g1),:END_ID(g1),:TYPE
                 foobar,foo,TTT
                 """));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                assertNextNode(
                        nodes,
                        groups.getOrCreate("g1"),
                        "foobar",
                        properties("id1", "foo", "id2", "bar", "name", "ABC"),
                        Set.of());
                assertNextNode(
                        nodes, groups.getOrCreate("g1"), "foo", properties("id1", "foo", "name", "DEF"), Set.of());
                assertThat(readNext(nodes)).isFalse();
            }
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g1"),
                        "foobar",
                        groups.getOrCreate("g1"),
                        "foo",
                        "TTT",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void shouldNotBeAbleToReferToMixedSingleIdAndCompositeIdWithMultipleStartIdColumns() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),name"), data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are 2 :START_ID columns for group 'g1', but 1 :START_ID columns is expected.");
        }
    }

    @Test
    void shouldNotBeAbleToReferToMixedSingleIdAndCompositeIdWithMultipleEndIdColumns() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),name"), data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are 2 :END_ID columns for group 'g1', but 1 :END_ID columns is expected.");
        }
    }

    @Test
    void shouldNotBeAbleToReferToCompositeIdWithWrongNumberOfStartIdColumns() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData =
                datas(data(":START_ID(g1),:START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are 3 :START_ID columns for group 'g1', but 2 :START_ID columns is expected.");
        }
    }

    @Test
    void shouldNotBeAbleToReferToCompositeIdWithWrongNumberOfEndIdColumns() {
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData =
                datas(data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "There are 3 :END_ID columns for group 'g1', but 2 :END_ID columns is expected.");
        }
    }

    @Test
    void shouldBeAbleToReferToCompositeIdAndSingleIdsWithMatchingNumberOfStartIdAndEndColumns() throws IOException {
        Iterable<DataFactory> nodeData = datas(data("""
                        id1:ID(g1),id2:ID(g1),name
                        foo,bar,ABC
                        """), data("""
                        id1:ID(g2),name
                        baz,DEF
                        """));
        Iterable<DataFactory> relData = datas(data("""
                        :START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE
                        foo,bar,foo,bar,T1
                        """), data("""
                        :START_ID(g2),:END_ID(g1),:END_ID(g1),:TYPE
                        baz,foo,bar,T2
                        """), data("""
                        :START_ID(g2),:END_ID(g2),:TYPE
                        baz,baz,T3
                        """));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                assertNextNode(
                        nodes,
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        properties("id1", "foo", "id2", "bar", "name", "ABC"),
                        Set.of());
                assertNextNode(
                        nodes, groups.getOrCreate("g2"), "baz", properties("id1", "baz", "name", "DEF"), Set.of());
                assertThat(readNext(nodes)).isFalse();
            }
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        "T1",
                        emptyMap());
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g2"),
                        "baz",
                        groups.getOrCreate("g1"),
                        "foo%cbar".formatted(IdValueBuilder.DELIMITER),
                        "T2",
                        emptyMap());
                assertNextRelationship(
                        relationships,
                        groups.getOrCreate("g2"),
                        "baz",
                        groups.getOrCreate("g2"),
                        "baz",
                        "T3",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void shouldNotBeAbleToMixHowToReferToCompositeIDs1() {
        // First using multiple :START_ID, then single :START_ID
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData = datas(
                data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"),
                data(":START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "How to refer to composite IDs (multiple :ID columns) from :START_ID/:END_ID must be consistent: "
                                    + "Either using a single :START_ID/:END_ID column, or a matching amount of :START_ID/:END_ID columns.");
        }
    }

    @Test
    void shouldNotBeAbleToMixHowToReferToCompositeIDs2() {
        // First using single :START_ID, then multiple :START_ID
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData = datas(
                data(":START_ID(g1),:END_ID(g1),:TYPE"),
                data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "How to refer to composite IDs (multiple :ID columns) from :START_ID/:END_ID must be consistent: "
                                    + "Either using a single :START_ID/:END_ID column, or a matching amount of :START_ID/:END_ID columns.");
        }
    }

    @Test
    void shouldNotBeAbleToMixHowToReferToCompositeIDs3() {
        // First using single :START_ID, then multiple :END_ID
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "How to refer to composite IDs (multiple :ID columns) from :START_ID/:END_ID must be consistent: "
                                    + "Either using a single :START_ID/:END_ID column, or a matching amount of :START_ID/:END_ID columns.");
        }
    }

    @Test
    void shouldNotBeAbleToMixHowToReferToCompositeIDs4() {
        // First using multiple :START_ID, then single :END_ID
        Iterable<DataFactory> nodeData = datas(data("id1:ID(g1),id2:ID(g1),name"));
        Iterable<DataFactory> relData = datas(data(":START_ID(g1),:START_ID(g1),:END_ID(g1),:TYPE"));
        try (var input = new CsvInput(
                nodeData,
                defaultFormatNodeFileHeader(),
                relData,
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            assertThatThrownBy(() -> input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "How to refer to composite IDs (multiple :ID columns) from :START_ID/:END_ID must be consistent: "
                                    + "Either using a single :START_ID/:END_ID column, or a matching amount of :START_ID/:END_ID columns.");
        }
    }

    @Test
    void shouldFailOnStoringMultipleCompositeIdColumnsInSameProperty() throws IOException {
        // given
        var file = writeFile("nodes", "id:ID,id:ID,name,:LABEL", "ABC,123,First,Person", "ABC,456,Second,Person");

        // when/then
        assertThatThrownBy(() -> new CsvInput(
                                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                                defaultFormatNodeFileHeader(),
                                datas(),
                                defaultFormatRelationshipFileHeader(),
                                STRING,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(InputException.class)
                .hasMessageContaining("Cannot store composite IDs");
    }

    @Test
    void shouldHandleCompositeIDsOfMixedTypes() throws IOException {
        // given
        var nodeData = writeFile(
                "nodes",
                "id1:ID(g1){id-type: string},id2:ID(g1){id-type: int},name,:LABEL",
                "ABC,123,First,Person",
                "ABC,456,Second,Person");
        var relData = writeFile(
                "relationships", ":START_ID(g1),:START_ID(g1),:END_ID(g1),:END_ID(g1),:TYPE", "ABC,123,ABC,456,T1");

        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), nodeData)),
                defaultFormatNodeFileHeader(),
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), relData)),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);

            var group = groups.getOrCreate("g1");
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                var labels = Set.of("Person");
                assertNextNode(
                        nodes,
                        group,
                        "ABC%c123".formatted(IdValueBuilder.DELIMITER),
                        properties("id1", "ABC", "id2", 123, "name", "First"),
                        labels);
                assertNextNode(
                        nodes,
                        group,
                        "ABC%c456".formatted(IdValueBuilder.DELIMITER),
                        properties("id1", "ABC", "id2", 456, "name", "Second"),
                        labels);
                assertThat(readNext(nodes)).isFalse();
            }
            try (var relationships = input.relationships(Collector.STRICT).iterator()) {
                assertNextRelationship(
                        relationships,
                        group,
                        "ABC%c123".formatted(IdValueBuilder.DELIMITER),
                        group,
                        "ABC%c456".formatted(IdValueBuilder.DELIMITER),
                        "T1",
                        emptyMap());
                assertThat(readNext(relationships)).isFalse();
            }
        }
    }

    @Test
    void shouldFailOnCompositeIdColumnsForDifferentGroups() throws IOException {
        // given
        var file = writeFile(
                "nodes", ":ID(group1),:ID(group2),name,:LABEL", "ABC,123,First,Person", "ABC,456,Second,Person");

        // when/then
        assertThatThrownBy(() -> new CsvInput(
                                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                                defaultFormatNodeFileHeader(),
                                datas(),
                                defaultFormatRelationshipFileHeader(),
                                STRING,
                                COMMAS,
                                false,
                                NO_MONITOR,
                                groups,
                                INSTANCE)
                        .validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("referring to different groups");
    }

    @Test
    void shouldParseAction() throws IOException {
        // given
        var file = writeFile(
                "nodes",
                ":ID,p1,:LABEL,:ACTION",
                "A,abc,Test,",
                "B,def,Test,CREATE",
                "C,ghi,Test,UPDATE",
                "D,jkl,Test,DELETE",
                "E,aaa,Test,C",
                "F,bbb,Test,U",
                "G,ccc,Test,D");

        // when
        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                // then
                assertNextNode(nodes, globalGroup, "A", properties("p1", "abc"), Set.of("Test"), null);
                assertNextNode(nodes, globalGroup, "B", properties("p1", "def"), Set.of("Test"), CREATE);
                assertNextNode(nodes, globalGroup, "C", properties("p1", "ghi"), Set.of("Test"), UPDATE);
                assertNextNode(nodes, globalGroup, "D", properties("p1", "jkl"), Set.of("Test"), DELETE);
                assertNextNode(nodes, globalGroup, "E", properties("p1", "aaa"), Set.of("Test"), CREATE);
                assertNextNode(nodes, globalGroup, "F", properties("p1", "bbb"), Set.of("Test"), UPDATE);
                assertNextNode(nodes, globalGroup, "G", properties("p1", "ccc"), Set.of("Test"), DELETE);
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    @Test
    void shouldParseRemoveLabel() throws IOException {
        // given
        var file = writeFile("nodes", ":ID,:ACTION,:-LABEL", "a,U,Label1", "b,U,Label2;Label3");

        // when
        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                // then
                assertThat(readNext(nodes)).isTrue();
                assertThat(visitor.removedLabels).containsExactlyElementsOf(List.of("Label1"));
                assertThat(readNext(nodes)).isTrue();
                assertThat(visitor.removedLabels).containsExactlyElementsOf(List.of("Label2", "Label3"));
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    @Test
    void shouldParseRemoveProperty() throws IOException {
        // given
        var file = writeFile("nodes", ":ID,:ACTION,unwantedProp:-PROPERTY,wantedProp", "a,U,x,val");

        // when
        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                // then
                assertThat(readNext(nodes)).isTrue();
                assertThat(visitor.propertiesAsMap()).containsExactlyInAnyOrderEntriesOf(Map.of("wantedProp", "val"));
                assertThat(visitor.removedProperties).containsExactlyElementsOf(List.of("unwantedProp"));
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    @Test
    void shouldParseRemoveProperties() throws IOException {
        // given
        var file = writeFile("nodes", ":ID,:ACTION,:-PROPERTY,wantedProp", "a,U,unwanted1;unwanted2,val");

        // when
        try (var input = new CsvInput(
                datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                STRING,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE)) {
            input.validateAndEstimate(PROPERTY_SIZE_CALCULATOR, NUMBER_OF_ESTIMATE_THREADS);
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                // then
                assertThat(readNext(nodes)).isTrue();
                assertThat(visitor.propertiesAsMap()).containsExactlyInAnyOrderEntriesOf(Map.of("wantedProp", "val"));
                assertThat(visitor.removedProperties).containsExactlyElementsOf(List.of("unwanted1", "unwanted2"));
                assertThat(readNext(nodes)).isFalse();
            }
        }
    }

    private Path writeFile(String name, String... lines) throws FileNotFoundException {
        Path file = directory.file(name);
        try (PrintWriter writer = new PrintWriter(file.toFile())) {
            for (String line : lines) {
                writer.println(line);
            }
        }
        return file;
    }

    private TokenHolder tokenHolder(Map<String, Integer> tokens) {
        var tokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, "type");
        tokenHolder.setInitialTokens(tokens.entrySet().stream()
                .map(e -> new NamedToken(e.getKey(), e.getValue()))
                .toList());
        return tokenHolder;
    }

    private static void assertEstimatesEquals(Input.Estimates a, Input.Estimates b, double errorMargin) {
        var margin = withPercentage(errorMargin * 100);
        assertThat(b.numberOfNodes()).isCloseTo(a.numberOfNodes(), margin);
        assertThat(b.numberOfNodeLabels()).isCloseTo(a.numberOfNodeLabels(), margin);
        assertThat(b.numberOfNodeProperties()).isCloseTo(a.numberOfNodeProperties(), margin);
        assertThat(b.numberOfRelationships()).isCloseTo(a.numberOfRelationships(), margin);
        assertThat(b.numberOfRelationshipProperties()).isCloseTo(a.numberOfRelationshipProperties(), margin);
        assertThat(b.sizeOfNodeProperties()).isCloseTo(a.sizeOfNodeProperties(), margin);
        assertThat(b.sizeOfRelationshipProperties()).isCloseTo(a.sizeOfRelationshipProperties(), margin);
    }

    private Input.Estimates calculateEstimates(
            IdType idType, int numberOfThreads, List<Path> nodeDataFiles, List<Path> relationshipDataFiles)
            throws IOException {
        Input input = new CsvInput(
                !nodeDataFiles.isEmpty()
                        ? dataIterable(
                                DataFactories.data(NO_DECORATOR, defaultCharset(), nodeDataFiles.toArray(Path[]::new)))
                        : emptyList(),
                defaultFormatNodeFileHeader(),
                !relationshipDataFiles.isEmpty()
                        ? dataIterable(DataFactories.data(
                                NO_DECORATOR, defaultCharset(), relationshipDataFiles.toArray(Path[]::new)))
                        : emptyList(),
                defaultFormatRelationshipFileHeader(),
                idType,
                COMMAS,
                false,
                NO_MONITOR,
                groups,
                INSTANCE);
        // We don't care about correct value size calculation really, as long as it's consistent
        return input.validateAndEstimate(
                (values, tracer, memTracker) ->
                        Stream.of(values).mapToInt(v -> v.toString().length()).sum(),
                numberOfThreads);
    }

    private Path compressWithZip(Path uncompressedFile) throws IOException {
        Path file = directory.file(uncompressedFile.getFileName() + "-compressed");
        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(file.toFile()));
                InputStream in = new BufferedInputStream(new FileInputStream(uncompressedFile.toFile()))) {
            out.putNextEntry(new ZipEntry(uncompressedFile.getFileName().toString()));
            IOUtils.copy(in, out);
        }
        return file;
    }

    private Path compressWithGZip(Path uncompressedFile) throws IOException {
        Path file = directory.file(uncompressedFile.getFileName() + "-compressed");
        try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(file.toFile()));
                InputStream in = new BufferedInputStream(new FileInputStream(uncompressedFile.toFile()))) {
            IOUtils.copy(in, out);
        }
        return file;
    }

    private Path createNodeInputDataFile(long roughSize) throws FileNotFoundException {
        Path file = directory.file("data-file");
        MutableLong bytesWritten = new MutableLong();
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file.toFile())) {
            @Override
            public synchronized void write(byte[] b, int off, int len) throws IOException {
                super.write(b, off, len);
                bytesWritten.add(len);
            }

            @Override
            public synchronized void write(int b) throws IOException {
                super.write(b);
                bytesWritten.add(1);
            }

            @Override
            public void write(byte[] b) throws IOException {
                super.write(b);
                bytesWritten.add(b.length);
            }
        };
        try (PrintWriter writer = new PrintWriter(out)) {
            writer.println(":ID,name:string,prop:int");
            while (bytesWritten.longValue() < roughSize) {
                writer.println(format(
                        "%s,%s,%d",
                        random.nextAlphaNumericString(6), random.nextAlphaNumericString(5, 20), random.nextInt()));
            }
        }
        return file;
    }

    private void assertNextRelationship(
            InputIterator relationship, Object startNode, Object endNode, String type, Map<String, Object> properties)
            throws IOException {
        assertNextRelationship(relationship, globalGroup, startNode, globalGroup, endNode, type, properties);
    }

    private void assertNextRelationship(
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

    private void assertNextNode(InputIterator data, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertNextNode(data, globalGroup, id, properties, labels);
    }

    private void assertNextNode(
            InputIterator data, Group group, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertNextNode(data, group, id, properties, labels, null);
    }

    private void assertNextNode(
            InputIterator data,
            Group group,
            Object id,
            Map<String, Object> properties,
            Set<String> labels,
            ApplicationMode action)
            throws IOException {
        assertThat(readNext(data)).isTrue();
        assertThat(visitor.idGroup).isEqualTo(group);
        assertThat(visitor.id()).isEqualTo(id);
        assertThat(asSet(visitor.labels())).hasSameElementsAs(labels);
        assertPropertiesEquals(properties, visitor.propertiesAsMap());
        assertThat(visitor.applicationMode).isEqualTo(action);
    }

    private void assertPropertiesEquals(Map<String, Object> expected, Map<String, Object> actual) {
        // Do this more complicated assert to handle primitive array equality
        assertThat(primitiveArraysAsLists(actual)).containsExactlyInAnyOrderEntriesOf(primitiveArraysAsLists(expected));
    }

    private Map<String, Object> primitiveArraysAsLists(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        for (var entry : map.entrySet()) {
            var value = entry.getValue();
            var cls = value.getClass();
            if (cls.isArray()) {
                List<Object> listValue = new ArrayList<>();
                var length = Array.getLength(value);
                for (var i = 0; i < length; i++) {
                    listValue.add(Array.get(value, i));
                }
                value = listValue;
            }
            result.put(entry.getKey(), value);
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

    private static Header.Factory header(final Header.Entry... entries) {
        return new Header.Factory() {
            @Override
            public boolean isDefined() {
                return true;
            }

            @Override
            public Header create(
                    CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups, Monitor monitor) {
                return new Header(entries);
            }
        };
    }

    private Header.Entry entry(String name, Type type, Extractor<?> extractor) {
        return entry(name, type, null, extractor);
    }

    private Header.Entry entry(String name, Type type, String groupName, Extractor<?> extractor) {
        return new Header.Entry(name, type, groups.getOrCreate(groupName), extractor);
    }

    private DataFactory data(String data) {
        return data(data, value -> value);
    }

    private DataFactory data(String data, Decorator decorator) {
        return DataFactories.data(decorator, defaultCharset(), writeContents(data));
    }

    private Path writeContents(String contents) {
        try {
            Path file = directory.getFileSystem().createTempFile(directory.homePath(), "data-", ".csv");
            Files.writeString(file, contents, defaultCharset());
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sourceDescription(Path file) {
        return file.toAbsolutePath().toString();
    }

    private static Iterable<DataFactory> dataIterable(DataFactory... data) {
        return Iterables.iterable(data);
    }

    private static class FailingNodeDecorator implements Decorator {
        private final RuntimeException failure;

        FailingNodeDecorator(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public InputEntityVisitor apply(InputEntityVisitor t) {
            return new InputEntityVisitor.Delegate(t) {
                @Override
                public void endOfEntity() {
                    throw failure;
                }
            };
        }
    }

    private enum MultilineSetting {
        LEGACY,
        ALLOW,
        DISALLOW
    }

    private static Configuration config(MultilineSetting setting) {
        var builder = COMMAS.toBuilder();
        builder = switch (setting) {
            case LEGACY -> builder.withLegacyMultilineBehaviour();
            case ALLOW -> builder.withMultilineDocuments(Predicates.alwaysTrue());
            case DISALLOW -> builder.withMultilineDocuments(Predicates.alwaysFalse());
        };
        return builder.build();
    }
}
