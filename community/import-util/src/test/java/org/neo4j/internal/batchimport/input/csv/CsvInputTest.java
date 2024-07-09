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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.batchimport.api.input.Collector.EMPTY;
import static org.neo4j.batchimport.api.input.IdType.ACTUAL;
import static org.neo4j.batchimport.api.input.IdType.INTEGER;
import static org.neo4j.batchimport.api.input.IdType.STRING;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.csv.reader.Configuration.TABS;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.internal.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.internal.batchimport.input.csv.CsvInput.NO_MONITOR;
import static org.neo4j.internal.batchimport.input.csv.Data.undecorated;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.internal.helpers.ArrayUtil.union;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
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
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Readables;
import org.neo4j.internal.batchimport.input.DuplicateHeaderException;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.csv.Header.Monitor;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
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
@ExtendWith(RandomExtension.class)
class CsvInputTest {
    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory directory;

    private final Extractors extractors = new Extractors(',');

    private final InputEntity visitor = new InputEntity();
    private final Groups groups = new Groups();
    private final Group globalGroup = groups.getOrCreate(null);
    private InputChunk chunk;
    private InputIterator referenceData;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProvideNodesFromCsvInput(boolean allowMultilineFields) throws Exception {
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
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 123L, properties("name", "Mattias Persson"), labels("HACKER"));
            assertFalse(chunk.next(visitor));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProvideRelationshipsFromCsvInput(boolean allowMultilineFields) throws Exception {
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
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, "node1", "node2", "KNOWS", properties("since", 1234567L));
            assertNextRelationship(relationships, "node2", "node10", "HACKS", properties("since", 987654L));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldCloseDataIteratorsInTheEnd(boolean allowMultilineFields) throws Exception {
        // GIVEN
        CapturingDataFactories nodeData = new CapturingDataFactories(config -> charReader("1"), NO_DECORATOR);
        CapturingDataFactories relationshipData = new CapturingDataFactories(
                config -> charReader("1,1"), InputEntityDecorators.defaultRelationshipType("TYPE"));

        IdType idType = IdType.STRING;
        Input input = new CsvInput(
                nodeData,
                header(entry(null, Type.ID, CsvInput.idExtractor(idType, extractors))),
                relationshipData,
                header(
                        entry(null, Type.START_ID, CsvInput.idExtractor(idType, extractors)),
                        entry(null, Type.END_ID, CsvInput.idExtractor(idType, extractors))),
                idType,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator iterator = input.nodes(EMPTY).iterator()) {
            readNext(iterator);
        }
        try (InputIterator iterator = input.relationships(EMPTY).iterator()) {
            readNext(iterator);
        }

        // THEN
        assertClosed(nodeData.last());
        assertClosed(relationshipData.last());
    }

    private static void assertClosed(CharReadable reader) {
        try {
            reader.read(new char[1], 0, 1);
            fail(reader + " not closed");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldCopeWithLinesThatHasTooFewValuesButStillValidates(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data =
                dataIterable(data("1,ultralisk,ZERG,10\n" + "2,corruptor,ZERG\n" + "3,mutalisk,ZERG,3"));
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
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("unit", "ultralisk", "kills", 10), labels("ZERG"));
            assertNextNode(nodes, 2L, properties("unit", "corruptor"), labels("ZERG"));
            assertNextNode(nodes, 3L, properties("unit", "mutalisk", "kills", 3), labels("ZERG"));
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnoreValuesAfterHeaderEntries(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = dataIterable(data("1,zergling,bubble,bobble\n" + "2,scv,pun,intended"));
        Input input = new CsvInput(
                data,
                header(entry(null, Type.ID, extractors.long_()), entry("name", Type.PROPERTY, extractors.string())),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("name", "zergling"), labels());
            assertNextNode(nodes, 2L, properties("name", "scv"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleMultipleInputGroups(boolean allowMultilineFields) throws Exception {
        // GIVEN multiple input groups, each with their own, specific, header
        DataFactory group1 = data(":ID,name,kills:int,health:int\n" + "1,Jim,10,100\n" + "2,Abathur,0,200\n");
        DataFactory group2 = data(":ID,type\n" + "3,zergling\n" + "4,csv\n");
        Iterable<DataFactory> data = dataIterable(group1, group2);
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN iterating over them, THEN the expected data should come out
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, "1", properties("name", "Jim", "kills", 10, "health", 100), labels());
            assertNextNode(nodes, "2", properties("name", "Abathur", "kills", 0, "health", 200), labels());
            assertNextNode(nodes, "3", properties("type", "zergling"), labels());
            assertNextNode(nodes, "4", properties("type", "csv"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProvideAdditiveLabels(boolean allowMultilineFields) throws Exception {
        // GIVEN
        String[] addedLabels = {"Two", "AddTwo"};
        DataFactory data = data(
                ":ID,name,:LABEL\n" + "0,First,\n" + "1,Second,One\n" + "2,Third,One;Two",
                InputEntityDecorators.additiveLabels(addedLabels));
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 0L, properties("name", "First"), labels(addedLabels));
            assertNextNode(nodes, 1L, properties("name", "Second"), labels(union(new String[] {"One"}, addedLabels)));
            assertNextNode(nodes, 2L, properties("name", "Third"), labels(union(new String[] {"One"}, addedLabels)));
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProvideDefaultRelationshipType(boolean allowMultilineFields) throws Exception {
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
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 0L, 1L, defaultType, emptyMap());
            assertNextRelationship(relationships, 1L, 2L, customType, emptyMap());
            assertNextRelationship(relationships, 2L, 1L, defaultType, emptyMap());
            assertFalse(readNext(relationships));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldAllowNodesWithoutIdHeader(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data("name:string,level:int\n" + "Mattias,1\n" + "Johan,2\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, null, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldAllowSomeNodesToBeAnonymous(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data =
                data(":ID,name:string,level:int\n" + "abc,Mattias,1\n" + ",Johan,2\n"); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldAllowNodesToBeAnonymousEvenIfIdHeaderIsNamed(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data =
                data("id:ID,name:string,level:int\n" + "abc,Mattias,1\n" + ",Johan,2\n"); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.STRING,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, "abc", properties("id", "abc", "name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, null, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldNotHaveIdSetAsPropertyIfIdHeaderEntryIsNamedForActualIds(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data =
                data("myId:ID,name:string,level:int\n" + "0,Mattias,1\n" + "1,Johan,2\n"); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                ACTUAL,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, null, 0L, properties("name", "Mattias", "level", 1), labels());
            assertNextNode(nodes, null, 1L, properties("name", "Johan", "level", 2), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnoreEmptyPropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,extra\n" + "0,Mattias,\n"
                + // here we leave out "extra" property
                "1,Johan,Additional\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", "Additional"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnoreEmptyIntPropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,extra:int\n" + "0,Mattias,\n"
                + // here we leave out "extra" property
                "1,Johan,10\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias"), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "extra", 10), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParsePointPropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,point:Point\n" + "0,Mattias,\"{x: 2.7, y:3.2 }\"\n"
                + "1,Johan,\" { height :0.01 ,longitude:5, latitude : -4.2 } \"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldNotParsePointPropertyValuesWithDuplicateKeys(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,point:Point\n"
                + "1,Johan,\" { height :0.01 ,longitude:5, latitude : -4.2, latitude : 4.2 } \"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            readNext(nodes);
            fail("Should have failed when key assigned multiple times, but didn't.");
        } catch (InputException ignore) {
            // this is fine
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParsePointPropertyValuesWithCRSInHeader(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,point:Point{crs:WGS-84-3D}\n"
                + "0,Johan,\" { height :0.01 ,longitude:5, latitude : -4.2 } \"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldUseHeaderInformationToParsePoint(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,point:Point{crs:WGS-84}\n" + "0,Johan,\" { x :1 ,y:2 } \"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseDatePropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,date:Date\n" + "0,Mattias,2018-02-27\n" + "1,Johan,2018-03-01\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 0L, properties("name", "Mattias", "date", DateValue.date(2018, 2, 27)), labels());
            assertNextNode(nodes, 1L, properties("name", "Johan", "date", DateValue.date(2018, 3, 1)), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseTimePropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data =
                data(":ID,name,time:Time\n" + "0,Mattias,13:37\n" + "1,Johan,\"16:20:01\"\n" + "2,Bob,07:30-05:00\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseTimePropertyValuesWithTimezoneInHeader(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,time:Time{timezone:+02:00}\n" + "0,Mattias,13:37\n"
                + "1,Johan,\"16:20:01\"\n"
                + "2,Bob,07:30-05:00\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseDateTimePropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,time:DateTime\n" + "0,Mattias,2018-02-27T13:37\n"
                + "1,Johan,\"2018-03-01T16:20:01\"\n"
                + "2,Bob,1981-05-11T07:30-05:00\n");

        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseDateTimePropertyValuesWithTimezoneInHeader(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,time:DateTime{timezone:Europe/Stockholm}\n" + "0,Mattias,2018-02-27T13:37\n"
                + "1,Johan,\"2018-03-01T16:20:01\"\n"
                + "2,Bob,1981-05-11T07:30-05:00\n");

        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseLocalTimePropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,time:LocalTime\n" + "0,Mattias,13:37\n" + "1,Johan,\"16:20:01\"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseLocalDateTimePropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:LocalDateTime\n" + "0,Mattias,2018-02-27T13:37\n" + "1,Johan,\"2018-03-01T16:20:01\"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseDurationPropertyValues(boolean allowMultilineFields) throws Exception {
        // GIVEN
        DataFactory data = data(":ID,name,duration:Duration\n" + "0,Mattias,P3MT13H37M\n" + "1,Johan,\"P-1YT4H20M\"\n");
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                dataIterable,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldFailOnArrayDelimiterBeingSameAsDelimiter(boolean allowMultilineFields) {
        // WHEN
        try {
            new CsvInput(
                    null,
                    null,
                    null,
                    null,
                    INTEGER,
                    config(allowMultilineFields).toBuilder()
                            .withDelimiter(',')
                            .withArrayDelimiter(',')
                            .build(),
                    false,
                    NO_MONITOR,
                    INSTANCE);

            fail("Should not be possible");
        } catch (IllegalArgumentException e) {
            // THEN
            assertTrue(e.getMessage().contains("array delimiter"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldFailOnQuotationCharacterBeingSameAsDelimiter(boolean allowMultilineFields) {
        // WHEN
        try {
            new CsvInput(
                    null,
                    null,
                    null,
                    null,
                    INTEGER,
                    config(allowMultilineFields).toBuilder()
                            .withDelimiter(',')
                            .withArrayDelimiter(';')
                            .withQuotationCharacter(',')
                            .build(),
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should not be possible");
        } catch (IllegalArgumentException e) {
            // THEN
            assertTrue(e.getMessage().contains("delimiter"));
            assertTrue(e.getMessage().contains("quotation"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldFailOnQuotationCharacterBeingSameAsArrayDelimiter(boolean allowMultilineFields) {
        // WHEN
        try {
            new CsvInput(
                    null,
                    null,
                    null,
                    null,
                    INTEGER,
                    config(allowMultilineFields).toBuilder()
                            .withQuotationCharacter(';')
                            .build(),
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should not be possible");
        } catch (IllegalArgumentException e) {
            // THEN
            assertTrue(e.getMessage().contains("array delimiter"));
            assertTrue(e.getMessage().contains("quotation"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHaveNodesBelongToGroupSpecifiedInHeader(boolean allowMultilineFields) throws Exception {
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
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, group, 123L, properties("name", "one"), labels());
            assertNextNode(nodes, group, 456L, properties("name", "two"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHaveRelationshipsSpecifyStartEndNodeIdGroupsInHeader(boolean allowMultilineFields) throws Exception {
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
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertRelationship(relationships, startNodeGroup, 123L, endNodeGroup, 234L, "TYPE", properties());
            assertRelationship(relationships, startNodeGroup, 345L, endNodeGroup, 456L, "TYPE", properties());
            assertFalse(readNext(relationships));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldDoWithoutRelationshipTypeHeaderIfDefaultSupplied(boolean allowMultilineFields) throws Exception {
        // GIVEN relationship data w/o :TYPE header
        String defaultType = "HERE";
        DataFactory data = data(
                ":START_ID,:END_ID,name\n" + "0,1,First\n" + "2,3,Second\n",
                InputEntityDecorators.defaultRelationshipType(defaultType));
        Iterable<DataFactory> dataIterable = dataIterable(data);
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                dataIterable,
                defaultFormatRelationshipFileHeader(),
                INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            // THEN
            assertNextRelationship(relationships, 0L, 1L, defaultType, properties("name", "First"));
            assertNextRelationship(relationships, 2L, 3L, defaultType, properties("name", "Second"));
            assertFalse(readNext(relationships));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnoreNodeEntriesMarkedIgnoreUsingHeader(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data =
                datas(CsvInputTest.data(":ID,name:IGNORE,other:int,:LABEL\n" + "1,Mattias,10,Person\n"
                        + "2,Johan,111,Person\n"
                        + "3,Emil,12,Person"));
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatNodeFileHeader(),
                IdType.INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, properties("other", 10), labels("Person"));
            assertNextNode(nodes, 2L, properties("other", 111), labels("Person"));
            assertNextNode(nodes, 3L, properties("other", 12), labels("Person"));
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnoreRelationshipEntriesMarkedIgnoreUsingHeader(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data =
                datas(CsvInputTest.data(":START_ID,:TYPE,:END_ID,prop:IGNORE,other:int\n" + "1,KNOWS,2,Mattias,10\n"
                        + "2,KNOWS,3,Johan,111\n"
                        + "3,KNOWS,4,Emil,12"));
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                data,
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            assertNextRelationship(relationships, 1L, 2L, "KNOWS", properties("other", 10));
            assertNextRelationship(relationships, 2L, 3L, "KNOWS", properties("other", 111));
            assertNextRelationship(relationships, 3L, 4L, "KNOWS", properties("other", 12));
            assertFalse(readNext(relationships));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldPropagateExceptionFromFailingDecorator(boolean allowMultilineFields) throws Exception {
        // GIVEN
        RuntimeException failure = new RuntimeException("FAILURE");
        Iterable<DataFactory> data = datas(CsvInputTest.data(":ID,name\n1,Mattias", new FailingNodeDecorator(failure)));
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatNodeFileHeader(),
                IdType.INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            readNext(nodes);
        } catch (InputException e) {
            // THEN
            assertSame(e.getCause(), failure);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldNotIncludeEmptyArraysInEntities(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data =
                datas(CsvInputTest.data(":ID,sprop:String[],lprop:long[]\n" + "1,,\n" + "2,a;b,10;20"));
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatNodeFileHeader(),
                IdType.INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN/THEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            assertNextNode(nodes, 1L, emptyMap(), labels());
            assertNextNode(
                    nodes, 2L, properties("sprop", new String[] {"a", "b"}, "lprop", new long[] {10, 20}), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldTreatEmptyQuotedStringsAsNullIfConfiguredTo(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(CsvInputTest.data(":ID,one,two,three\n" + "1,\"\",,value"));
        Configuration config = config(allowMultilineFields).toBuilder()
                .withEmptyQuotedStringsAsNull(true)
                .build();

        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config,
                false,
                NO_MONITOR,
                INSTANCE);

        // WHEN
        try (InputIterator nodes = input.nodes(EMPTY).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("three", "value"), labels());
            assertFalse(readNext(nodes));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldIgnoreEmptyExtraColumns(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(CsvInputTest.data(":ID,one\n" + "1,test,\n" + "2,test,,additional"));

        // WHEN
        Collector collector = mock(Collector.class);
        Input input = new CsvInput(
                data,
                defaultFormatNodeFileHeader(),
                datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                INSTANCE);

        // THEN
        try (InputIterator nodes = input.nodes(collector).iterator()) {
            // THEN
            assertNextNode(nodes, 1L, properties("one", "test"), labels());
            assertNextNode(nodes, 2L, properties("one", "test"), labels());
            assertFalse(readNext(nodes));
        }
        verify(collector).collectExtraColumns(anyString(), eq(1L), eq(null));
        verify(collector).collectExtraColumns(anyString(), eq(2L), eq(null));
        verify(collector).collectExtraColumns(anyString(), eq(2L), eq("additional"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSkipRelationshipValidationIfToldTo(boolean allowMultilineFields) throws Exception {
        // GIVEN
        Iterable<DataFactory> data = datas(CsvInputTest.data(":START_ID,:END_ID,:TYPE\n" + ",,"));
        Input input = new CsvInput(
                datas(),
                defaultFormatNodeFileHeader(),
                data,
                defaultFormatRelationshipFileHeader(),
                IdType.INTEGER,
                config(allowMultilineFields),
                false,
                NO_MONITOR,
                groups,
                INSTANCE);

        // WHEN
        try (InputIterator relationships = input.relationships(EMPTY).iterator()) {
            readNext(relationships);
            assertNull(visitor.startId());
            assertNull(visitor.endId());
            assertNull(visitor.stringType);
        }
    }

    @Test
    void shouldFailOnUnparsableNodeHeader() {
        // given
        Iterable<DataFactory> data = datas(data(":SOMETHING,abcde#rtg:123,"));

        try {
            // when
            new CsvInput(
                    data,
                    defaultFormatNodeFileHeader(),
                    datas(),
                    defaultFormatRelationshipFileHeader(),
                    IdType.INTEGER,
                    COMMAS,
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should not parse");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldFailOnUnparsableRelationshipHeader() {
        // given
        Iterable<DataFactory> data = datas(data(":SOMETHING,abcde#rtg:123,"));

        try {
            // when
            new CsvInput(
                    datas(),
                    defaultFormatNodeFileHeader(),
                    data,
                    defaultFormatRelationshipFileHeader(),
                    IdType.INTEGER,
                    COMMAS,
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should not parse");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldFailOnUndefinedGroupInRelationshipHeader() {
        // given
        Iterable<DataFactory> nodeData = datas(data(":ID(left)"), data(":ID(right)"));
        Iterable<DataFactory> relationshipData = datas(data(":START_ID(left),:END_ID(rite)"));

        try {
            // when
            new CsvInput(
                    nodeData,
                    defaultFormatNodeFileHeader(),
                    relationshipData,
                    defaultFormatRelationshipFileHeader(),
                    IdType.INTEGER,
                    COMMAS,
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should not validate");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldFailOnGlobalGroupInRelationshipHeaderIfNoGLobalGroupInNodeHeader() {
        // given
        Iterable<DataFactory> nodeData = datas(data(":ID(left)"), data(":ID(right)"));
        Iterable<DataFactory> relationshipData = datas(data(":START_ID(left),:END_ID(rite)"));

        try {
            // when
            new CsvInput(
                    nodeData,
                    defaultFormatNodeFileHeader(),
                    relationshipData,
                    defaultFormatRelationshipFileHeader(),
                    IdType.INTEGER,
                    COMMAS,
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should not validate");
        } catch (InputException e) {
            // then
            // OK
        }
    }

    @Test
    void shouldReportDuplicateNodeSourceFiles() {
        // given
        String sourceDescription = "The single data source";
        Supplier<CharReadable> source = () -> wrap(dataWithSourceDescription(":ID", sourceDescription), 3);
        Iterable<DataFactory> data = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                // Contains two of the same file
                return asRawIterator(iterator(source.get(), source.get()));
            }

            @Override
            public Decorator decorator() {
                return NO_DECORATOR;
            }
        });
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
                INSTANCE);

        // then
        verify(monitor).duplicateSourceFile(sourceDescription);
    }

    @Test
    void shouldReportDuplicateRelationshipSourceFiles() {
        // given
        String sourceDescription = "The single data source";
        Supplier<CharReadable> source =
                () -> wrap(dataWithSourceDescription(":START_ID,:END_ID,:TYPE", sourceDescription), 3);
        Iterable<DataFactory> data = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                // Contains two of the same file
                return asRawIterator(iterator(source.get(), source.get()));
            }

            @Override
            public Decorator decorator() {
                return NO_DECORATOR;
            }
        });
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
                INSTANCE);

        // then
        verify(monitor).duplicateSourceFile(sourceDescription);
    }

    @Test
    void shouldReportDuplicateSourceFileUsedAsBothNodeAndRelationshipSourceFile() {
        // given
        String sourceDescription = "The single data source";
        Supplier<CharReadable> nodeHeaderSource = () -> wrap(dataWithSourceDescription(":ID", "node source"), 3);
        Supplier<CharReadable> relationshipHeaderSource =
                () -> wrap(dataWithSourceDescription(":START_ID,:END_ID,:TYPE", "relationship source"), 10);
        Supplier<CharReadable> source = () -> wrap(dataWithSourceDescription("1,2,3", sourceDescription), 6);
        Iterable<DataFactory> nodeData = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return asRawIterator(iterator(nodeHeaderSource.get(), source.get()));
            }

            @Override
            public Decorator decorator() {
                return NO_DECORATOR;
            }
        });
        Iterable<DataFactory> relationshipData = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return asRawIterator(iterator(relationshipHeaderSource.get(), source.get()));
            }

            @Override
            public Decorator decorator() {
                return NO_DECORATOR;
            }
        });
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
                INSTANCE);

        // then
        verify(monitor).duplicateSourceFile(sourceDescription);
    }

    private static Reader dataWithSourceDescription(String data, String sourceDescription) {
        return new StringReader(data) {
            @Override
            public String toString() {
                return sourceDescription;
            }
        };
    }

    @Test
    void shouldNormalizeTypes() throws IOException {
        // given
        Iterable<DataFactory> nodeData = datas(
                data("source1", ":ID,shortProp:short,intProp:int"),
                data("source2", ":ID,floatProp:float,doubleProp:double"));
        Iterable<DataFactory> relationshipData =
                datas(data("source3", ":START_ID,:END_ID,byteProp:byte,longProp:long"));
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
                INSTANCE);
        input.calculateEstimates((values, NULL, INSTANCE) -> 1 /*doesn't quite matter*/);

        // then
        verify(monitor, times(1)).typeNormalized("source1", "shortProp", "short", "long");
        verify(monitor, times(1)).typeNormalized("source1", "intProp", "int", "long");
        verify(monitor, times(1)).typeNormalized("source2", "floatProp", "float", "double");
        verify(monitor, times(1)).typeNormalized("source3", "byteProp", "byte", "long");
        verifyNoMoreInteractions(monitor);
    }

    @Test
    void shouldCalculateCorrectEstimatesForZippedInputFile() throws IOException {
        // GIVEN
        IdType idType = STRING;
        Path uncompressedFile = createNodeInputDataFile(mebiBytes(10));
        Path compressedFile = compressWithZip(uncompressedFile);
        Assertions.assertThat(Files.size(compressedFile)).isLessThan(Files.size(uncompressedFile));

        // WHEN
        Input.Estimates uncompressedEstimates = calculateEstimatesOnSingleFileNodeData(idType, uncompressedFile);
        Input.Estimates compressedEstimates = calculateEstimatesOnSingleFileNodeData(idType, compressedFile);

        // then
        assertEstimatesEquals(uncompressedEstimates, compressedEstimates, 0);
    }

    @Test
    void shouldCalculateCorrectEstimatesForGZippedInputFile() throws IOException {
        // GIVEN
        IdType idType = STRING;
        Path uncompressedFile = createNodeInputDataFile(mebiBytes(10));
        Path compressedFile = compressWithGZip(uncompressedFile);
        Assertions.assertThat(Files.size(compressedFile)).isLessThan(Files.size(uncompressedFile));

        // WHEN
        Input.Estimates uncompressedEstimates = calculateEstimatesOnSingleFileNodeData(idType, uncompressedFile);
        Input.Estimates compressedEstimates = calculateEstimatesOnSingleFileNodeData(idType, compressedFile);

        // then the compressed and uncompressed should be _roughly_ equal. The thing with GZIP is that there's no
        // reliable way
        // of getting the uncompressed data size w/o decompressing it in its entirety, and this is why the estimator
        // doesn't do this
        // but instead tries to estimate its compression rate after reading a chunk of it
        assertEstimatesEquals(uncompressedEstimates, compressedEstimates, 0.01);
    }

    @Test
    void shouldReportNoNodeLabels() {
        // given
        String sourceDescription = "source";
        Supplier<CharReadable> headerSource = () -> wrap(dataWithSourceDescription(":ID", sourceDescription), 3);
        Iterable<DataFactory> data = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return asRawIterator(iterator(headerSource.get()));
            }

            @Override
            public Decorator decorator() {
                return NO_DECORATOR;
            }
        });
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
                INSTANCE);

        // then
        verify(monitor).noNodeLabelsSpecified(sourceDescription);
    }

    @Test
    void shouldNotReportNoNodeLabelsIfDecorated() {
        // given
        String sourceDescription = "source";
        Supplier<CharReadable> headerSource = () -> wrap(dataWithSourceDescription(":ID", sourceDescription), 3);
        Iterable<DataFactory> data = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return asRawIterator(iterator(headerSource.get()));
            }

            @Override
            public Decorator decorator() {
                return additiveLabels(new String[] {"MyLabel"});
            }
        });
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
                INSTANCE);

        // then
        verify(monitor, never()).noRelationshipTypeSpecified(sourceDescription);
    }

    @Test
    void shouldReportNoRelationshipType() {
        // given
        String sourceDescription = "source";
        Supplier<CharReadable> headerSource =
                () -> wrap(dataWithSourceDescription(":START_ID,:END_ID", sourceDescription), 3);
        Iterable<DataFactory> data = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return asRawIterator(iterator(headerSource.get()));
            }

            @Override
            public Decorator decorator() {
                return NO_DECORATOR;
            }
        });
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
                INSTANCE);

        // then
        verify(monitor).noRelationshipTypeSpecified(sourceDescription);
    }

    @Test
    void shouldNotReportNoRelationshipTypeIfDecorated() {
        // given
        String sourceDescription = "source";
        Supplier<CharReadable> headerSource =
                () -> wrap(dataWithSourceDescription(":START_ID,:END_ID", sourceDescription), 3);
        Iterable<DataFactory> data = datas(config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return asRawIterator(iterator(headerSource.get()));
            }

            @Override
            public Decorator decorator() {
                return defaultRelationshipType("MyType");
            }
        });
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
                INSTANCE);

        // then
        verify(monitor, never()).noRelationshipTypeSpecified(sourceDescription);
    }

    @Test
    void shouldReportDuplicateNodeHeader() throws FileNotFoundException {
        // GIVEN
        Path file = writeFile("node-header", ":ID,name:string,name");

        // WHEN
        try {
            new CsvInput(
                    datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                    defaultFormatNodeFileHeader(),
                    datas(),
                    defaultFormatRelationshipFileHeader(),
                    STRING,
                    COMMAS,
                    false,
                    NO_MONITOR,
                    INSTANCE);
            fail("Should have failed");
        } catch (DuplicateHeaderException e) {
            // THEN
            assertThat(e).hasMessageContaining(file.getFileName().toString());
        }
    }

    @Test
    void shouldReportDuplicateRelationshipHeader() throws FileNotFoundException {
        // GIVEN
        Path file = writeFile("relationship-header", ":START_ID,:TYPE,:END_ID,:TYPE,name:string");

        // WHEN
        try {
            new CsvInput(
                    datas(),
                    defaultFormatNodeFileHeader(),
                    datas(DataFactories.data(NO_DECORATOR, defaultCharset(), file)),
                    defaultFormatRelationshipFileHeader(),
                    STRING,
                    COMMAS,
                    false,
                    NO_MONITOR,
                    groups,
                    INSTANCE);
            fail("Should have failed");
        } catch (DuplicateHeaderException e) {
            // THEN
            assertThat(e).hasMessageContaining(file.getFileName().toString());
        }
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
                INSTANCE)) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));

            // then
            var referencedNodeSchema = input.referencedNodeSchema(tokenHolders);
            assertThat(referencedNodeSchema.get("MyGroup"))
                    .isEqualTo(SchemaDescriptors.forLabel(
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
                INSTANCE)) {
            // when
            var tokenHolders = new TokenHolders(
                    tokenHolder(Map.of("myId", 4)), tokenHolder(Map.of("Person", 2)), tokenHolder(Map.of()));
            var schema = input.referencedNodeSchema(tokenHolders);

            // then
            Assertions.assertThat(schema).isEqualTo(Map.of("My Group", SchemaDescriptors.forLabel(2, 4)));
        }
    }

    @Test
    void shouldStoreIdAsPropertyInSpecificValueType() throws IOException {
        // given nodes w/ IDs as ints
        var nodeData = datas(CsvInputTest.data("id:ID{id-type:int},prop\n123,val"));

        // when using string id-type in the input
        try (var input = new CsvInput(
                        nodeData,
                        defaultFormatNodeFileHeader(),
                        datas(),
                        defaultFormatRelationshipFileHeader(),
                        STRING,
                        config(false),
                        false,
                        NO_MONITOR,
                        INSTANCE);
                var nodes = input.nodes(EMPTY).iterator()) {
            // then
            assertNextNode(nodes, 123, properties("id", 123, "prop", "val"), labels());
            assertFalse(readNext(nodes));
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
                INSTANCE)) {
            try (var nodes = input.nodes(Collector.STRICT).iterator()) {
                assertNextNode(
                        nodes, "ABC123", properties("id1", "ABC", "id2", "123", "name", "First"), Set.of("Person"));
                assertNextNode(
                        nodes, "ABC456", properties("id1", "ABC", "id2", "456", "name", "Second"), Set.of("Person"));
                assertFalse(readNext(nodes));
            }
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
                        INSTANCE))
                .isInstanceOf(InputException.class)
                .hasMessageContaining("Cannot store composite IDs");
    }

    @Test
    void shouldFailOnCompositeIdColumnsForDifferntGroups() throws IOException {
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
                        INSTANCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("referring to different groups");
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
        assertEquals(a.numberOfNodes(), b.numberOfNodes(), a.numberOfNodes() * errorMargin);
        assertEquals(a.numberOfNodeLabels(), b.numberOfNodeLabels(), a.numberOfNodeLabels() * errorMargin);
        assertEquals(a.numberOfNodeProperties(), b.numberOfNodeProperties(), a.numberOfNodeProperties() * errorMargin);
        assertEquals(a.numberOfRelationships(), b.numberOfRelationships(), a.numberOfRelationships() * errorMargin);
        assertEquals(
                a.numberOfRelationshipProperties(),
                b.numberOfRelationshipProperties(),
                a.numberOfRelationshipProperties() * errorMargin);
        assertEquals(a.sizeOfNodeProperties(), b.sizeOfNodeProperties(), a.sizeOfNodeProperties() * errorMargin);
        assertEquals(
                a.sizeOfRelationshipProperties(),
                b.sizeOfRelationshipProperties(),
                a.sizeOfRelationshipProperties() * errorMargin);
    }

    private static Input.Estimates calculateEstimatesOnSingleFileNodeData(IdType idType, Path nodeDataFile)
            throws IOException {
        Input input = new CsvInput(
                dataIterable(config -> undecorated(() -> Readables.individualFiles(defaultCharset(), nodeDataFile))),
                defaultFormatNodeFileHeader(),
                emptyList(),
                defaultFormatRelationshipFileHeader(),
                idType,
                COMMAS,
                false,
                NO_MONITOR,
                INSTANCE);
        // We don't care about correct value size calculation really, as long as it's consistent
        return input.calculateEstimates((values, tracer, memTracker) ->
                Stream.of(values).mapToInt(v -> v.toString().length()).sum());
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
                        random.nextAlphaNumericString(6, 6), random.nextAlphaNumericString(5, 20), random.nextInt()));
            }
        }
        return file;
    }

    private static Data dataItem(final CharReadable data, final Decorator decorator) {
        return DataFactories.data(decorator, () -> data).create(COMMAS /*doesn't matter here in this test*/);
    }

    private void assertNextRelationship(
            InputIterator relationship, Object startNode, Object endNode, String type, Map<String, Object> properties)
            throws IOException {
        assertRelationship(relationship, globalGroup, startNode, globalGroup, endNode, type, properties);
    }

    private void assertRelationship(
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

    private void assertNextNode(InputIterator data, Object id, Map<String, Object> properties, Set<String> labels)
            throws IOException {
        assertNextNode(data, globalGroup, id, properties, labels);
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

    private void assertPropertiesEquals(Map<String, Object> expected, Map<String, Object> actual) {
        // Do this more complicated assert to handle primitive array equality
        assertEquals(primitiveArraysAsLists(expected), primitiveArraysAsLists(actual));
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

    private static DataFactory data(String sourceDescription, String data) {
        return config -> dataItem(charReader(sourceDescription, data), d -> d);
    }

    private static DataFactory data(String data) {
        return data(data, value -> value);
    }

    private static DataFactory data(final String data, final Decorator decorator) {
        return config -> dataItem(charReader(data), decorator);
    }

    private static CharReadable charReader(String sourceDescription, String data) {
        return wrap(sourceDescription, data);
    }

    private static CharReadable charReader(String data) {
        return wrap(data);
    }

    private static Iterable<DataFactory> dataIterable(DataFactory... data) {
        return Iterables.iterable(data);
    }

    private static class CapturingDataFactories implements Iterable<DataFactory> {
        private final Function<Configuration, CharReadable> factory;
        private CharReadable last;
        private final Decorator decorator;

        CapturingDataFactories(Function<Configuration, CharReadable> factory, Decorator decorator) {
            this.factory = factory;
            this.decorator = decorator;
        }

        @Override
        public Iterator<DataFactory> iterator() {
            return Iterators.iterator(config -> new Data() {
                @Override
                public RawIterator<CharReadable, IOException> stream() {
                    last = factory.apply(config);
                    return Readables.iterator(in -> in, last);
                }

                @Override
                public Decorator decorator() {
                    return decorator;
                }
            });
        }

        CharReadable last() {
            return last;
        }
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

    private static Configuration config(boolean allowMultilineFields) {
        return COMMAS.toBuilder().withMultilineFields(allowMultilineFields).build();
    }
}
