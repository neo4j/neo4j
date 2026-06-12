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
package org.neo4j.importer;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.cli.CommandTestUtils.capturingExecutionContext;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.helpers.collection.MapUtil.store;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.storemigration.StoreMigrationTestUtils.getStoreVersion;

import blue.strategic.parquet.ParquetWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.CommandTestUtils;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.importer.FileImporter.CsvImportException;
import org.neo4j.internal.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.internal.batchimport.input.HeaderException;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.input.MissingRelationshipDataException;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

@Neo4jLayoutExtension
@RandomSupportExtension
class ImportCommandTest {
    private static final int MAX_LABEL_ID = 4;
    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;
    private static final IntPredicate TRUE = i -> true;

    private static final String REPORT = "import.report";

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private Neo4jLayout layout;

    @Inject
    private RandomSupport random;

    private DatabaseManagementService managementService;
    private int dataIndex;

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void shouldImportAndCreateTokenIndexes() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();

        // WHEN
        var ctx = capturingCtx();
        runImport(
                ctx,
                "--nodes",
                nodeData(true, COMMAS, nodeIds, TRUE).toAbsolutePath().toString(),
                "--high-parallel-io",
                "off",
                "--relationships",
                relationshipData(true, COMMAS, nodeIds, TRUE, true)
                        .toAbsolutePath()
                        .toString());

        // THEN
        assertTrue(ctx.outAsString().contains("IMPORT DONE"));
        assertTokenIndexesCreated();
        verifyData();
    }

    @Test
    void shouldNotBeAllowedToImportToOnlineDb() {
        List<String> nodeIds = nodeIds();

        // Started neo4j db
        var db = getDatabaseApi();

        var ctx = capturingCtx();
        assertThatThrownBy(() -> runImport(
                        ctx,
                        db.databaseName(),
                        "--nodes",
                        nodeData(true, COMMAS, nodeIds, TRUE).toAbsolutePath().toString(),
                        "--high-parallel-io",
                        "off",
                        "--relationships",
                        relationshipData(true, COMMAS, nodeIds, TRUE, true)
                                .toAbsolutePath()
                                .toString(),
                        "--overwrite-destination" // Overwrite to not get complaint that it contains data
                        ))
                .isInstanceOf(CommandFailedException.class)
                .hasCauseInstanceOf(FileLockException.class);
    }

    @Test
    void shouldNotImportOnEmptyExistingDatabase() {
        // Given a db with default token indexes
        var dbName = createDefaultDatabaseWithTokenIndexes();
        List<String> nodeIds = nodeIds();
        Configuration config = COMMAS;

        // When csv is imported
        assertThatThrownBy(() -> runImport(
                        dbName,
                        "--nodes",
                        nodeData(true, config, nodeIds, TRUE).toAbsolutePath().toString(),
                        "--high-parallel-io",
                        "off",
                        "--relationships",
                        relationshipData(true, config, nodeIds, TRUE, true)
                                .toAbsolutePath()
                                .toString()))
                .isInstanceOf(CommandFailedException.class)
                .hasCauseInstanceOf(CsvImportException.class)
                .cause()
                .hasCauseInstanceOf(DirectoryNotEmptyException.class);
    }

    private void assertTokenIndexesCreated() {
        try (var dbms = dbmsService();
                var tx = dbms.database(DEFAULT_DATABASE_NAME).beginTx()) {
            var indexes = stream(tx.schema().getIndexes().spliterator(), false).toList();
            assertThat(indexes.stream()
                            .filter(index -> index.getIndexType() == LOOKUP)
                            .count())
                    .isEqualTo(2);
            assertTrue(indexes.stream().anyMatch(IndexDefinition::isNodeIndex));
            assertTrue(indexes.stream().anyMatch(IndexDefinition::isRelationshipIndex));
        }
    }

    @Test
    void shouldImportWithHeadersBeingInSeparateFiles() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        runImport(
                "--delimiter",
                "TAB",
                "--array-delimiter",
                String.valueOf(config.arrayDelimiter()),
                "--nodes",
                nodeHeader(config).toAbsolutePath() + ","
                        + nodeData(false, config, nodeIds, TRUE).toAbsolutePath(),
                "--relationships",
                relationshipHeader(config).toAbsolutePath() + ","
                        + relationshipData(false, config, nodeIds, TRUE, true).toAbsolutePath());

        // THEN
        verifyData();
    }

    @Test
    void import4097Labels() throws Exception {
        // GIVEN
        Path header = file(fileName("4097labels-header.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(header))) {
            writer.println(":LABEL");
        }
        Path data = file(fileName("4097labels.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            // Need to have unique names in order to get unique ids for labels. Want 4096 unique label ids present.
            for (int i = 0; i < 4096; i++) {
                writer.println("SIMPLE" + i);
            }
            // Then insert one with 3 array entries which will get ids greater than 4096. These cannot be inlined
            // due 36 bits being divided into 3 parts of 12 bits each and 4097 > 2^12, thus these labels will be
            // need to be dynamic records.
            writer.println("FIRST 4096|SECOND 4096|THIRD 4096");
        }

        // WHEN
        runImport(
                "--delimiter",
                "TAB",
                "--array-delimiter",
                "|",
                "--nodes",
                header.toAbsolutePath() + "," + data.toAbsolutePath());

        // THEN
        GraphDatabaseService databaseService = getDatabaseApi();
        try (Transaction tx = databaseService.beginTx()) {
            long nodeCount = Iterables.count(tx.getAllNodes());
            assertEquals(4097, nodeCount);

            assertThat(Iterators.count(tx.findNodes(label("FIRST 4096")))).isEqualTo(1);
            assertThat(Iterators.count(tx.findNodes(label("SECOND 4096")))).isEqualTo(1);
            tx.commit();
        }
    }

    @Test
    void shouldIgnoreWhitespaceAroundIntegers() throws Exception {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        List<String> values = Arrays.asList("17", "    21", "99   ", "  34  ", "-34", "        -12", "-92 ");

        Path data = file(fileName("whitespace.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            writer.println(":LABEL,name,s:short,b:byte,i:int,l:long,f:float,d:double");

            // For each test value
            for (String value : values) {
                // Save value as a String in name
                writer.print("PERSON,'" + value + "'");
                // For each numerical type
                for (int j = 0; j < 6; j++) {
                    writer.print("," + value);
                }
                // End line
                writer.println();
            }
        }

        // WHEN
        runImport("--quote", "'", "--nodes", data.toAbsolutePath().toString());

        // THEN
        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try (Transaction tx = databaseApi.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                nodeCount++;
                String name = (String) node.getProperty("name");

                String expected = name.trim();

                assertEquals(7, node.getAllProperties().size());
                for (String key : node.getPropertyKeys()) {
                    if (key.equals("name")) {
                        continue;
                    } else if (key.equals("f") || key.equals("d")) {
                        // Floating points have decimals
                        expected = String.valueOf(Double.parseDouble(expected));
                    }

                    assertEquals(expected, node.getProperty(key).toString(), "Wrong value for " + key);
                }
            }

            tx.commit();
        }

        assertEquals(values.size(), nodeCount);
    }

    @Test
    void shouldIgnoreWhitespaceAroundDecimalNumbers() throws Exception {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        List<String> values = Arrays.asList(
                "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745", "-412.153    ", "   -5.12   ");

        Path data = file(fileName("whitespace.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            writer.println(":LABEL,name,f:float,d:double");

            // For each test value
            for (String value : values) {
                // Save value as a String in name
                writer.print("PERSON,'" + value + "'");
                // For each numerical type
                for (int j = 0; j < 2; j++) {
                    writer.print("," + value);
                }
                // End line
                writer.println();
            }
        }

        // WHEN
        runImport("--quote", "'", "--nodes", data.toAbsolutePath().toString());

        // THEN
        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try (Transaction tx = databaseApi.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                nodeCount++;
                String name = (String) node.getProperty("name");

                double expected = Double.parseDouble(name.trim());

                assertEquals(3, node.getAllProperties().size());
                for (String key : node.getPropertyKeys()) {
                    if (key.equals("name")) {
                        continue;
                    }

                    assertEquals(
                            expected,
                            Double.parseDouble(node.getProperty(key).toString()),
                            0.0,
                            "Wrong value for " + key);
                }
            }

            tx.commit();
        }

        assertEquals(values.size(), nodeCount);
    }

    @Test
    void shouldIgnoreWhitespaceAroundBooleans() throws Exception {
        // GIVEN
        Path data = file(fileName("whitespace.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            writer.println(":LABEL,name,adult:boolean");

            writer.println("PERSON,'t1',true");
            writer.println("PERSON,'t2',  true");
            writer.println("PERSON,'t3',true  ");
            writer.println("PERSON,'t4',  true  ");

            writer.println("PERSON,'f1',false");
            writer.println("PERSON,'f2',  false");
            writer.println("PERSON,'f3',false  ");
            writer.println("PERSON,'f4',  false  ");
            writer.println("PERSON,'f5',  truebutactuallyfalse  ");

            writer.println("PERSON,'f6',  non true things are interpreted as false  ");
        }

        // WHEN
        runImport("--quote", "'", "--nodes", data.toAbsolutePath().toString());

        // THEN
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try (Transaction tx = databaseApi.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                String name = (String) node.getProperty("name");
                if (name.startsWith("t")) {
                    assertTrue((boolean) node.getProperty("adult"), "Wrong value on " + name);
                } else {
                    assertFalse((boolean) node.getProperty("adult"), "Wrong value on " + name);
                }
            }

            long nodeCount = Iterables.count(tx.getAllNodes());
            assertEquals(10, nodeCount);
            tx.commit();
        }
    }

    @Test
    void shouldIgnoreWhitespaceInAndAroundIntegerArrays() throws Exception {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values = new String[] {"   17", "21", "99   ", "  34  ", "-34", "        -12", "-92 "};

        Path data = writeArrayCsv(
                new String[] {"s:short[]", "b:byte[]", "i:int[]", "l:long[]", "f:float[]", "d:double[]"}, values);

        // WHEN
        runImport("--quote", "'", "--nodes", data.toAbsolutePath().toString());

        // THEN
        // Expected value for integer types
        String iExpected = joinStringArray(values);

        // Expected value for floating point types
        String fExpected = Arrays.stream(values)
                .map(String::trim)
                .map(Double::valueOf)
                .map(String::valueOf)
                .collect(joining(", ", "[", "]"));

        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try (Transaction tx = databaseApi.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                nodeCount++;

                assertEquals(6, node.getAllProperties().size());
                for (String key : node.getPropertyKeys()) {
                    Object things = node.getProperty(key);
                    String result = "";
                    String expected = iExpected;
                    switch (key) {
                        case "s" -> result = Arrays.toString((short[]) things);
                        case "b" -> result = Arrays.toString((byte[]) things);
                        case "i" -> result = Arrays.toString((int[]) things);
                        case "l" -> result = Arrays.toString((long[]) things);
                        case "f" -> {
                            result = Arrays.toString((float[]) things);
                            expected = fExpected;
                        }
                        case "d" -> {
                            result = Arrays.toString((double[]) things);
                            expected = fExpected;
                        }
                        default -> {}
                    }

                    assertEquals(expected, result);
                }
            }

            tx.commit();
        }

        assertEquals(1, nodeCount);
    }

    @Test
    void shouldIgnoreWhitespaceInAndAroundDecimalArrays() throws Exception {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values = new String[] {
            "1.0", "   3.5", "45.153    ", "   925.12   ", "-2.121", "   -3.745", "-412.153    ", "   -5.12   "
        };

        Path data = writeArrayCsv(new String[] {"f:float[]", "d:double[]"}, values);

        // WHEN
        runImport("--quote", "'", "--nodes", data.toAbsolutePath().toString());

        // THEN
        String expected = joinStringArray(values);

        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try (Transaction tx = databaseApi.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                nodeCount++;

                assertEquals(2, node.getAllProperties().size());
                for (String key : node.getPropertyKeys()) {
                    Object things = node.getProperty(key);
                    String result =
                            switch (key) {
                                case "f" -> Arrays.toString((float[]) things);
                                case "d" -> Arrays.toString((double[]) things);
                                default -> "";
                            };

                    assertEquals(expected, result);
                }
            }

            tx.commit();
        }

        assertEquals(1, nodeCount);
    }

    @Test
    void shouldIgnoreWhitespaceInAndAroundBooleanArrays() throws Exception {
        // GIVEN
        // Faster to do all successful in one import than in N separate tests
        String[] values =
                new String[] {"true", "  true", "true   ", "  true  ", " false ", "false ", " false", "false ", " false"
                };
        String expected = joinStringArray(values);

        Path data = writeArrayCsv(new String[] {"b:boolean[]"}, values);

        // WHEN
        runImport("--quote", "'", "--nodes", data.toAbsolutePath().toString());

        // THEN
        int nodeCount = 0;
        GraphDatabaseAPI databaseApi = getDatabaseApi();
        try (Transaction tx = databaseApi.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                nodeCount++;

                assertEquals(1, node.getAllProperties().size());
                for (String key : node.getPropertyKeys()) {
                    Object things = node.getProperty(key);
                    String result = Arrays.toString((boolean[]) things);

                    assertEquals(expected, result);
                }
            }

            tx.commit();
        }

        assertEquals(1, nodeCount);
    }

    @Test
    void shouldFailIfHeaderHasLessColumnsThanData() {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        var ctx = capturingCtx();
        assertThatThrownBy(() -> runImport(
                        ctx,
                        "--delimiter",
                        "TAB",
                        "--array-delimiter",
                        String.valueOf(config.arrayDelimiter()),
                        "--nodes",
                        nodeHeader(config).toAbsolutePath() + ","
                                + nodeData(false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns)
                                        .toAbsolutePath(),
                        "--relationships",
                        relationshipHeader(config).toAbsolutePath() + ","
                                + relationshipData(false, config, nodeIds, TRUE, true)
                                        .toAbsolutePath()))
                .isInstanceOf(CommandFailedException.class)
                .satisfies(e -> {
                    assertThat(ctx.outAsString()).contains("IMPORT FAILED");
                    assertThat(ctx.errAsString()).doesNotContain(e.getClass().getName());
                    assertThat(e.getCause().getMessage())
                            .contains("Row has more columns than expected based on the header.");
                });
    }

    @Test
    void shouldWarnIfHeaderHasLessColumnsThanDataWhenToldTo() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        runImport(
                "--bad-tolerance",
                Integer.toString(nodeIds.size() * extraColumns),
                "--ignore-extra-columns",
                "--delimiter",
                "TAB",
                "--array-delimiter",
                String.valueOf(config.arrayDelimiter()),
                "--nodes=" + nodeHeader(config).toAbsolutePath() + ","
                        + nodeData(false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns)
                                .toAbsolutePath(),
                "--relationships",
                relationshipHeader(config).toAbsolutePath() + ","
                        + relationshipData(false, config, nodeIds, TRUE, true).toAbsolutePath());

        // THEN
        String badContents = Files.readString(reportFile(), Charset.defaultCharset());
        assertTrue(badContents.contains("Row has more columns than expected based on the header."));
    }

    @Test
    void shouldImportSplitInputFiles() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN
        runImport(
                "--nodes", // One group with one header file and one data file
                nodeHeader(config).toAbsolutePath() + ","
                        + nodeData(false, config, nodeIds, lines(0, NODE_COUNT / 2))
                                .toAbsolutePath(),
                "--nodes", // One group with two data files, where the header sits in the first file
                nodeData(true, config, nodeIds, lines(NODE_COUNT / 2, NODE_COUNT * 3 / 4))
                                .toAbsolutePath() + ","
                        + nodeData(false, config, nodeIds, lines(NODE_COUNT * 3 / 4, NODE_COUNT))
                                .toAbsolutePath(),
                "--relationships",
                relationshipHeader(config).toAbsolutePath() + ","
                        + relationshipData(false, config, nodeIds, TRUE, true).toAbsolutePath());

        // THEN
        verifyData();
    }

    @Test
    void shouldImportMultipleInputsWithAddedLabelsAndDefaultRelationshipType() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final String[] firstLabels = {"AddedOne", "AddedTwo"};
        final String[] secondLabels = {"AddedThree"};
        final String firstType = "TYPE_1";
        final String secondType = "TYPE_2";

        // WHEN
        runImport(
                "--nodes=" + join(":", firstLabels) + "="
                        + nodeData(true, config, nodeIds, lines(0, NODE_COUNT / 2))
                                .toAbsolutePath(),
                "--nodes=" + join(":", secondLabels) + "="
                        + nodeData(true, config, nodeIds, lines(NODE_COUNT / 2, NODE_COUNT))
                                .toAbsolutePath(),
                "--relationships=" + firstType + "="
                        + relationshipData(true, config, nodeIds, lines(0, RELATIONSHIP_COUNT / 2), false)
                                .toAbsolutePath(),
                "--relationships=" + secondType + "="
                        + relationshipData(
                                        true, config, nodeIds, lines(RELATIONSHIP_COUNT / 2, RELATIONSHIP_COUNT), false)
                                .toAbsolutePath());

        // THEN
        MutableInt numberOfNodesWithFirstSetOfLabels = new MutableInt();
        MutableInt numberOfNodesWithSecondSetOfLabels = new MutableInt();
        MutableInt numberOfRelationshipsWithFirstType = new MutableInt();
        MutableInt numberOfRelationshipsWithSecondType = new MutableInt();
        verifyData(
                node -> {
                    if (nodeHasLabels(node, firstLabels)) {
                        numberOfNodesWithFirstSetOfLabels.increment();
                    } else if (nodeHasLabels(node, secondLabels)) {
                        numberOfNodesWithSecondSetOfLabels.increment();
                    } else {
                        fail(node + " has neither set of labels, it has " + labelsOf(node));
                    }
                },
                relationship -> {
                    if (relationship.isType(RelationshipType.withName(firstType))) {
                        numberOfRelationshipsWithFirstType.increment();
                    } else if (relationship.isType(RelationshipType.withName(secondType))) {
                        numberOfRelationshipsWithSecondType.increment();
                    } else {
                        fail(relationship + " didn't have either type, it has "
                                + relationship.getType().name());
                    }
                });
        assertEquals(NODE_COUNT / 2, numberOfNodesWithFirstSetOfLabels.intValue());
        assertEquals(NODE_COUNT / 2, numberOfNodesWithSecondSetOfLabels.intValue());
        assertEquals(RELATIONSHIP_COUNT / 2, numberOfRelationshipsWithFirstType.intValue());
        assertEquals(RELATIONSHIP_COUNT / 2, numberOfRelationshipsWithSecondType.intValue());
    }

    private static String labelsOf(Node node) {
        return String.join(" ", Iterables.map(node.getLabels(), Label::name));
    }

    private static boolean nodeHasLabels(Node node, String[] labels) {
        for (String name : labels) {
            if (!node.hasLabel(label(name))) {
                return false;
            }
        }
        return true;
    }

    @Test
    void shouldImportOnlyNodes() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();

        // WHEN
        runImport(
                "--nodes",
                nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                        .toAbsolutePath()
                        .toString());
        // no relationships

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            int nodeCount = 0;
            for (Node node : allNodes) {
                assertTrue(node.hasProperty("name"));
                nodeCount++;
                assertFalse(node.hasRelationship());
            }
            assertEquals(NODE_COUNT, nodeCount);
            tx.commit();
        }
    }

    @Test
    void failOnInvalidDatabaseName() {
        List<String> nodeIds = nodeIds();

        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                                .toAbsolutePath()
                                .toString(),
                        "--", // force the parser to pick up the end of the nodes
                        "__incorrect_db__"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Invalid database name '__incorrect_db__'.");
    }

    @Test
    void importIntoLowerCasedDatabaseName() throws Exception {
        List<String> nodeIds = nodeIds();

        var mixedCaseDatabaseName = "TestDataBase";
        runImport(
                "--nodes",
                nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                        .toAbsolutePath()
                        .toString(),
                "--", // force the parser to pick up the end of the nodes
                mixedCaseDatabaseName);

        var db = getDatabaseApi(mixedCaseDatabaseName.toLowerCase(Locale.ROOT));
        assertEquals(mixedCaseDatabaseName.toLowerCase(Locale.ROOT), db.databaseName());

        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            int nodeCount = 0;
            for (Node node : allNodes) {
                assertTrue(node.hasProperty("name"));
                nodeCount++;
                assertFalse(node.hasRelationship());
            }
            assertEquals(NODE_COUNT, nodeCount);
            tx.commit();
        }
    }

    @Test
    void shouldImportGroupsOfOverlappingIds() throws Exception {
        // GIVEN
        List<String> groupOneNodeIds = asList("1", "2", "3");
        List<String> groupTwoNodeIds = asList("4", "5", "2");
        List<RelationshipDataLine> rels =
                asList(relationship("1", "4", "TYPE"), relationship("2", "5", "TYPE"), relationship("3", "2", "TYPE"));
        Configuration config = Configuration.COMMAS;
        String groupOne = "Actor";
        String groupTwo = "Movie";

        // WHEN
        runImport(
                "--nodes",
                nodeHeader(config, groupOne) + "," + nodeData(false, config, groupOneNodeIds, TRUE),
                "--nodes",
                nodeHeader(config, groupTwo) + "," + nodeData(false, config, groupTwoNodeIds, TRUE),
                "--relationships",
                relationshipHeader(config, groupOne, groupTwo, true) + ","
                        + relationshipData(false, config, rels.iterator(), TRUE, true));

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            int nodeCount = 0;
            for (Node node : allNodes) {
                assertTrue(node.hasProperty("name"));
                nodeCount++;
                assertEquals(1, Iterables.count(node.getRelationships()));
            }
            assertEquals(6, nodeCount);
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToMixSpecifiedAndUnspecifiedGroups() throws Exception {
        // GIVEN
        List<String> groupOneNodeIds = asList("1", "2", "3");
        List<String> groupTwoNodeIds = asList("4", "5", "2");
        Configuration config = Configuration.COMMAS;

        // WHEN
        runImport(
                "--nodes",
                nodeHeader(config, "MyGroup").toAbsolutePath() + ","
                        + nodeData(false, config, groupOneNodeIds, TRUE).toAbsolutePath(),
                "--nodes",
                nodeHeader(config).toAbsolutePath() + ","
                        + nodeData(false, config, groupTwoNodeIds, TRUE).toAbsolutePath());

        // THEN
        verifyData(6, 0, Validators.emptyValidator(), Validators.emptyValidator());
    }

    @Test
    void shouldImportWithoutTypeSpecifiedInRelationshipHeaderbutWithDefaultTypeInArgument() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        String type = randomType();

        // WHEN
        runImport(
                "--nodes",
                nodeData(true, config, nodeIds, TRUE).toAbsolutePath().toString(),
                // there will be no :TYPE specified in the header of the relationships below
                "--relationships",
                type + "="
                        + relationshipData(true, config, nodeIds, TRUE, false).toAbsolutePath());

        // THEN
        verifyData();
    }

    @Test
    void shouldIncludeSourceInformationInNodeIdCollisionError() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c", "d", "e", "f", "a", "g");
        Configuration config = Configuration.COMMAS;
        Path nodeHeaderFile = nodeHeader(config);
        Path nodeData1 = nodeData(false, config, nodeIds, lines(0, 4));
        Path nodeData2 = nodeData(false, config, nodeIds, lines(4, nodeIds.size()));

        // WHEN
        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeHeaderFile.toAbsolutePath() + "," + nodeData1.toAbsolutePath() + ","
                                + nodeData2.toAbsolutePath()))
                .rootCause()
                .isInstanceOf(DuplicateInputIdException.class)
                .hasMessageContaining("'a' is defined more than once");
    }

    @Test
    void shouldSkipDuplicateNodesIfToldTo() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c", "d", "e", "f", "a", "g");
        Configuration config = Configuration.COMMAS;
        Path nodeHeaderFile = nodeHeader(config);
        Path nodeData1 = nodeData(false, config, nodeIds, lines(0, 4));
        Path nodeData2 = nodeData(false, config, nodeIds, lines(4, nodeIds.size()));

        // WHEN
        runImport(
                "--skip-duplicate-nodes",
                "--nodes",
                nodeHeaderFile.toAbsolutePath() + "," + nodeData1.toAbsolutePath() + "," + nodeData2.toAbsolutePath());

        // THEN there should not be duplicates of any node
        GraphDatabaseService db = getDatabaseApi();
        Set<String> expectedNodeIds = new HashSet<>(nodeIds);
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            Set<String> foundNodesIds = new HashSet<>();
            for (Node node : allNodes) {
                String id = (String) node.getProperty("id");
                assertTrue(foundNodesIds.add(id), id + ", " + foundNodesIds);
                assertTrue(expectedNodeIds.contains(id));
            }
            assertEquals(expectedNodeIds, foundNodesIds);

            // also all nodes in the label index should exist
            for (int i = 0; i < MAX_LABEL_ID; i++) {
                Label label = label(labelName(i));
                try (ResourceIterator<Node> nodesByLabel = tx.findNodes(label)) {
                    while (nodesByLabel.hasNext()) {
                        Node node = nodesByLabel.next();
                        if (!node.hasLabel(label)) {
                            fail("Expected " + node + " to have label " + label.name() + ", but instead had "
                                    + asList(node.getLabels()));
                        }
                    }
                }
            }

            tx.commit();
        }
    }

    @Test
    void shouldLogRelationshipsReferringToMissingNode() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c");
        Configuration config = Configuration.COMMAS;
        Path nodeData = nodeData(true, config, nodeIds, TRUE);
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship("a", "b", "TYPE", "aa"), //          line 2 of file1
                relationship("c", "bogus", "TYPE", "bb"), //      line 3 of file1
                relationship("b", "c", "KNOWS", "cc"), //         line 1 of file2
                relationship("c", "a", "KNOWS", "dd"), //         line 2 of file2
                relationship("missing", "a", "KNOWS", "ee")); // line 3 of file2
        Path relationshipData1 = relationshipData(true, config, relationships.iterator(), lines(0, 2), true);
        Path relationshipData2 = relationshipData(false, config, relationships.iterator(), lines(2, 5), true);

        // WHEN importing data where some relationships refer to missing nodes
        runImport(
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--skip-bad-relationships",
                "--bad-tolerance",
                "2",
                "--relationships",
                relationshipData1.toAbsolutePath() + "," + relationshipData2.toAbsolutePath());

        // THEN
        String badContents = Files.readString(reportFile(), Charset.defaultCharset());
        assertTrue(badContents.contains("bogus"), "Didn't contain first bad relationship");
        assertTrue(badContents.contains("missing"), "Didn't contain second bad relationship");
        verifyRelationships(relationships);
    }

    @Test
    void skipLoggingOfBadEntries() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c");
        Configuration config = Configuration.COMMAS;
        Path nodeData = nodeData(true, config, nodeIds, TRUE);
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship("a", "b", "TYPE", "aa"), //          line 2 of file1
                relationship("c", "bogus", "TYPE", "bb"), //      line 3 of file1
                relationship("b", "c", "KNOWS", "cc"), //         line 1 of file2
                relationship("c", "a", "KNOWS", "dd"), //         line 2 of file2
                relationship("missing", "a", "KNOWS", "ee")); // line 3 of file2
        Path relationshipData1 = relationshipData(true, config, relationships.iterator(), lines(0, 2), true);
        Path relationshipData2 = relationshipData(false, config, relationships.iterator(), lines(2, 5), true);

        // WHEN importing data where some relationships refer to missing nodes
        runImport(
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--bad-tolerance",
                "2",
                "--skip-bad-entries-logging",
                "true",
                "--skip-bad-relationships",
                "true",
                "--relationships",
                relationshipData1.toAbsolutePath() + "," + relationshipData2.toAbsolutePath());

        assertThat(reportFile()).content().isEmpty();
        verifyRelationships(relationships);
    }

    @Test
    void shouldFailIfTooManyBadRelationships() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c");
        Configuration config = Configuration.COMMAS;
        Path nodeData = nodeData(true, config, nodeIds, TRUE);
        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship("a", "b", "TYPE"), //          line 2 of file1
                relationship("c", "bogus", "TYPE"), //      line 3 of file1
                relationship("b", "c", "KNOWS"), //         line 1 of file2
                relationship("c", "a", "KNOWS"), //         line 2 of file2
                relationship("missing", "a", "KNOWS")); // line 3 of file2
        Path relationshipData = relationshipData(true, config, relationships.iterator(), TRUE, true);

        // WHEN importing data where some relationships refer to missing nodes
        assertThatThrownBy(() -> runImport(
                        "--nodes", nodeData.toAbsolutePath().toString(),
                        "--bad-tolerance", "1",
                        "--relationships", relationshipData.toAbsolutePath().toString()))
                .rootCause()
                .isInstanceOf(InputException.class)
                .hasMessageContaining(relationshipData.toAbsolutePath().toString());
    }

    @Test
    void shouldBeAbleToDisableSkippingOfBadRelationships() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c");
        Configuration config = Configuration.COMMAS;
        Path nodeData = nodeData(true, config, nodeIds, TRUE);

        List<RelationshipDataLine> relationships = Arrays.asList(
                // header                                   line 1 of file1
                relationship("a", "b", "TYPE"), //          line 2 of file1
                relationship("c", "bogus", "TYPE")); //    line 3 of file1

        Path relationshipData1 = relationshipData(true, config, relationships.iterator(), lines(0, 2), true);
        Path relationshipData2 = relationshipData(false, config, relationships.iterator(), lines(2, 5), true);

        // WHEN importing data where some relationships refer to missing nodes
        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeData.toAbsolutePath().toString(),
                        "--skip-bad-relationships=false",
                        "--relationships",
                        relationshipData1.toAbsolutePath() + "," + relationshipData2.toAbsolutePath()))
                .rootCause()
                .isInstanceOf(InputException.class)
                .hasMessageContaining(relationshipData1.toAbsolutePath().toString());
    }

    @Test
    void shouldHandleAdditiveLabelsWithSpaces() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        final Label label1 = label("My First Label");
        final Label label2 = label("My Other Label");

        // WHEN
        runImport(
                "--nodes=My First Label:My Other Label="
                        + nodeData(true, config, nodeIds, TRUE).toAbsolutePath(),
                "--relationships",
                relationshipData(true, config, nodeIds, TRUE, true)
                        .toAbsolutePath()
                        .toString());

        // THEN
        verifyData(
                node -> {
                    assertTrue(node.hasLabel(label1));
                    assertTrue(node.hasLabel(label2));
                },
                Validators.emptyValidator());
    }

    @Test
    void shouldImportFromInputDataEncodedWithSpecificCharset() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;
        Charset charset = StandardCharsets.UTF_16;

        // WHEN
        runImport(
                "--input-encoding",
                charset.name(),
                "--nodes",
                nodeData(true, config, nodeIds, TRUE, charset).toAbsolutePath().toString(),
                "--relationships",
                relationshipData(true, config, nodeIds, TRUE, true, charset)
                        .toAbsolutePath()
                        .toString());

        // THEN
        verifyData();
    }

    @Test
    void shouldDisallowImportWithoutNodesInput() {
        // GIVEN
        List<String> nodeIds = nodeIds();

        // WHEN
        assertThatThrownBy(() -> runImport(
                        "--relationships",
                        relationshipData(true, Configuration.COMMAS, nodeIds, TRUE, true)
                                .toAbsolutePath()
                                .toString()))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("Missing required option: '--nodes");
    }

    @Test
    void shouldBeAbleToImportAnonymousNodes() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("1", "", "", "", "3", "", "", "", "", "", "5");
        List<RelationshipDataLine> relationshipData = List.of(relationship("1", "3", "KNOWS"));

        // WHEN
        runImport(
                "--nodes",
                nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                        .toAbsolutePath()
                        .toString(),
                "--relationships",
                relationshipData(true, Configuration.COMMAS, relationshipData.iterator(), TRUE, true)
                        .toAbsolutePath()
                        .toString());

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        int anonymousCount = 0;
        try (Transaction tx = db.beginTx()) {
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                for (final String id : nodeIds) {
                    if (id.isEmpty()) {
                        anonymousCount++;
                    } else {
                        assertNotNull(Iterators.single(Iterators.filter(nodeFilter(id), allNodes.iterator())));
                    }
                }
                assertEquals(anonymousCount, count(Iterators.filter(nodeFilter(""), allNodes.iterator())));
            }
            tx.commit();
        }
    }

    @Test
    void shouldDisallowMultilineFieldsByDefault() throws Exception {
        // GIVEN
        Path data = data(":ID,name", "1,\"This is a line with\nnewlines in\"");

        // WHEN
        assertThatThrownBy(() -> runImport("--nodes", data.toAbsolutePath().toString()))
                .rootCause()
                .isInstanceOf(IllegalMultilineFieldException.class)
                .hasMessageContaining("Multi-line");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldDisallowMultilineDocumentsWithV1Format(boolean useFormatOption) throws Exception {
        // GIVEN
        final var path = data(":ID,name", "1,\"This is a line with\nnewlines in\"")
                .toAbsolutePath()
                .toString();

        // WHEN
        final String[] args;
        if (useFormatOption) {
            args = new String[] {"--nodes", path, "--multiline-fields", path, "--multiline-fields-format", "v1"};
        } else {
            args = new String[] {"--nodes", path, "--multiline-fields", path};
        }

        assertThatThrownBy(() -> runImport(args))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Illegal format for --multiline-fields when using the v1 format - must be either true or"
                        + " false");
    }

    @Test
    void shouldDisallowMultilineDocumentsWhenNotMatching() throws Exception {
        // GIVEN
        final var path = data(":ID,name", "1,\"This is a line with\nnewlines in\"")
                .toAbsolutePath()
                .toString();

        // WHEN
        final var badMultiPath = "not going to match";
        assertThatThrownBy(() -> runImport(
                        "--nodes", path, "--multiline-fields", badMultiPath, "--multiline-fields-format", "v2"))
                .isInstanceOf(CommandFailedException.class)
                .hasCauseInstanceOf(NoSuchFileException.class)
                .hasMessageContainingAll("Unable to find the parent of the path", badMultiPath);
    }

    @Test
    void shouldNotTrimStringsByDefault() throws Exception {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        Path data = data(":ID,name", "1,\"" + name + "\"");

        // WHEN
        runImport("--nodes", data.toAbsolutePath().toString());

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Node node = Iterables.single(tx.getAllNodes());
            assertEquals(name, node.getProperty("name"));
            tx.commit();
        }
    }

    @Test
    void shouldTrimStringsIfConfiguredTo() throws Exception {
        // GIVEN
        String name = "  This is a line with leading and trailing whitespaces   ";
        Path data = data(":ID,name", "1,\"" + name + "\"", "2," + name);

        // WHEN
        runImport("--nodes", data.toAbsolutePath().toString(), "--trim-strings", "true");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            Set<String> names = new HashSet<>();
            for (final var node : allNodes) {
                names.add(node.getProperty("name").toString());
            }

            assertTrue(names.remove(name));
            assertTrue(names.remove(name.trim()));
            assertTrue(names.isEmpty());

            tx.commit();
        }
    }

    @Test
    void shouldCollectUnlimitedNumberOfBadEntries() throws Exception {
        // GIVEN
        List<String> nodeIds = Collections.nCopies(10_000, "A");

        // WHEN
        runImport(
                "--nodes=" + nodeData(true, Configuration.COMMAS, nodeIds, TRUE).toAbsolutePath(),
                "--skip-duplicate-nodes",
                "--bad-tolerance=-1");

        // THEN
        // all those duplicates should just be accepted using the - for specifying bad tolerance
    }

    @Test
    void shouldAllowMultilineFieldsWhenEnabled() throws Exception {
        // GIVEN
        Path data = data(":ID,name", "1,\"This is a line with\nnewlines in\"");

        // WHEN
        runImport("--nodes", data.toAbsolutePath().toString(), "--multiline-fields", "true");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Node node = Iterables.single(tx.getAllNodes());
            assertEquals("This is a line with\nnewlines in", node.getProperty("name"));
            tx.commit();
        }
    }

    @Test
    void shouldAllowMultilineDocumentsWhenMatching() throws Exception {
        // GIVEN
        final var nodePath = data(":ID,name", "1,\"This is a line with\nnewlines in\"")
                .toAbsolutePath()
                .toString();

        // WHEN
        runImport("--nodes", nodePath, "--multiline-fields", nodePath, "--multiline-fields-format", "v2");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Node node = Iterables.single(tx.getAllNodes());
            assertEquals("This is a line with\nnewlines in", node.getProperty("name"));
            tx.commit();
        }
    }

    @Test
    void shouldSkipEmptyFiles() throws Exception {
        // GIVEN
        Path data = data("");

        // WHEN
        runImport("--nodes", data.toAbsolutePath().toString());

        // THEN
        GraphDatabaseService graphDatabaseService = getDatabaseApi();
        try (Transaction tx = graphDatabaseService.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            assertThat(Iterables.asList(allNodes))
                    .as("Expected database to be empty")
                    .isEmpty();
            tx.commit();
        }
    }

    @Test
    void shouldIgnoreEmptyQuotedStringsIfConfiguredTo() throws Exception {
        // GIVEN
        Path data = data(":ID,one,two,three", "1,\"\",,value");

        // WHEN
        runImport("--nodes", data.toAbsolutePath().toString(), "--ignore-empty-strings", "true");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Node node = single(tx.getAllNodes());
            assertFalse(node.hasProperty("one"));
            assertFalse(node.hasProperty("two"));
            assertEquals("value", node.getProperty("three"));
            tx.commit();
        }
    }

    @Test
    void shouldPrintUserFriendlyMessageAboutUnsupportedMultilineFields() throws Exception {
        // GIVEN
        Path data = data(":ID,name", "1,\"one\ntwo\nthree\"", "2,four");

        var ctx = capturingCtx();
        assertThatThrownBy(
                        () -> runImport(ctx, "--nodes", data.toAbsolutePath().toString(), "--multiline-fields=false"))
                .isInstanceOf(CommandFailedException.class)
                // This happens at the end of the error handling process for all AbstractCommand calls (assuming
                // exception extends ConsoleFriendlyException).
                .satisfies(e -> ((CommandFailedException) e).prettyPrint(ctx.err()))
                .cause()
                .isInstanceOf(CsvImportException.class)
                .hasCauseInstanceOf(InputException.class);

        // THEN
        assertThat(ctx.errAsString()).contains("Detected field which spanned multiple lines");
        assertThat(ctx.errAsString()).contains("multiline-fields");
    }

    @Test
    void shouldAcceptRawAsciiCharacterCodeAsQuoteConfiguration() throws Exception {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        Path data = data(":ID,name", "1," + name1, "2," + name2);

        // WHEN
        runImport(
                "--nodes", data.toAbsolutePath().toString(),
                "--quote", String.valueOf(weirdDelimiter));

        // THEN
        Set<String> names = asSet("Weird", name2);
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                String name = (String) node.getProperty("name");
                assertTrue(names.remove(name), "Didn't expect node with name '" + name + "'");
            }
            assertTrue(names.isEmpty());
            tx.commit();
        }
    }

    @Test
    void shouldAcceptSpecialTabCharacterAsDelimiterConfiguration() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        runImport(
                "--delimiter",
                "\\t",
                "--array-delimiter",
                String.valueOf(config.arrayDelimiter()),
                "--nodes",
                nodeData(true, config, nodeIds, TRUE).toAbsolutePath().toString(),
                "--relationships",
                relationshipData(true, config, nodeIds, TRUE, true)
                        .toAbsolutePath()
                        .toString());

        // THEN
        verifyData();
    }

    @Test
    void shouldReportBadDelimiterConfiguration() {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.TABS;

        // WHEN
        assertThatThrownBy(() -> runImport(
                        "--delimiter",
                        "\\bogus",
                        "--array-delimiter",
                        String.valueOf(config.arrayDelimiter()),
                        "--nodes",
                        nodeData(true, config, nodeIds, TRUE).toAbsolutePath().toString(),
                        "--relationships",
                        relationshipData(true, config, nodeIds, TRUE, true)
                                .toAbsolutePath()
                                .toString()))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("bogus");
    }

    @Test
    void shouldFailAndReportStartingLineForUnbalancedQuoteInMiddle() {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeDataWithMissingQuote(2 * unbalancedStartLine, unbalancedStartLine)
                                .toAbsolutePath()
                                .toString()))
                .isInstanceOf(CommandFailedException.class)
                .hasCauseInstanceOf(CsvImportException.class)
                .cause()
                .hasCauseInstanceOf(InputException.class)
                .hasMessageContaining("Multi-line fields are illegal");
    }

    @Test
    void shouldAcceptRawEscapedAsciiCodeAsQuoteConfiguration() throws Exception {
        // GIVEN
        char weirdDelimiter = 1; // not '1', just the character represented with code 1, which seems to be SOH
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        Path data = data(":ID,name", "1," + name1, "2," + name2);

        // WHEN
        runImport("--nodes", data.toAbsolutePath().toString(), "--quote", "\\1");

        // THEN
        Set<String> names = asSet("Weird", name2);
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                String name = (String) node.getProperty("name");
                assertTrue(names.remove(name), "Didn't expect node with name '" + name + "'");
            }
            assertTrue(names.isEmpty());
            tx.commit();
        }
    }

    @Test
    void shouldFailAndReportStartingLineForUnbalancedQuoteAtEnd() {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeDataWithMissingQuote(unbalancedStartLine, unbalancedStartLine)
                                .toAbsolutePath()
                                .toString()))
                .isInstanceOf(CommandFailedException.class)
                .hasCauseInstanceOf(CsvImportException.class)
                .cause()
                .hasCauseInstanceOf(InputException.class)
                .hasMessageContaining("Multi-line fields");
    }

    @Test
    void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration1() throws Exception {
        // GIVEN
        char weirdDelimiter = 126; // 126 ~ (tilde)
        String weirdStringDelimiter = "\\126";
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        Path data = data(":ID,name", "1," + name1, "2," + name2);

        // WHEN given as raw ascii
        runImport("--nodes", data.toAbsolutePath().toString(), "--quote", weirdStringDelimiter);

        // THEN
        assertEquals('~', weirdDelimiter);

        Set<String> names = asSet("Weird", name2);
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                String name = (String) node.getProperty("name");
                assertTrue(names.remove(name), "Didn't expect node with name '" + name + "'");
            }
            assertTrue(names.isEmpty());
            tx.commit();
        }
    }

    @Test
    void shouldFailOnUnbalancedQuoteWithMultilinesEnabled() {
        // GIVEN
        int unbalancedStartLine = 10;

        // WHEN
        assertThatExceptionOfType(CommandFailedException.class)
                .isThrownBy(() -> runImport(
                        "--multiline-fields",
                        "true",
                        "--nodes",
                        nodeDataWithMissingQuote(2 * unbalancedStartLine, unbalancedStartLine)
                                .toAbsolutePath()
                                .toString()));
    }

    private Path nodeDataWithMissingQuote(int totalLines, int unbalancedStartLine) throws Exception {
        String[] lines = new String[totalLines + 1];

        lines[0] = "ID,:LABEL";

        for (int i = 1; i <= totalLines; i++) {
            StringBuilder line = new StringBuilder(format("%d,", i));
            if (i == unbalancedStartLine) {
                // Missing the end quote
                line.append("\"Secret Agent");
            } else {
                line.append("Agent");
            }
            lines[i] = line.toString();
        }

        return data(lines);
    }

    @Test
    void shouldBeEquivalentToUseRawAsciiOrCharacterAsQuoteConfiguration2() throws Exception {
        // GIVEN
        char weirdDelimiter = 126; // 126 ~ (tilde)
        String weirdStringDelimiter = "~";
        String name1 = weirdDelimiter + "Weird" + weirdDelimiter;
        String name2 = "Start " + weirdDelimiter + "middle thing" + weirdDelimiter + " end!";
        Path data = data(":ID,name", "1," + name1, "2," + name2);

        // WHEN given as string
        runImport("--nodes", data.toAbsolutePath().toString(), "--quote", weirdStringDelimiter);

        // THEN
        assertEquals(weirdStringDelimiter, "" + weirdDelimiter);
        assertEquals(weirdStringDelimiter.charAt(0), weirdDelimiter);

        Set<String> names = asSet("Weird", name2);
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                String name = (String) node.getProperty("name");
                assertTrue(names.remove(name), "Didn't expect node with name '" + name + "'");
            }
            assertTrue(names.isEmpty());
            tx.commit();
        }
    }

    @Test
    void useProvidedAdditionalConfig() throws Exception {
        // GIVEN
        final var arrayBlockSize = 10;
        final var stringBlockSize = 12;
        final var dbConfig = file("neo4j.properties");
        store(
                stringMap(
                        databases_root_path.name(),
                        layout.databasesDirectory().toAbsolutePath().toString(),
                        GraphDatabaseInternalSettings.array_block_size.name(),
                        String.valueOf(arrayBlockSize),
                        GraphDatabaseInternalSettings.string_block_size.name(),
                        String.valueOf(stringBlockSize),
                        transaction_logs_root_path.name(),
                        getTransactionLogsRoot()),
                dbConfig);
        final var nodeIds = nodeIds();

        // WHEN
        runImport(
                "--additional-config",
                dbConfig.toAbsolutePath().toString(),
                "--nodes",
                nodeData(true, Configuration.COMMAS, nodeIds, value -> true)
                        .toAbsolutePath()
                        .toString());

        // THEN
        final var db = assumeAlignedFormat(getDatabaseApi());

        //noinspection resource
        final var stores = db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
        final var headerSize = defaultFormat().dynamic().getRecordHeaderSize();
        assertEquals(
                arrayBlockSize + headerSize,
                stores.getPropertyStore().getArrayStore().getRecordSize());
        assertEquals(
                stringBlockSize + headerSize,
                stores.getPropertyStore().getStringStore().getRecordSize());
    }

    @Test
    void shouldDisableLegacyStyleQuotingIfToldTo() throws Exception {
        // GIVEN
        String nodeId = "me";
        String labelName = "Alive";
        List<String> lines = new ArrayList<>();
        lines.add(":ID,name,:LABEL");
        lines.add(nodeId + "," + "\"abc\"\"def\\\"\"ghi\"" + "," + labelName);

        // WHEN
        runImport(
                "--nodes",
                data(lines.toArray(new String[0])).toAbsolutePath().toString(),
                "--legacy-style-quoting",
                "false");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            assertNotNull(tx.findNode(label(labelName), "name", "abc\"def\\\"ghi"));
        }
    }

    @Test
    void shouldRespectBufferSizeSetting() {
        // GIVEN
        List<String> lines = new ArrayList<>();
        lines.add(":ID,name,:LABEL");
        lines.add("id," + "l".repeat(2_000) + ",Person");

        // WHEN
        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        data(lines.toArray(new String[0])).toAbsolutePath().toString(),
                        "--read-buffer-size",
                        "1k"))
                .isInstanceOf(CommandFailedException.class)
                .cause()
                .isInstanceOf(CsvImportException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessageContaining("input data");
    }

    @Test
    void shouldRespectMaxMemoryPercentageSetting() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds(10);

        // WHEN
        runImport(
                "--nodes",
                nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                        .toAbsolutePath()
                        .toString(),
                "--max-off-heap-memory",
                "60%");
    }

    @Test
    void shouldFailOnInvalidMaxMemoryPercentageSetting() {
        // GIVEN
        List<String> nodeIds = nodeIds(10);

        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                                .toAbsolutePath()
                                .toString(),
                        "--max-off-heap-memory",
                        "110%"))
                .isInstanceOf(ParameterException.class)
                .hasMessageContaining("Expected int value between 1 (inclusive) and 100 (exclusive), got 110.");
    }

    @Test
    void shouldRespectMaxMemorySuffixedSetting() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds(10);

        // WHEN
        runImport(
                "--nodes",
                nodeData(true, Configuration.COMMAS, nodeIds, TRUE)
                        .toAbsolutePath()
                        .toString(),
                "--max-off-heap-memory",
                "100M");
    }

    @Test
    void shouldTreatRelationshipWithMissingStartOrEndIdOrTypeAsBadRelationship() throws Exception {
        // GIVEN
        List<String> nodeIds = asList("a", "b", "c");
        Configuration config = Configuration.COMMAS;
        Path nodeData = nodeData(true, config, nodeIds, TRUE);

        List<RelationshipDataLine> relationships = Arrays.asList(
                relationship("a", null, "TYPE"), relationship(null, "b", "TYPE"), relationship("a", "b", null));

        Path relationshipData = relationshipData(true, config, relationships.iterator(), TRUE, true);

        // WHEN importing data where some relationships refer to missing nodes
        runImport(
                "--nodes", nodeData.toAbsolutePath().toString(),
                "--skip-bad-relationships", "true",
                "--relationships", relationshipData.toAbsolutePath().toString());

        String badContents = Files.readString(reportFile(), Charset.defaultCharset());
        // is missing data|to missing node
        assertEquals(3, occurrencesOf(badContents, "missing data"), badContents);
    }

    @Test
    void shouldKeepStoreFilesAfterFailedImport() throws Exception {
        // GIVEN
        List<String> nodeIds = nodeIds();
        Configuration config = Configuration.COMMAS;

        // WHEN data file contains more columns than header file
        int extraColumns = 3;
        var ctx = capturingCtx();
        assertThatExceptionOfType(CommandFailedException.class)
                .isThrownBy(() -> runImport(
                        ctx,
                        "--nodes",
                        nodeHeader(config).toAbsolutePath() + ","
                                + nodeData(false, config, nodeIds, TRUE, Charset.defaultCharset(), extraColumns)
                                        .toAbsolutePath()));
        // THEN the store files should be there
        var fs = testDirectory.getFileSystem();
        var databaseLayout = layout.databaseLayout(DEFAULT_DATABASE_NAME);
        List<Path> storeFiles = StorageEngineFactory.selectStorageEngine(fs, databaseLayout)
                .orElseThrow()
                .listStorageFiles(fs, databaseLayout);
        assertThat(storeFiles.size()).isGreaterThan(5);

        assertTrue(ctx.errAsString()
                .contains("Starting a database on these store files will likely fail or observe inconsistent records"));
    }

    @Test
    void shouldSupplyArgumentsAsFile() throws Exception {
        // given
        List<String> nodeIds = nodeIds();
        Configuration config = COMMAS;
        Path argumentFile = file("args");
        String nodesEscapedSpaces =
                nodeData(true, config, nodeIds, TRUE).toAbsolutePath().toString();
        String relationshipsEscapedSpaced = relationshipData(true, config, nodeIds, TRUE, true)
                .toAbsolutePath()
                .toString();
        Path dbConfig = prepareDefaultConfigFile();
        String arguments = format(
                "--additional-config=%s%n--nodes=%s%n--relationships=%s%n--report-file=%s%n",
                dbConfig.toAbsolutePath(), nodesEscapedSpaces, relationshipsEscapedSpaced, reportFile());
        Files.writeString(argumentFile, arguments);

        // when
        runImport(capturingCtx(), () -> new String[] {"@" + argumentFile.toAbsolutePath()});

        // then
        verifyData();
    }

    @Test
    void shouldCreateDebugLogInExpectedPlace() throws Exception {
        var ctx = capturingCtx();
        // given
        runImport(
                ctx,
                "--verbose",
                "--nodes",
                nodeData(true, COMMAS, nodeIds(), TRUE).toAbsolutePath().toString());

        var fileNamePattern =
                Pattern.compile(".*output will be saved in the directory: (?<path>.*)", Pattern.MULTILINE);
        var filenameMatcher = fileNamePattern.matcher(ctx.outAsString());
        assertTrue(filenameMatcher.find());

        var importContextDir = Path.of(filenameMatcher.group("path"));
        assertThat(testDirectory.getFileSystem().isDirectory(importContextDir)).isTrue();
        assertThat(importContextDir.getFileName()).asString().startsWith(DEFAULT_DATABASE_NAME + "-admin-import-");
        assertThat(importContextDir.getParent())
                .isEqualTo(Config.defaults(neo4j_home, testDirectory.homePath()).get(logs_directory));

        var internalLogFile = importContextDir.resolve(ImportContext.LOG_FILE_NAME);
        // THEN go and read the debug.log where it's expected to be and see if there's an IMPORT DONE line in it
        assertContains("debug", Files.readAllLines(internalLogFile), "Import completed successfully");
    }

    @Test
    void shouldNormalizeTypes() throws Exception {
        // GIVEN

        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID,prop1:short,prop2:float,prop3:char");
            writer.println("1,123,456.789,a");
            writer.println("2,1000000,24850457689578965796.458348570,b"); // <-- short too big, float too big
        });
        Path relationshipData = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,:TYPE,prop1:int,prop2:byte");
            writer.println("1,2,DC,123,12");
            writer.println("2,1,DC,9999999999,123456789");
        });
        var ctx = capturingCtx();
        runImport(
                ctx,
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--relationships",
                relationshipData.toAbsolutePath().toString());

        // THEN
        var out = ctx.outAsString();
        assertTrue(out.contains("IMPORT DONE"));
        assertTrue(out.contains(format(
                "Property type of 'prop1' normalized from 'short' --> 'long' in %s", nodeData.toAbsolutePath())));
        assertTrue(out.contains(format(
                "Property type of 'prop2' normalized from 'float' --> 'double' in %s", nodeData.toAbsolutePath())));
        assertTrue(out.contains(format(
                "Property type of 'prop3' normalized from 'char' --> 'String' in %s", nodeData.toAbsolutePath())));
        assertTrue(out.contains(format(
                "Property type of 'prop1' normalized from 'int' --> 'long' in %s", relationshipData.toAbsolutePath())));
        assertTrue(out.contains(format(
                "Property type of 'prop2' normalized from 'byte' --> 'long' in %s",
                relationshipData.toAbsolutePath())));
        // The properties should have been normalized, let's verify that
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Map<String, Node> nodes = new HashMap<>();
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                allNodes.forEach(node -> nodes.put(node.getProperty("id").toString(), node));
            }
            Node node1 = nodes.get("1");
            assertEquals(123L, node1.getProperty("prop1"));
            assertEquals(456.789D, node1.getProperty("prop2"));
            assertEquals("a", node1.getProperty("prop3"));
            Node node2 = nodes.get("2");
            assertEquals(1000000L, node2.getProperty("prop1"));
            assertEquals(24850457689578965796.458348570D, node2.getProperty("prop2"));
            assertEquals("b", node2.getProperty("prop3"));

            Relationship relationship1 = single(node1.getRelationships(Direction.OUTGOING));
            assertEquals(123L, relationship1.getProperty("prop1"));
            assertEquals(12L, relationship1.getProperty("prop2"));
            Relationship relationship2 = single(node1.getRelationships(Direction.INCOMING));
            assertEquals(9999999999L, relationship2.getProperty("prop1"));
            assertEquals(123456789L, relationship2.getProperty("prop2"));

            tx.commit();
        }
    }

    @Test
    void shouldNotImportVectorDataInRecordStorageEngine() throws Exception {
        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID,\"int8V:vector{coordinateType:byte,dimensions:3}\"");
            writer.println("1,123;-2;127");
            writer.println("2,987;33;0");
        });
        var ctx = capturingCtx();
        // THEN
        assertThatThrownBy(() -> runImport(
                        ctx,
                        "--format",
                        "aligned",
                        "--nodes",
                        nodeData.toAbsolutePath().toString()))
                .rootCause()
                .hasMessageContaining("storing properties of type vector is not supported in aligned store format");
    }

    @Test
    void shouldAllowReferringToCompositeNodeIDByConcatenatingStringsForStartIdAndEndId() throws Exception {
        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("firstname:ID(name){label:Person},lastname:ID(name){label:Person},state");
            writer.println("ABC,DEF,NY");
        });
        Path relationshipData = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID(name),:END_ID(name),:TYPE,state");
            writer.println("ABCDEF,ABCDEF,FOO,CA");
        });

        var ctx = capturingCtx();
        runImport(
                ctx,
                "--format",
                "aligned",
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--relationships",
                relationshipData.toAbsolutePath().toString());
        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Map<String, Node> nodes = new HashMap<>();
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                allNodes.forEach(node -> nodes.put(node.getProperty("firstname").toString(), node));
            }
            Node node1 = nodes.get("ABC");
            assertThat(node1.getProperty("lastname")).isEqualTo("DEF");
            assertThat(node1.getProperty("state")).isEqualTo("NY");

            Relationship relationship1 = single(node1.getRelationships(Direction.OUTGOING));
            assertThat(relationship1.getType()).isEqualTo(RelationshipType.withName("FOO"));
            assertThat(relationship1.getProperty("state")).isEqualTo("CA");

            tx.commit();
        }
    }

    @Test
    void shouldNotFindFalsePositiveDuplicateIDsWithMultipleIDColumns() throws Exception {
        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("firstname:ID(name){label:Person},lastname:ID(name){label:Person},state");
            writer.println("ABC,DEF,NY");
            writer.println("ABCD,EF,NZ");
        });
        var ctx = capturingCtx();
        runImport(
                ctx, "--format", "aligned", "--nodes", nodeData.toAbsolutePath().toString());
        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Map<String, Node> nodes = new HashMap<>();
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                allNodes.forEach(node -> nodes.put(node.getProperty("firstname").toString(), node));
            }
            Node node1 = nodes.get("ABC");
            assertThat(node1.getProperty("lastname")).isEqualTo("DEF");
            assertThat(node1.getProperty("state")).isEqualTo("NY");
            Node node2 = nodes.get("ABCD");
            assertThat(node2.getProperty("lastname")).isEqualTo("EF");
            assertThat(node2.getProperty("state")).isEqualTo("NZ");
            tx.commit();
        }
    }

    @Test
    void shouldNotFindFalsePositiveDuplicateIDsWithMultipleIDColumnsForRelationships() throws Exception {
        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("firstname:ID(name){label:Person},lastname:ID(name){label:Person},state");
            writer.println("ABC,DEF,NY");
            writer.println("ABCD,EF,NZ");
        });
        Path relationshipData = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID(name),:START_ID(name),:END_ID(name),:END_ID(name),:TYPE,state");
            writer.println("ABC,DEF,ABCD,EF,FOO,CA");
            writer.println("ABCD,EF,ABC,DEF,BAR,CL");
        });

        var ctx = capturingCtx();
        runImport(
                ctx,
                "--format",
                "aligned",
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--relationships",
                relationshipData.toAbsolutePath().toString());
        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Map<String, Node> nodes = new HashMap<>();
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                allNodes.forEach(node -> nodes.put(node.getProperty("firstname").toString(), node));
            }
            Node node1 = nodes.get("ABC");
            assertThat(node1.getProperty("lastname")).isEqualTo("DEF");
            assertThat(node1.getProperty("state")).isEqualTo("NY");
            Node node2 = nodes.get("ABCD");
            assertThat(node2.getProperty("lastname")).isEqualTo("EF");
            assertThat(node2.getProperty("state")).isEqualTo("NZ");

            Relationship relationship1 = single(node1.getRelationships(Direction.OUTGOING));
            assertThat(relationship1.getType()).isEqualTo(RelationshipType.withName("FOO"));
            assertThat(relationship1.getProperty("state")).isEqualTo("CA");

            Relationship relationship2 = single(node2.getRelationships(Direction.OUTGOING));
            assertThat(relationship2.getType()).isEqualTo(RelationshipType.withName("BAR"));
            assertThat(relationship2.getProperty("state")).isEqualTo("CL");

            tx.commit();
        }
    }

    @Test
    void shouldNotNormalizeArrayTypes() throws Exception {
        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID,prop1:short[],prop2:float[]");
            writer.println("1,123,456.789");
            writer.println("2,987,654.321");
        });
        Path relationshipData = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,:TYPE,prop1:int[],prop2:byte[]");
            writer.println("1,2,DC,123,12");
            writer.println("2,1,DC,987,98");
        });
        var ctx = capturingCtx();
        runImport(
                ctx,
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--relationships",
                relationshipData.toAbsolutePath().toString());

        // THEN
        var out = ctx.outAsString();
        assertTrue(out.contains("IMPORT DONE"));
        assertFalse(out.contains(format(
                "Property type of 'prop1' normalized from 'short[]' --> 'long[]' in %s", nodeData.toAbsolutePath())));
        assertFalse(out.contains(format(
                "Property type of 'prop2' normalized from 'float[]' --> 'double[]' in %s", nodeData.toAbsolutePath())));
        assertFalse(out.contains(format(
                "Property type of 'prop1' normalized from 'int[]' --> 'long[]' in %s",
                relationshipData.toAbsolutePath())));
        assertFalse(out.contains(format(
                "Property type of 'prop2' normalized from 'byte[]' --> 'long[]' in %s",
                relationshipData.toAbsolutePath())));
        // The properties should have not been normalized, let's verify that
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            Map<String, Node> nodes = new HashMap<>();
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                allNodes.forEach(node -> nodes.put(node.getProperty("id").toString(), node));
            }
            Node node1 = nodes.get("1");
            assertThat(node1.getProperty("prop1")).isEqualTo(new short[] {123});
            assertThat(node1.getProperty("prop2")).isEqualTo(new float[] {456.789F});
            Node node2 = nodes.get("2");
            assertThat(node2.getProperty("prop1")).isEqualTo(new short[] {987});
            assertThat(node2.getProperty("prop2")).isEqualTo(new float[] {654.321F});

            Relationship relationship1 = single(node1.getRelationships(Direction.OUTGOING));
            assertThat(relationship1.getProperty("prop1")).isEqualTo(new int[] {123});
            assertThat(relationship1.getProperty("prop2")).isEqualTo(new byte[] {12});
            Relationship relationship2 = single(node1.getRelationships(Direction.INCOMING));
            assertThat(relationship2.getProperty("prop1")).isEqualTo(new int[] {987});
            assertThat(relationship2.getProperty("prop2")).isEqualTo(new byte[] {98});

            tx.commit();
        }
    }

    @Test
    void shouldFailParsingOnTooLargeNumbersWithoutTypeNormalization() throws Exception {
        // WHEN
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID,prop1:short,prop2:float");
            writer.println("1,1000000,24850457689578965796.458348570"); // <-- short too big, float too big
        });
        Path relationshipData = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,:TYPE,prop1:int,prop2:byte");
            writer.println("1,1,DC,9999999999,123456789");
        });
        assertThatThrownBy(() -> runImport(
                        "--normalize-types", "false",
                        "--nodes", nodeData.toAbsolutePath().toString(),
                        "--relationships", relationshipData.toAbsolutePath().toString()))
                .isInstanceOf(CommandFailedException.class)
                .cause()
                .hasMessageContaining("1000000")
                .hasMessageContaining("too big");
    }

    @Test
    void shouldHandleDuplicatesWithLargeIDs() throws Exception {
        String id1 = "SKJDSKDJKSJKD-SDJKSJDKJKJ-IUISUDISUIJDKJSKDJKSD-SLKDJSKDJKSDJKSJDK-<DJJ<LJELJIL#$JILJSLRJKS";
        String id2 = "DSURKSJKCSJKJ-SDKJDJRKJKS-KJSKRJKXFJKSJKJCKJSRK-SJKSURUKSUKSSKJDKSK-JSKSSSKJDKJ#K$JKSJDK";
        Path nodeData = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID,prop1");
            writer.println(id1 + ",abc");
            writer.println(id2 + ",def");
            writer.println(id1 + ",ghi");
        });
        Path relationshipData = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,:TYPE,prop1:int,prop2:byte");
            writer.println(id1 + "," + id2 + ",DC,9999999999,123456789");
        });

        // WHEN
        runImport(
                "--nodes",
                nodeData.toAbsolutePath().toString(),
                "--relationships",
                relationshipData.toAbsolutePath().toString(),
                "--skip-duplicate-nodes");

        // THEN
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            try (Stream<Node> stream = tx.getAllNodes().stream()) {
                Set<String> nodes =
                        stream.map(n -> (String) n.getProperty("prop1")).collect(Collectors.toSet());
                assertThat(nodes).hasSize(2);
                assertThat(nodes).contains("def");
                assertThat(nodes.contains("abc") || nodes.contains("ghi")).isTrue();
            }
            assertThat(count(tx.getAllRelationships())).isEqualTo(1);
        }
    }

    @Test
    void shouldUseImportCommandConfigIfAvailable() throws Exception {
        Path nodesFile = testDirectory.file("something");
        Files.createFile(nodesFile);
        Path configDir = testDirectory.absolutePath().resolve(Config.DEFAULT_CONFIG_DIR_NAME);
        Files.createDirectories(configDir);
        Path importCommandConfig = configDir.resolve("neo4j-admin-database-import.conf");

        // Checking that the command is unhappy about an invalid value is enough to verify
        // that the command-specific config is being taken into account.
        Files.writeString(importCommandConfig, pagecache_memory.name() + "=some nonsense");

        assertThatThrownBy(() -> runImport("--nodes", nodesFile.toAbsolutePath().toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'some nonsense' is not a valid size");
    }

    @Test
    void shouldStoreIDValuesAsSpecifiedValueType() throws Exception {
        // GIVEN
        var nodeData1 = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne){id-type:long},name,:LABEL");
            writer.println("123,P1,Person");
            writer.println("456,P2,Person");
        });
        var nodeData2 = createAndWriteFile("games.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupTwo),name,:LABEL");
            writer.println("ABC,G1,Game");
            writer.println("DEF,G2,Game");
        });

        // WHEN
        runImport(
                "--nodes",
                nodeData1.toAbsolutePath().toString(),
                "--nodes",
                nodeData2.toAbsolutePath().toString(),
                "--id-type",
                "string");

        // THEN
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var expectedPersonIds = Set.of(123L, 456L);
                var actualPersonIds = new HashSet<Long>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(Long.class);
                    actualPersonIds.add((Long) id);
                }
                assertThat(actualPersonIds).isEqualTo(expectedPersonIds);
            }
            try (var games = tx.findNodes(label("Game"))) {
                var expectedGameIds = Set.of("ABC", "DEF");
                var actualGameIds = new HashSet<String>();
                while (games.hasNext()) {
                    var node = games.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualGameIds.add((String) id);
                }
                assertThat(actualGameIds).isEqualTo(expectedGameIds);
            }
        }
    }

    // Note: This was probably never intended to work, it just slipped through (only in record format - not block).
    // The --id-type global setting only supports string and integer.
    // Now it is in the product and difficult to remove without risking breaking someones workflow.
    // This test is just here to document this weird behavior.
    @Test
    void shouldAllowEsotericIDType() throws Exception {
        final var confDir = testDirectory.absolutePath().resolve("conf");
        final var config = Config.newBuilder()
                .fromFileNoThrow(confDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME))
                .build();
        final var dbFormat = config.get(GraphDatabaseSettings.db_format);
        assumeThat(dbFormat).isNotEqualTo("block");

        // GIVEN
        var nodeData1 = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne){id-type:date},name,:LABEL");
            writer.println("1980-01-01,P1,Person");
            writer.println("1984-01-01,P2,Person");
        });

        // WHEN
        runImport("--nodes", nodeData1.toAbsolutePath().toString());

        // THEN
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var expectedPersonIds = Set.of(LocalDate.parse("1980-01-01"), LocalDate.parse("1984-01-01"));
                var actualPersonIds = new HashSet<LocalDate>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(LocalDate.class);
                    actualPersonIds.add((LocalDate) id);
                }
                assertThat(actualPersonIds).isEqualTo(expectedPersonIds);
            }
        }
    }

    // We won't allow having vectors as IDs.
    @Test
    void shouldNotAllowVectorIDType() throws Exception {
        var nodeData = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne){id-type:vector},name,:LABEL");
            writer.println("1;2,P1,Person");
            writer.println("1;2,P2,Person");
        });

        assertThatThrownBy(() -> runImport("--nodes", nodeData.toAbsolutePath().toString()))
                .rootCause()
                .isInstanceOf(HeaderException.class)
                .hasMessageContaining("vector is not allowed as an id-type");
    }

    @Test
    void autoSkipSubsequentHeadersShouldWorkAcrossMultipleFiles() throws Exception {
        // GIVEN
        final var header = ":LABEL,node_id:ID,counter:int";
        var nodeData1 = createAndWriteFile("part0.csv", Charset.defaultCharset(), writer -> {
            writer.println(header);
            writer.println("A,1,2");
        });
        var nodeData2 = createAndWriteFile("part1.csv", Charset.defaultCharset(), writer -> {
            writer.println(header);
            writer.println("A,2,3");
        });

        final var expectedNodes = Maps.immutable.of("1", 2, "2", 3);

        // WHEN
        runImport(
                "--auto-skip-subsequent-headers",
                "true",
                "--normalize-types",
                "false",
                "--nodes",
                nodeData1.toAbsolutePath() + "," + nodeData2.toAbsolutePath());

        // THEN
        var actualNodes = Maps.mutable.empty();
        try (var tx = getDatabaseApi().beginTx()) {
            try (var nodes = tx.findNodes(label("A"))) {
                while (nodes.hasNext()) {
                    var node = nodes.next();
                    var counter = node.getProperty("counter");
                    assertThat(counter).isInstanceOf(Integer.class);
                    actualNodes.put(node.getProperty("node_id"), counter);
                }
            }
        }

        assertThat(actualNodes.toImmutable()).isEqualTo(expectedNodes);
    }

    @Test
    void autoSkipSubsequentHeadersShouldNotBeTrippedUpByWeirdLine() throws Exception {
        // GIVEN
        final var header = ":LABEL,node_id:ID,counter:int";
        var nodeData1 = createAndWriteFile("part0.csv", Charset.defaultCharset(), writer -> {
            writer.println(header);
            writer.println("A,1,2");
        });
        var nodeData2 = createAndWriteFile("part1.csv", Charset.defaultCharset(), writer -> {
            writer.println("a(b,3,4");
            writer.println("A,2,3");
        });

        // WHEN
        runImport(
                "--auto-skip-subsequent-headers",
                "true",
                "--normalize-types",
                "false",
                "--nodes",
                nodeData1.toAbsolutePath() + "," + nodeData2.toAbsolutePath());

        // THEN
        var actualNodes = Maps.mutable.empty();
        try (var tx = getDatabaseApi().beginTx()) {
            try (var nodes = tx.findNodes(label("A"))) {
                while (nodes.hasNext()) {
                    var node = nodes.next();
                    var counter = node.getProperty("counter");
                    assertThat(counter).isInstanceOf(Integer.class);
                    actualNodes.put(node.getProperty("node_id"), counter);
                }
            }
        }

        final var expectedNodes = Maps.immutable.of("1", 2, "2", 3);
        assertThat(actualNodes.toImmutable()).isEqualTo(expectedNodes);

        try (var tx = getDatabaseApi().beginTx()) {
            try (var nodes = tx.findNodes(label("a(b"))) {
                assertThat(nodes).hasNext();
                var node = nodes.next();
                assertThat(node.getProperty("node_id")).isEqualTo("3");
                assertThat(node.getProperty("counter")).isEqualTo(4);
                assertThat(nodes).isExhausted();
            }
        }
    }

    @Test
    void autoSkipSubsequentHeadersShouldWorkAcrossMultipleIndividuallyListedFiles() throws Exception {
        // GIVEN
        final var header = ":LABEL,node_id:ID,counter:int";
        var nodeData1 = createAndWriteFile("group1.csv", Charset.defaultCharset(), writer -> {
            writer.println(header);
            writer.println("A,1,3");
        });
        var nodeData2 = createAndWriteFile("group2.csv", Charset.defaultCharset(), writer -> {
            writer.println(header);
            writer.println("A,2,4");
        });

        final var expectedNodes = Maps.immutable.of("1", 3, "2", 4);

        // WHEN
        runImport(
                "--auto-skip-subsequent-headers",
                "true",
                "--normalize-types",
                "false",
                "--nodes",
                nodeData1.toAbsolutePath().toString(),
                "--nodes",
                nodeData2.toAbsolutePath().toString());

        // THEN
        var actualNodes = Maps.mutable.empty();
        try (var tx = getDatabaseApi().beginTx()) {
            try (var nodes = tx.findNodes(label("A"))) {
                while (nodes.hasNext()) {
                    var node = nodes.next();
                    var counter = node.getProperty("counter");
                    assertThat(counter).isInstanceOf(Integer.class);
                    actualNodes.put(node.getProperty("node_id"), counter);
                }
            }
        }

        assertThat(actualNodes.toImmutable()).isEqualTo(expectedNodes);
    }

    @Test
    void autoSkipSubsequentHeadersOnMultiLineData() throws Exception {
        // GIVEN
        var part1 = createAndWriteFile("part1.csv", Charset.defaultCharset(), writer -> {
            writer.println(":LABEL,node_id:ID,count:int");
            writer.println("A,1,1");
        });
        var part2 = createAndWriteFile("part2.csv", Charset.defaultCharset(), writer -> {
            writer.println(":LABEL,node_id:ID,count:int");
            writer.println("A,2,1");
            writer.println("A,3,1");
        });
        var part3 = createAndWriteFile("part3.csv", Charset.defaultCharset(), writer -> {
            writer.println("A,4,2");
            writer.println("A,5,2");
        });

        // WHEN
        runImport(
                "--multiline-fields=true",
                "--auto-skip-subsequent-headers=true",
                "--nodes=" + part1.toAbsolutePath() + "," + part2.toAbsolutePath() + "," + part3.toAbsolutePath());

        // THEN
        try (var tx = getDatabaseApi().beginTx();
                var nodes = tx.findNodes(label("A"))) {
            var actualNodes = new HashSet<String>();
            nodes.forEachRemaining(node -> actualNodes.add((String) node.getProperty("node_id")));
            assertThat(actualNodes).isEqualTo(Set.of("1", "2", "3", "4", "5"));
        }
    }

    private static Stream<Arguments> v1multilineFieldsFormatValidation() {
        return Stream.of(
                Arguments.of("true", List.of()),
                Arguments.of("false", List.of()),
                Arguments.of(null, List.of("Missing required argument", "--multiline-fields")),
                Arguments.of(
                        "nodes.csv",
                        List.of("Illegal format", "--multiline-fields", "v1", "must be either true or false")));
    }

    @ParameterizedTest
    @MethodSource
    void v1multilineFieldsFormatValidation(String fields, List<String> errors) throws Exception {
        // GIVEN
        var nodes = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
                    writer.println(":LABEL,node_id:ID,prop");
                    writer.println("A,1,stuff");
                })
                .toAbsolutePath()
                .toString();

        final var args = Lists.mutable.<String>empty();
        args.add("--nodes=" + nodes);
        args.add("--multiline-fields-format=v1");

        if (fields != null) {
            args.add("--multiline-fields=" + (fields.endsWith("nodes.csv") ? nodes : fields));
        }

        if (errors.isEmpty()) {
            // WHEN
            runImport(args.toArray(String[]::new));

            // THEN
            try (var tx = getDatabaseApi().beginTx()) {
                assertThat(Iterators.count(tx.findNodes(label("A")))).isOne();
            }
        } else {
            assertThatThrownBy(() -> runImport(args.toArray(String[]::new)))
                    .hasMessageContainingAll(errors.toArray(String[]::new));
        }
    }

    private static Stream<Arguments> v2multilineFieldsFormatValidation() {
        return Stream.of(
                Arguments.of("nodes.csv", List.of()),
                Arguments.of(null, List.of("Missing required argument", "--multiline-fields")),
                Arguments.of("/duff", List.of("File '/duff' doesn't exist")));
    }

    @ParameterizedTest
    @MethodSource
    void v2multilineFieldsFormatValidation(String fields, List<String> errors) throws Exception {
        // GIVEN
        var nodes = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
                    writer.println(":LABEL,node_id:ID,prop");
                    writer.println("A,1,\"stuff\"");
                })
                .toAbsolutePath()
                .toString();

        final var args = Lists.mutable.<String>empty();
        args.add("--nodes=" + nodes);
        args.add("--multiline-fields-format=v2");

        if (fields != null) {
            args.add("--multiline-fields=" + (fields.endsWith("nodes.csv") ? nodes : fields));
        }

        if (errors.isEmpty()) {
            // WHEN
            runImport(args.toArray(String[]::new));

            // THEN
            try (var tx = getDatabaseApi().beginTx()) {
                assertThat(Iterators.count(tx.findNodes(label("A")))).isOne();
            }
        } else {
            assertThatThrownBy(() -> runImport(args.toArray(String[]::new)))
                    .hasMessageContainingAll(errors.toArray(String[]::new));
        }
    }

    @Test
    void cloudStorageUrisShouldReportSchemeError() {
        var schemeBase = "s3://boom/";
        assertThatThrownBy(() -> runImport("--nodes=" + schemeBase + "time.csv"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining(
                        "Unable to resolve the path: ",
                        schemeBase,
                        "The scheme of the provided URI is not currently supported",
                        "currently only 's3', 'gs' and 'azb' schemes are supported.");
    }

    @Test
    void shouldDefaultToNoLabelTokenIfNeitherSpecifiedNorInArgument() throws Exception {
        // i.e. --nodes=/path1,/path2... AND the header doesn't specify any :LABEL

        // given
        var nodes = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println(":ID,p1:int");
            writer.println("A,10");
        });

        // when
        runImport("--nodes", nodes.toString());

        // then
        try (var dbms = dbmsService()) {
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            try (var tx = db.beginTx()) {
                try (var iterator = tx.getAllNodes().iterator()) {
                    var node = iterator.next();
                    assertThat(node.getProperty("p1")).isEqualTo(10L);
                    assertThat(node.getLabels().iterator()).isExhausted();
                }
            }
        }
    }

    @Test
    void shouldDefaultToNoRelationshipTypeTokenIfNeitherSpecifiedNorInArgument() throws Exception {
        // i.e. --relationships=/path1,/path2... AND the header doesn't specify any :TYPE

        // given
        var nodes = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println(":ID");
            writer.println("A");
            writer.println("B");
        });
        var relationships = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,p1:int");
            writer.println("A,B,10");
        });

        // when/then
        assertThatThrownBy(() -> runImport("--nodes", nodes.toString(), "--relationships", relationships.toString()))
                .hasRootCauseInstanceOf(MissingRelationshipDataException.class);
    }

    @Test
    void shouldImportWithCustomTempPath() throws Exception {
        // given
        var nodes = createAndWriteFile("nodes.csv", Charset.defaultCharset(), writer -> {
            writer.println(":ID");
            writer.println("A");
            writer.println("B");
        });
        var relationships = createAndWriteFile("relationships.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:TYPE,:END_ID");
            writer.println("A,KNOWS,B");
        });

        // when
        Path tempDirectory = testDirectory.directory("some-temp-dir");
        runImport(
                "--nodes",
                nodes.toString(),
                "--relationships",
                relationships.toString(),
                "--temp-path",
                tempDirectory.toAbsolutePath().toString());

        // then not sure how to verify that the temp directory was actually used?
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldBoomCorrectlyWhenOverridingIdType(boolean defaultIntegerIdType) throws Exception {
        // given
        var overriddenIdType = defaultIntegerIdType ? "string" : "int";
        var nodeData1 = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne){id-type:" + overriddenIdType + "},name,:LABEL");
            writer.println("1,P1,Person");
            writer.println("1,P2,Person");
        });

        // when
        assertThatThrownBy(() -> runImport(
                        "--nodes",
                        nodeData1.toAbsolutePath().toString(),
                        "--id-type",
                        defaultIntegerIdType ? IdType.INTEGER.name() : IdType.STRING.name()))
                // then
                .rootCause()
                .isInstanceOf(DuplicateInputIdException.class)
                .hasMessageContaining("'1' is defined more than once in group 'GroupOne'");
    }

    @Test
    void shouldAllowOverridingIntegerIdTypeWithStringIdTypeInHeader() throws Exception {
        // given a non-long-castable string value used as the ID, while the global --id-type is Integer
        var nodeData = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne){id-type:string},name,:LABEL");
            writer.println("alpha,P1,Person");
            writer.println("beta,P2,Person");
        });

        // when
        runImport("--nodes", nodeData.toAbsolutePath().toString(), "--id-type", IdType.INTEGER.name());

        // then the import succeeds and the ids are stored as strings
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var actualIds = new HashSet<String>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualIds.add((String) id);
                }
                assertThat(actualIds).containsExactlyInAnyOrder("alpha", "beta");
            }
        }
    }

    @Test
    void shouldMixGlobalIntegerIdTypeWithStringIdTypeOverrideAcrossGroups() throws Exception {
        // given --id-type=integer with two CSVs:
        // GroupOne uses the global integer id-type, GroupTwo overrides to id-type:string
        var nodeData1 = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne),name,:LABEL");
            writer.println("123,P1,Person");
            writer.println("456,P2,Person");
        });
        var nodeData2 = createAndWriteFile("games.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupTwo){id-type:string},name,:LABEL");
            writer.println("alpha,G1,Game");
            writer.println("beta,G2,Game");
        });

        // when
        runImport(
                "--nodes",
                nodeData1.toAbsolutePath().toString(),
                "--nodes",
                nodeData2.toAbsolutePath().toString(),
                "--id-type",
                IdType.INTEGER.name());

        // then the import succeeds and each group stores its IDs as the type configured for that group
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var actualPersonIds = new HashSet<Long>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(Long.class);
                    actualPersonIds.add((Long) id);
                }
                assertThat(actualPersonIds).containsExactlyInAnyOrder(123L, 456L);
            }
            try (var games = tx.findNodes(label("Game"))) {
                var actualGameIds = new HashSet<String>();
                while (games.hasNext()) {
                    var node = games.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualGameIds.add((String) id);
                }
                assertThat(actualGameIds).containsExactlyInAnyOrder("alpha", "beta");
            }
        }
    }

    @Test
    void shouldMixGlobalStringIdTypeWithIntIdTypeOverrideAcrossGroups() throws Exception {
        // given --id-type=string with two CSVs:
        // GroupOne uses the global string id-type, GroupTwo overrides to id-type:int
        var nodeData1 = createAndWriteFile("persons.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupOne),name,:LABEL");
            writer.println("alpha,P1,Person");
            writer.println("beta,P2,Person");
        });
        var nodeData2 = createAndWriteFile("games.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID(GroupTwo){id-type:int},name,:LABEL");
            writer.println("123,G1,Game");
            writer.println("456,G2,Game");
        });

        // when
        runImport(
                "--nodes",
                nodeData1.toAbsolutePath().toString(),
                "--nodes",
                nodeData2.toAbsolutePath().toString(),
                "--id-type",
                IdType.STRING.name());

        // then the import succeeds and each group stores its IDs as the type configured for that group
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var actualPersonIds = new HashSet<String>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualPersonIds.add((String) id);
                }
                assertThat(actualPersonIds).containsExactlyInAnyOrder("alpha", "beta");
            }
            try (var games = tx.findNodes(label("Game"))) {
                var actualGameIds = new HashSet<Integer>();
                while (games.hasNext()) {
                    var node = games.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(Integer.class);
                    actualGameIds.add((Integer) id);
                }
                assertThat(actualGameIds).containsExactlyInAnyOrder(123, 456);
            }
        }
    }

    @Test
    void shouldAllowOverridingIntegerIdTypeWithStringIdTypeInHeaderForParquet() throws Exception {
        // given a non-long-castable string value used as the ID, while the global --id-type is Integer
        var nodeTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("id:ID(GroupOne){id-type:string}"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(":LABEL"));
        var nodeData = createParquetFile(
                "persons.parquet",
                nodeTypes,
                List.of(new Object[] {"alpha", "P1", "Person"}, new Object[] {"beta", "P2", "Person"}));

        // when
        runImport("--input-type=parquet", "--id-type", IdType.INTEGER.name(), "--nodes", nodeData.toString());

        // then the import succeeds and the ids are stored as strings
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var actualIds = new HashSet<String>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualIds.add((String) id);
                }
                assertThat(actualIds).containsExactlyInAnyOrder("alpha", "beta");
            }
        }
    }

    @Test
    void shouldMixGlobalIntegerIdTypeWithStringIdTypeOverrideAcrossGroupsForParquet() throws Exception {
        // given --id-type=integer with two parquet files:
        // GroupOne uses the global integer id-type, GroupTwo overrides to id-type:string
        var personTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id:ID(GroupOne)"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(":LABEL"));
        var personData = createParquetFile(
                "persons.parquet",
                personTypes,
                List.of(new Object[] {123L, "P1", "Person"}, new Object[] {456L, "P2", "Person"}));
        var gameTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("id:ID(GroupTwo){id-type:string}"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(":LABEL"));
        var gameData = createParquetFile(
                "games.parquet",
                gameTypes,
                List.of(new Object[] {"alpha", "G1", "Game"}, new Object[] {"beta", "G2", "Game"}));

        // when
        runImport(
                "--input-type=parquet",
                "--id-type",
                IdType.INTEGER.name(),
                "--nodes",
                personData.toString(),
                "--nodes",
                gameData.toString());

        // then the import succeeds and each group stores its IDs as the type configured for that group
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var actualPersonIds = new HashSet<Long>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(Long.class);
                    actualPersonIds.add((Long) id);
                }
                assertThat(actualPersonIds).containsExactlyInAnyOrder(123L, 456L);
            }
            try (var games = tx.findNodes(label("Game"))) {
                var actualGameIds = new HashSet<String>();
                while (games.hasNext()) {
                    var node = games.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualGameIds.add((String) id);
                }
                assertThat(actualGameIds).containsExactlyInAnyOrder("alpha", "beta");
            }
        }
    }

    @Test
    void shouldMixGlobalStringIdTypeWithIntIdTypeOverrideAcrossGroupsForParquet() throws Exception {
        // given --id-type=string with two parquet files:
        // GroupOne uses the global string id-type, GroupTwo overrides to id-type:int
        var personTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("id:ID(GroupOne)"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(":LABEL"));
        var personData = createParquetFile(
                "persons.parquet",
                personTypes,
                List.of(new Object[] {"alpha", "P1", "Person"}, new Object[] {"beta", "P2", "Person"}));
        var gameTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id:ID(GroupTwo){id-type:int}"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(":LABEL"));
        var gameData = createParquetFile(
                "games.parquet", gameTypes, List.of(new Object[] {123, "G1", "Game"}, new Object[] {456, "G2", "Game"
                }));

        // when
        runImport(
                "--input-type=parquet",
                "--id-type",
                IdType.STRING.name(),
                "--nodes",
                personData.toString(),
                "--nodes",
                gameData.toString());

        // then the import succeeds and each group stores its IDs as the type configured for that group
        var db = getDatabaseApi();
        try (var tx = db.beginTx()) {
            try (var persons = tx.findNodes(label("Person"))) {
                var actualPersonIds = new HashSet<String>();
                while (persons.hasNext()) {
                    var node = persons.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(String.class);
                    actualPersonIds.add((String) id);
                }
                assertThat(actualPersonIds).containsExactlyInAnyOrder("alpha", "beta");
            }
            try (var games = tx.findNodes(label("Game"))) {
                var actualGameIds = new HashSet<Integer>();
                while (games.hasNext()) {
                    var node = games.next();
                    var id = node.getProperty("id");
                    assertThat(id).isInstanceOf(Integer.class);
                    actualGameIds.add((Integer) id);
                }
                assertThat(actualGameIds).containsExactlyInAnyOrder(123, 456);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleParquetInput(boolean explicitlySetParquetFormat) throws Exception {
        // given
        List<org.apache.parquet.schema.Type> types = List.of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":ID"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("name"));
        var nodes = createParquetFile(
                "nodes.parquet", types, List.of(new Object[] {1L, "Tom"}, new Object[] {2L, "Jerry"}));

        // when
        List<String> args = Lists.mutable.of("--nodes", nodes.toString());
        if (explicitlySetParquetFormat) {
            args.add("--input-type=parquet");
        }
        runImport(args.toArray(new String[0]));

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            Set<String> namedNodes = new HashSet<>();
            tx.getAllNodes().forEach(node -> {
                boolean added = namedNodes.add(node.getProperty("name").toString());
                assertThat(added).isTrue();
            });
            assertThat(namedNodes).containsExactlyInAnyOrder("Tom", "Jerry");
        }
    }

    @Test
    void shouldHandleParquetInputWithCsvHeader() throws Exception {
        // given;
        var types = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ix"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("character"));
        var header = createAndWriteFile("header.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID,name");
            writer.println("ix,character");
        });

        var parquet = createParquetFile(
                "nodes.parquet", types, List.of(new Object[] {1L, "Tom"}, new Object[] {2L, "Jerry"}));

        // when
        var nodesFiles = "--nodes=" + header.toString() + "," + parquet.toString();
        runImport("--input-type=parquet", nodesFiles);

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            var namedNodes = new HashSet<String>();
            tx.getAllNodes()
                    .forEach(node -> assertThat(
                                    namedNodes.add(node.getProperty("name").toString()))
                            .isTrue());
            assertThat(namedNodes).containsExactlyInAnyOrder("Tom", "Jerry");
        }
    }

    @Test
    void shouldHandleParquetInputWithCsvHeaderAndCustomDelimiter() throws Exception {
        // given;
        var types = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("ix"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("character"));
        var header = createAndWriteFile("header.csv", Charset.defaultCharset(), writer -> {
            writer.println("id:ID|name");
            writer.println("ix|character");
        });

        var parquet = createParquetFile(
                "nodes.parquet", types, List.of(new Object[] {1L, "Tom"}, new Object[] {2L, "Jerry"}));

        // when
        var nodesFiles = "--nodes=" + header.toString() + "," + parquet.toString();
        runImport("--delimiter", "|", "--input-type=parquet", nodesFiles);

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            var namedNodes = new HashSet<String>();
            tx.getAllNodes()
                    .forEach(node -> assertThat(
                                    namedNodes.add(node.getProperty("name").toString()))
                            .isTrue());
            assertThat(namedNodes).containsExactlyInAnyOrder("Tom", "Jerry");
        }
    }

    @Test
    void shouldHandleParquetInputWithLists() throws Exception {
        // given a parquet file with the schema {id:int32, name: varchar, aList: varcharp[], label:varchar}
        var nodes = getClass().getResource("/org/neo4j/importer/parquet/listColumns.parquet");

        // when
        List<String> args = Lists.mutable.of("--nodes", nodes.toString());
        args.add("--input-type=parquet");

        runImport(args.toArray(new String[0]));

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            List<String[]> allArrays = new ArrayList<>();
            tx.getAllNodes().forEach(node -> {
                // Verify that strArray property exists and is a String array
                Object property = node.getProperty("aList");
                assertThat(property).isInstanceOf(String[].class);

                String[] strArray = (String[]) property;
                assertThat(strArray).hasSize(3);
                allArrays.add(strArray);
            });

            // Verify we have 2 nodes
            assertThat(allArrays).hasSize(2);

            // Verify one array contains ["1", "2", "3"] and the other contains ["4", "5", "6"]
            assertThat(allArrays)
                    .anySatisfy(arr -> assertThat(arr).containsExactlyInAnyOrder("a", "b", "c"))
                    .anySatisfy(arr -> assertThat(arr).containsExactlyInAnyOrder("d", "e", "f"));
        }
    }

    @Test
    void shouldHandleParquetInputWithVariousListTypes() throws Exception {
        // given a parquet file with various list types and an associated header file explictly setting the property
        // type
        var nodes = getClass().getResource("/org/neo4j/importer/parquet/list_types.parquet");
        var header = getClass().getResource("/org/neo4j/importer/parquet/list_types_header.csv");

        // when
        List<String> args = Lists.mutable.of("--nodes", "ParquetList=" + header.toString() + "," + nodes.toString());
        args.add("--input-type=parquet");

        runImport(args.toArray(new String[0]));

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            var importedNode = tx.getAllNodes().stream().findFirst().get();

            assertThat(importedNode.getPropertyKeys()).hasSize(7);

            // Verify c_list_string property
            var c_list_string = importedNode.getProperty("c_list_string");
            assertThat(c_list_string).isInstanceOf(String[].class);
            String[] strArray = (String[]) c_list_string;
            assertThat(strArray).hasSize(3);
            assertThat(strArray).containsExactly("a", "b", "c");

            // Verify c_list_int32 property
            var c_list_int32 = importedNode.getProperty("c_list_int32");
            assertThat(c_list_int32).isInstanceOf(long[].class);
            long[] int32Array = (long[]) c_list_int32;
            assertThat(int32Array).hasSize(3);
            assertThat(int32Array).containsExactly(123L, 234L, 345L);

            // Verify c_list_int64 property
            var c_list_int64 = importedNode.getProperty("c_list_int64");
            assertThat(c_list_int64).isInstanceOf(long[].class);
            long[] int64Array = (long[]) c_list_int64;
            assertThat(int64Array).hasSize(3);
            assertThat(int64Array).containsExactly(123L, 234L, 345L);

            // Verify c_list_float property
            var c_list_float = importedNode.getProperty("c_list_float");
            assertThat(c_list_float).isInstanceOf(float[].class);
            float[] floatArray = (float[]) c_list_float;
            assertThat(floatArray).hasSize(3);
            assertThat(floatArray).containsExactly(1.01f, 2.21f, 3.23f);

            // Verify c_list_double property
            var c_list_double = importedNode.getProperty("c_list_double");
            assertThat(c_list_double).isInstanceOf(double[].class);
            double[] doubleArray = (double[]) c_list_double;
            assertThat(doubleArray).hasSize(3);
            assertThat(doubleArray).containsExactly(1.01, 2.21, 3.23);

            // Verify c_list_boolean property
            var c_list_boolean = importedNode.getProperty("c_list_boolean");
            assertThat(c_list_boolean).isInstanceOf(boolean[].class);
            boolean[] booleanArray = (boolean[]) c_list_boolean;
            assertThat(booleanArray).hasSize(3);
            assertThat(booleanArray).containsExactly(true, false, true);
        }
    }

    @Test
    void shouldHandleParquetInputWithMultipleNodeGroupsEachWithOwnHeader() throws Exception {
        // given - two node groups with different parquet schemas, each with their own header file
        var personTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("person_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("person_name"));
        var personParquet = createParquetFile(
                "persons.parquet", personTypes, List.of(new Object[] {1, "Alice"}, new Object[] {2, "Bob"}));
        var personHeader = createAndWriteFile("persons-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,name");
            writer.println("person_id,person_name");
        });

        var companyTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("company_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("company_name"));
        var companyParquet = createParquetFile(
                "companies.parquet", companyTypes, List.of(new Object[] {"c1", "Acme"}, new Object[] {"c2", "BigCorp"
                }));
        var companyHeader = createAndWriteFile("companies-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,name");
            writer.println("company_id,company_name");
        });

        // when - two separate --nodes groups each with their own header, no explicit label prefix
        runImport(
                "--input-type=parquet",
                "--nodes=" + personHeader.toAbsolutePath() + "," + personParquet.toAbsolutePath(),
                "--nodes=" + companyHeader.toAbsolutePath() + "," + companyParquet.toAbsolutePath());

        // then - all 4 nodes should be imported using each group's own header
        try (var tx = getDatabaseApi().beginTx()) {
            var allNames = new HashSet<String>();
            tx.getAllNodes()
                    .forEach(node -> allNames.add(node.getProperty("name").toString()));
            assertThat(allNames).containsExactlyInAnyOrder("Alice", "Bob", "Acme", "BigCorp");
        }
    }

    @Test
    void shouldHandleParquetInputWithMultipleNodeGroupsEachWithOwnHeaderAndExplicitLabels() throws Exception {
        // given - two node groups with different parquet schemas, each with their own header file
        var personTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("person_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("person_name"));
        var personParquet = createParquetFile(
                "persons.parquet", personTypes, List.of(new Object[] {1, "Alice"}, new Object[] {2, "Bob"}));
        var personHeader = createAndWriteFile("persons-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,name");
            writer.println("person_id,person_name");
        });

        var companyTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("company_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("company_name"));
        var companyParquet = createParquetFile(
                "companies.parquet", companyTypes, List.of(new Object[] {"c1", "Acme"}, new Object[] {"c2", "BigCorp"
                }));
        var companyHeader = createAndWriteFile("companies-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,name");
            writer.println("company_id,company_name");
        });

        // when - two separate --nodes groups each with their own header, with explicit label prefix
        runImport(
                "--input-type=parquet",
                "--nodes=Person=" + personHeader.toAbsolutePath() + "," + personParquet.toAbsolutePath(),
                "--nodes=Company=" + companyHeader.toAbsolutePath() + "," + companyParquet.toAbsolutePath());

        // then - all 4 nodes should be imported using each group's own header
        try (var tx = getDatabaseApi().beginTx()) {
            var allNames = new HashSet<String>();
            tx.getAllNodes()
                    .forEach(node -> allNames.add("%s:%s"
                            .formatted(labelsOf(node), node.getProperty("name").toString())));
            assertThat(allNames)
                    .containsExactlyInAnyOrder("Person:Alice", "Person:Bob", "Company:Acme", "Company:BigCorp");
        }
    }

    @Test
    void shouldHandleParquetInputWithMultipleNodeGroupsEachWithOwnHeaderAndLabelsFromInput() throws Exception {
        // given - two node groups with different parquet schemas, each with their own header file
        var personTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("person_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("person_name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("label"));
        var personParquet = createParquetFile(
                "persons.parquet",
                personTypes,
                List.of(new Object[] {1, "Alice", "Person"}, new Object[] {2, "Bob", "Person"}));
        var personHeader = createAndWriteFile("persons-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,name,:LABEL");
            writer.println("person_id,person_name,label");
        });

        var companyTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("company_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("company_name"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("label"));
        var companyParquet = createParquetFile(
                "companies.parquet",
                companyTypes,
                List.of(new Object[] {"c1", "Acme", "Company"}, new Object[] {"c2", "BigCorp", "Company"}));
        var companyHeader = createAndWriteFile("companies-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,name,:LABEL");
            writer.println("company_id,company_name,label");
        });

        // when - two separate --nodes groups each with their own header, no explicit label prefix but labels from data
        runImport(
                "--input-type=parquet",
                "--nodes=" + personHeader.toAbsolutePath() + "," + personParquet.toAbsolutePath(),
                "--nodes=" + companyHeader.toAbsolutePath() + "," + companyParquet.toAbsolutePath());

        // then - all 4 nodes should be imported using each group's own header
        try (var tx = getDatabaseApi().beginTx()) {
            var allNames = new HashSet<String>();
            tx.getAllNodes()
                    .forEach(node -> allNames.add("%s:%s"
                            .formatted(labelsOf(node), node.getProperty("name").toString())));
            assertThat(allNames)
                    .containsExactlyInAnyOrder("Person:Alice", "Person:Bob", "Company:Acme", "Company:BigCorp");
        }
    }

    @Test
    void shouldHandleParquetInputWithMultipleRelationshipGroupsEachWithOwnHeader() throws Exception {
        // given - nodes with string IDs
        var nodeTypes = List.<org.apache.parquet.schema.Type>of(Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("nid"));
        var nodeParquet = createParquetFile(
                "nodes.parquet", nodeTypes, List.of(new Object[] {"a"}, new Object[] {"b"}, new Object[] {"c"}));
        var nodeHeader = createAndWriteFile("nodes-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID");
            writer.println("nid");
        });

        var relTypes0 = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("src"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("tgt"));
        var relParquet0 = createParquetFile("knows.parquet", relTypes0, List.<Object[]>of(new Object[] {"a", "b"}));
        var relHeader0 = createAndWriteFile("knows-header.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID");
            writer.println("src,tgt");
        });

        var relTypes1 = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("from_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("to_id"));
        var relParquet1 = createParquetFile("likes.parquet", relTypes1, List.<Object[]>of(new Object[] {"b", "c"}));
        var relHeader1 = createAndWriteFile("likes-header.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID");
            writer.println("from_id,to_id");
        });

        // when
        runImport(
                "--input-type=parquet",
                "--id-type=string",
                "--nodes=" + nodeHeader.toAbsolutePath() + "," + nodeParquet.toAbsolutePath(),
                "--relationships=KNOWS=" + relHeader0.toAbsolutePath() + "," + relParquet0.toAbsolutePath(),
                "--relationships=LIKES=" + relHeader1.toAbsolutePath() + "," + relParquet1.toAbsolutePath());

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            assertThat(count(tx.findRelationships(withName("KNOWS")))).isEqualTo(1);
            assertThat(count(tx.findRelationships(withName("LIKES")))).isEqualTo(1);
        }
    }

    @Test
    void shouldHandleParquetInputWithMultipleRelationshipGroupsEachWithOwnHeaderAndLabelsFromInput() throws Exception {
        // given - nodes with string IDs
        var nodeTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("nid"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("label"));
        var nodeParquet = createParquetFile(
                "nodes.parquet",
                nodeTypes,
                List.of(new Object[] {"a", "LabelA"}, new Object[] {"b", "LabelB"}, new Object[] {"c", "LabelC"}));
        var nodeHeader = createAndWriteFile("nodes-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("nodeId:ID,:LABEL");
            writer.println("nid,label");
        });

        var relTypes0 = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("src"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("tgt"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("type"));
        var relParquet0 =
                createParquetFile("knows.parquet", relTypes0, List.<Object[]>of(new Object[] {"a", "b", "KNOWS"}));
        var relHeader0 = createAndWriteFile("knows-header.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,:TYPE");
            writer.println("src,tgt,type");
        });

        var relTypes1 = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("from_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("to_id"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("type"));
        var relParquet1 =
                createParquetFile("likes.parquet", relTypes1, List.<Object[]>of(new Object[] {"b", "c", "LIKES"}));
        var relHeader1 = createAndWriteFile("likes-header.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID,:END_ID,:TYPE");
            writer.println("from_id,to_id,type");
        });

        // when
        runImport(
                "--input-type=parquet",
                "--id-type=string",
                "--nodes=" + nodeHeader.toAbsolutePath() + "," + nodeParquet.toAbsolutePath(),
                "--relationships=" + relHeader0.toAbsolutePath() + "," + relParquet0.toAbsolutePath(),
                "--relationships=" + relHeader1.toAbsolutePath() + "," + relParquet1.toAbsolutePath());

        // then
        try (var tx = getDatabaseApi().beginTx()) {
            assertThat(count(tx.findNodes(label("LabelA")))).isEqualTo(1);
            assertThat(count(tx.findNodes(label("LabelB")))).isEqualTo(1);
            assertThat(count(tx.findNodes(label("LabelC")))).isEqualTo(1);
            assertThat(count(tx.findRelationships(withName("KNOWS")))).isEqualTo(1);
            assertThat(count(tx.findRelationships(withName("LIKES")))).isEqualTo(1);
        }
    }

    @Test
    void shouldHandleParquetInputWithCompositeKeyRelationships() throws Exception {
        // given
        // Person nodes: composite ID from firstName + lastName in Person group
        var personTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("firstName"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("lastName"));
        var personParquet = createParquetFile(
                "persons.parquet",
                personTypes,
                List.of(new Object[] {"John", "Smith"}, new Object[] {"Jane", "Doe"}, new Object[] {"Bob", "Smith"}));
        var personHeader = createAndWriteFile("persons-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("firstName:ID(Person){id-type:string},lastName:ID(Person){id-type:string}");
            writer.println("firstName,lastName");
        });

        var cityTypes = List.<org.apache.parquet.schema.Type>of(Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name"));
        var cityParquet = createParquetFile(
                "cities.parquet", cityTypes, List.of(new Object[] {"London"}, new Object[] {"Paris"}));
        var cityHeader = createAndWriteFile("cities-header.csv", Charset.defaultCharset(), writer -> {
            writer.println("name:ID(City)");
            writer.println("name");
        });

        // LIVES_IN relationships: two :START_ID(Person) columns form the composite key group,
        // one :END_ID(City) column for the target city
        var relTypes = List.<org.apache.parquet.schema.Type>of(
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("firstName"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("lastName"),
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("city"));
        var relParquet = createParquetFile(
                "lives-in.parquet",
                relTypes,
                List.of(new Object[] {"John", "Smith", "London"}, new Object[] {"Jane", "Doe", "Paris"}, new Object[] {
                    "Bob", "Smith", "London"
                }));
        var relHeader = createAndWriteFile("lives-in-header.csv", Charset.defaultCharset(), writer -> {
            writer.println(":START_ID(Person),:START_ID(Person),:END_ID(City)");
            writer.println("firstName,lastName,city");
        });

        // when
        runImport(
                "--input-type=parquet",
                "--id-type=string",
                "--nodes=Person=" + personHeader.toAbsolutePath() + "," + personParquet.toAbsolutePath(),
                "--nodes=City=" + cityHeader.toAbsolutePath() + "," + cityParquet.toAbsolutePath(),
                "--relationships=LIVES_IN=" + relHeader.toAbsolutePath() + "," + relParquet.toAbsolutePath());

        // then
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx()) {
            // 3 Person nodes, each with firstName and lastName stored as properties
            Map<String, Node> persons = new HashMap<>();
            try (var it = tx.findNodes(label("Person"))) {
                it.forEachRemaining(n -> persons.put(n.getProperty("firstName") + " " + n.getProperty("lastName"), n));
            }
            assertThat(persons).containsOnlyKeys("John Smith", "Jane Doe", "Bob Smith");

            // 2 City nodes, each with name stored as property (name:ID(City))
            Map<String, Node> cities = new HashMap<>();
            try (var it = tx.findNodes(label("City"))) {
                it.forEachRemaining(n -> cities.put((String) n.getProperty("name"), n));
            }
            assertThat(cities).containsOnlyKeys("London", "Paris");

            // Each person has exactly one outgoing LIVES_IN relationship to the correct city
            Relationship johnRel =
                    single(persons.get("John Smith").getRelationships(Direction.OUTGOING, withName("LIVES_IN")));
            assertThat(johnRel.getEndNode().getProperty("name")).isEqualTo("London");

            Relationship janeRel =
                    single(persons.get("Jane Doe").getRelationships(Direction.OUTGOING, withName("LIVES_IN")));
            assertThat(janeRel.getEndNode().getProperty("name")).isEqualTo("Paris");

            Relationship bobRel =
                    single(persons.get("Bob Smith").getRelationships(Direction.OUTGOING, withName("LIVES_IN")));
            assertThat(bobRel.getEndNode().getProperty("name")).isEqualTo("London");
        }
    }

    private Path createParquetFile(String name, List<org.apache.parquet.schema.Type> types, List<Object[]> data)
            throws Exception {
        Path path = testDirectory.file(name);
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

    private static void assertContains(String linesType, List<String> lines, String string) {
        for (String line : lines) {
            if (line.contains(string)) {
                return;
            }
        }
        fail("Expected " + linesType + " lines " + join(lineSeparator(), lines.toArray(new String[0]))
                + " to have at least one line containing the string '" + string + "'");
    }

    @SuppressWarnings("SameParameterValue")
    private static int occurrencesOf(String text, String lookFor) {
        int index = -1;
        int count = -1;
        do {
            count++;
            index = text.indexOf(lookFor, index + 1);
        } while (index != -1);
        return count;
    }

    private Path writeArrayCsv(String[] headers, String[] values) throws IOException {
        Path data = file(fileName("whitespace.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            writer.print(":LABEL");
            for (String header : headers) {
                writer.print("," + header);
            }
            // End line
            writer.println();

            // Save value as a String in name
            writer.print("PERSON");
            // For each type
            for (String ignored : headers) {
                boolean comma = true;
                for (String value : values) {
                    if (comma) {
                        writer.print(",");
                        comma = false;
                    } else {
                        writer.print(";");
                    }
                    writer.print(value);
                }
            }
            // End line
            writer.println();
        }
        return data;
    }

    private static String joinStringArray(String[] values) {
        return Arrays.stream(values).map(String::trim).collect(joining(", ", "[", "]"));
    }

    private Path data(String... lines) throws Exception {
        Path file = file(fileName("data.csv"));
        try (PrintStream writer = writer(file, Charset.defaultCharset())) {
            for (String line : lines) {
                writer.println(line);
            }
        }
        return file;
    }

    private static Predicate<Node> nodeFilter(final String id) {
        return node -> node.getProperty("id", "").equals(id);
    }

    private void verifyData() {
        verifyData(Validators.emptyValidator(), Validators.emptyValidator());
    }

    private void verifyData(
            Validator<Node> nodeAdditionalValidation, Validator<Relationship> relationshipAdditionalValidation) {
        verifyData(NODE_COUNT, RELATIONSHIP_COUNT, nodeAdditionalValidation, relationshipAdditionalValidation);
    }

    private void verifyData(
            int expectedNodeCount,
            int expectedRelationshipCount,
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation) {
        GraphDatabaseService db = getDatabaseApi();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            int nodeCount = 0;
            int relationshipCount = 0;
            for (Node node : allNodes) {
                assertTrue(node.hasProperty("name"));
                nodeAdditionalValidation.validate(node);
                nodeCount++;
            }
            assertEquals(expectedNodeCount, nodeCount);
            try (ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
                for (Relationship relationship : allRelationships) {
                    assertTrue(relationship.hasProperty("created"));
                    relationshipAdditionalValidation.validate(relationship);
                    relationshipCount++;
                }
            }
            assertEquals(expectedRelationshipCount, relationshipCount);
            tx.commit();
        }
    }

    private void verifyRelationships(List<RelationshipDataLine> relationships) {
        GraphDatabaseService db = getDatabaseApi();
        Map<String, Node> nodesById = allNodesById(db);
        try (Transaction tx = db.beginTx()) {
            for (RelationshipDataLine relationship : relationships) {
                Node startNode = nodesById.get(relationship.startNodeId);
                Node endNode = nodesById.get(relationship.endNodeId);
                if (startNode == null || endNode == null) {
                    // OK this is a relationship referring to a missing node, skip it
                    continue;
                }
                startNode = tx.getNodeByElementId(startNode.getElementId());
                endNode = tx.getNodeByElementId(endNode.getElementId());
                assertNotNull(findRelationship(startNode, endNode, relationship), relationship.toString());
            }
            tx.commit();
        }
    }

    private static Relationship findRelationship(
            Node startNode, final Node endNode, final RelationshipDataLine relationship) {
        try (Stream<Relationship> relationships = startNode.getRelationships(withName(relationship.type)).stream()) {
            return relationships
                    .filter(item -> item.getEndNode().equals(endNode)
                            && item.getProperty("name").equals(relationship.name))
                    .findFirst()
                    .orElse(null);
        }
    }

    private static Map<String, Node> allNodesById(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            Map<String, Node> nodes = new HashMap<>();
            for (Node node : allNodes) {
                nodes.put(idOf(node), node);
            }
            tx.commit();
            return nodes;
        }
    }

    private static String idOf(Node node) {
        return (String) node.getProperty("id");
    }

    private static List<String> nodeIds() {
        return nodeIds(NODE_COUNT);
    }

    private static List<String> nodeIds(int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(randomNodeId());
        }
        return ids;
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private Path nodeData(boolean includeHeader, Configuration config, List<String> nodeIds, IntPredicate linePredicate)
            throws Exception {
        return nodeData(includeHeader, config, nodeIds, linePredicate, Charset.defaultCharset());
    }

    private Path nodeData(
            boolean includeHeader,
            Configuration config,
            List<String> nodeIds,
            IntPredicate linePredicate,
            Charset encoding)
            throws Exception {
        return nodeData(includeHeader, config, nodeIds, linePredicate, encoding, 0);
    }

    private Path nodeData(
            boolean includeHeader,
            Configuration config,
            List<String> nodeIds,
            IntPredicate linePredicate,
            Charset encoding,
            int extraColumns)
            throws Exception {
        return createAndWriteFile("nodes.csv", encoding, writer -> {
            if (includeHeader) {
                writeNodeHeader(writer, config, null);
            }
            writeNodeData(writer, config, nodeIds, linePredicate, extraColumns);
        });
    }

    private Path createAndWriteFile(String name, Charset encoding, Consumer<PrintStream> dataWriter) throws Exception {
        Path file = file(fileName(name));
        try (PrintStream writer = writer(file, encoding)) {
            dataWriter.accept(writer);
        }
        return file;
    }

    private static PrintStream writer(Path file, Charset encoding) throws Exception {
        return new PrintStream(Files.newOutputStream(file), false, encoding);
    }

    private Path nodeHeader(Configuration config) throws Exception {
        return nodeHeader(config, null);
    }

    private Path nodeHeader(Configuration config, String idGroup) throws Exception {
        return nodeHeader(config, idGroup, Charset.defaultCharset());
    }

    private Path nodeHeader(Configuration config, String idGroup, Charset encoding) throws Exception {
        return createAndWriteFile("nodes-header.csv", encoding, writer -> writeNodeHeader(writer, config, idGroup));
    }

    private static void writeNodeHeader(PrintStream writer, Configuration config, String idGroup) {
        char delimiter = config.delimiter();
        writer.println(idEntry("id", Type.ID, idGroup) + delimiter + "name" + delimiter + "labels:LABEL");
    }

    private static String idEntry(String name, Type type, String idGroup) {
        return (name != null ? name : "") + ":" + type.name() + (idGroup != null ? "(" + idGroup + ")" : "");
    }

    private void writeNodeData(
            PrintStream writer,
            Configuration config,
            List<String> nodeIds,
            IntPredicate linePredicate,
            int extraColumns) {
        char delimiter = config.delimiter();
        char arrayDelimiter = config.arrayDelimiter();
        for (int i = 0; i < nodeIds.size(); i++) {
            if (linePredicate.test(i)) {
                writer.println(getLine(nodeIds.get(i), delimiter, arrayDelimiter, extraColumns));
            }
        }
    }

    private String getLine(String nodeId, char delimiter, char arrayDelimiter, int extraColumns) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(nodeId)
                .append(delimiter)
                .append(randomName())
                .append(delimiter)
                .append(randomLabels(arrayDelimiter));

        for (int i = 0; i < extraColumns; i++) {
            stringBuilder.append(delimiter).append("ExtraColumn").append(i);
        }

        return stringBuilder.toString();
    }

    private String randomLabels(char arrayDelimiter) {
        int length = random.nextInt(3);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(arrayDelimiter);
            }
            builder.append(labelName(random.nextInt(MAX_LABEL_ID)));
        }
        return builder.toString();
    }

    private static String labelName(int number) {
        return "LABEL_" + number;
    }

    private String randomName() {
        int length = random.nextInt(10) + 5;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(20)));
        }
        return builder.toString();
    }

    private Path relationshipData(
            boolean includeHeader,
            Configuration config,
            List<String> nodeIds,
            IntPredicate linePredicate,
            boolean specifyType)
            throws Exception {
        return relationshipData(includeHeader, config, nodeIds, linePredicate, specifyType, Charset.defaultCharset());
    }

    private Path relationshipData(
            boolean includeHeader,
            Configuration config,
            List<String> nodeIds,
            IntPredicate linePredicate,
            boolean specifyType,
            Charset encoding)
            throws Exception {
        return relationshipData(
                includeHeader, config, randomRelationships(nodeIds), linePredicate, specifyType, encoding);
    }

    private Path relationshipData(
            boolean includeHeader,
            Configuration config,
            Iterator<RelationshipDataLine> data,
            IntPredicate linePredicate,
            boolean specifyType)
            throws Exception {
        return relationshipData(includeHeader, config, data, linePredicate, specifyType, Charset.defaultCharset());
    }

    private Path relationshipData(
            boolean includeHeader,
            Configuration config,
            Iterator<RelationshipDataLine> data,
            IntPredicate linePredicate,
            boolean specifyType,
            Charset encoding)
            throws Exception {
        return createAndWriteFile("relationships.csv", encoding, writer -> {
            if (includeHeader) {
                writeRelationshipHeader(writer, config, null, null, specifyType);
            }
            writeRelationshipData(writer, config, data, linePredicate, specifyType);
        });
    }

    private Path relationshipHeader(Configuration config) throws Exception {
        return relationshipHeader(config, Charset.defaultCharset());
    }

    private Path relationshipHeader(Configuration config, Charset encoding) throws Exception {
        return relationshipHeader(config, null, null, true, encoding);
    }

    private Path relationshipHeader(Configuration config, String startIdGroup, String endIdGroup, boolean specifyType)
            throws Exception {
        return relationshipHeader(config, startIdGroup, endIdGroup, specifyType, Charset.defaultCharset());
    }

    private Path relationshipHeader(
            Configuration config, String startIdGroup, String endIdGroup, boolean specifyType, Charset encoding)
            throws Exception {
        return createAndWriteFile(
                "relationships-header.csv",
                encoding,
                writer -> writeRelationshipHeader(writer, config, startIdGroup, endIdGroup, specifyType));
    }

    private String fileName(String name) {
        return dataIndex++ + "-" + name;
    }

    private Path file(String localname) {
        return testDirectory.file(localname);
    }

    private Path reportFile() {
        return file(REPORT);
    }

    private static void writeRelationshipHeader(
            PrintStream writer, Configuration config, String startIdGroup, String endIdGroup, boolean specifyType) {
        char delimiter = config.delimiter();
        writer.println(idEntry(null, Type.START_ID, startIdGroup) + delimiter + idEntry(null, Type.END_ID, endIdGroup)
                + (specifyType ? (delimiter + ":" + Type.TYPE) : "")
                + delimiter
                + "created:long" + delimiter
                + "name:String");
    }

    private record RelationshipDataLine(String startNodeId, String endNodeId, String type, String name) {}

    private static RelationshipDataLine relationship(String startNodeId, String endNodeId, String type) {
        return relationship(startNodeId, endNodeId, type, null);
    }

    private static RelationshipDataLine relationship(String startNodeId, String endNodeId, String type, String name) {
        return new RelationshipDataLine(startNodeId, endNodeId, type, name);
    }

    private static void writeRelationshipData(
            PrintStream writer,
            Configuration config,
            Iterator<RelationshipDataLine> data,
            IntPredicate linePredicate,
            boolean specifyType) {
        char delimiter = config.delimiter();
        for (int i = 0; i < RELATIONSHIP_COUNT; i++) {
            if (!data.hasNext()) {
                break;
            }
            RelationshipDataLine entry = data.next();
            if (linePredicate.test(i)) {
                writer.println(nullSafeString(entry.startNodeId)
                        + delimiter
                        + nullSafeString(entry.endNodeId)
                        + (specifyType ? (delimiter + nullSafeString(entry.type)) : "")
                        + delimiter
                        + currentTimeMillis()
                        + delimiter
                        + (entry.name != null ? entry.name : ""));
            }
        }
    }

    private static String nullSafeString(String endNodeId) {
        return endNodeId != null ? endNodeId : "";
    }

    private Iterator<RelationshipDataLine> randomRelationships(final List<String> nodeIds) {
        return new PrefetchingIterator<>() {
            @Override
            protected RelationshipDataLine fetchNextOrNull() {
                return new RelationshipDataLine(
                        nodeIds.get(random.nextInt(nodeIds.size())),
                        nodeIds.get(random.nextInt(nodeIds.size())),
                        randomType(),
                        null);
            }
        };
    }

    private String randomType() {
        return "TYPE_" + random.nextInt(4);
    }

    private static IntPredicate lines(final int startingAt, final int endingAt /*excluded*/) {
        return line -> line >= startingAt && line < endingAt;
    }

    private String getTransactionLogsRoot() {
        return layout.transactionLogsRootDirectory().toAbsolutePath().toString();
    }

    private Path prepareDefaultConfigFile() throws IOException {
        Path dbConfig = file("neo4j.properties");
        store(
                Map.of(
                        neo4j_home.name(),
                        testDirectory.absolutePath().toString(),
                        preallocate_logical_logs.name(),
                        FALSE),
                dbConfig);

        return dbConfig;
    }

    private GraphDatabaseAPI getDatabaseApi() {
        return getDatabaseApi(DEFAULT_DATABASE_NAME);
    }

    private GraphDatabaseAPI getDatabaseApi(String defaultDatabaseName) {
        if (managementService == null) {
            managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                    .setConfig(initial_default_database, defaultDatabaseName)
                    .build();
        }
        return (GraphDatabaseAPI) managementService.database(defaultDatabaseName);
    }

    private String createDefaultDatabaseWithTokenIndexes() {
        // Default token indexes are created on startup
        try (var managementService = dbmsService()) {
            assertThat(managementService.database(DEFAULT_DATABASE_NAME).isAvailable(TimeUnit.MINUTES.toMillis(5)))
                    .isTrue();
            return managementService.database(DEFAULT_DATABASE_NAME).databaseName();
        }
    }

    private DatabaseManagementService dbmsService() {
        return new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(initial_default_database, DEFAULT_DATABASE_NAME)
                .build();
    }

    private CommandTestUtils.CapturingExecutionContext capturingCtx() {
        var homeDir = testDirectory.absolutePath();
        return capturingExecutionContext(homeDir, homeDir.resolve("conf"), testDirectory.getFileSystem());
    }

    private void runImport(String... arguments) throws Exception {
        runImport(capturingCtx(), arguments);
    }

    private void runImport(ExecutionContext ctx, String... arguments) throws Exception {
        runImport(ctx, () -> {
            var list = new ArrayList<>(Arrays.asList(arguments));
            if (!list.contains("--additional-config")) {
                var dbConfig = prepareDefaultConfigFile();
                list.add(0, "--additional-config");
                list.add(1, dbConfig.toAbsolutePath().toString());
            }
            // make sure we write in test directory if not specified
            if (!list.contains("--report-file")) {
                // prepend to not break the use of terminal positional arguments, ex. DB name
                list.add(0, "--report-file");
                list.add(1, testDirectory.file(REPORT).toAbsolutePath().toString());
            }
            return list.toArray(new String[0]);
        });
    }

    private void runImport(ExecutionContext ctx, ThrowingSupplier<String[], IOException> argProvider) throws Exception {
        var cmd = new ImportCommand.Full(ctx);
        new CommandLine(cmd).setUseSimplifiedAtFiles(true).parseArgs(argProvider.get());
        cmd.execute();
    }

    private GraphDatabaseAPI assumeAlignedFormat(GraphDatabaseAPI db) {
        assumeThat(getStoreVersion(db).formatName())
                .as("cannot migrate from block to record format variant")
                .isEqualTo("aligned");
        return db;
    }
}
