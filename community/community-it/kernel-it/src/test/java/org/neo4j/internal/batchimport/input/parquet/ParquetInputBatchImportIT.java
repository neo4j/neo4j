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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.batchimport.api.input.IdType.INTEGER;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.values;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import blue.strategic.parquet.Dehydrator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.FileGroup;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.DefaultAdditionalIds;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.impl.util.AutoCreatingHashMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

@Neo4jLayoutExtension
@RandomSupportExtension
class ParquetInputBatchImportIT {
    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;

    private static final long ROW_GROUP_SIZE = 1024;
    private static final int GENERATED_NODE_COUNT = 4096;
    private static final int GENERATED_RELATIONSHIP_COUNT = 4096 * 3;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private static final Supplier<ZoneId> testDefaultTimeZone = () -> ZoneId.of("Asia/Shanghai");

    @Test
    void shouldImportDataComingFromParquetFiles() throws Exception {
        // GIVEN
        Config dbConfig = Config.newBuilder()
                .set(db_timezone, LogTimeZone.SYSTEM)
                .set(dense_node_threshold, 5)
                .build();
        try (JobScheduler scheduler = new ThreadPoolJobScheduler()) {
            BatchImporter importer = new ParallelBatchImporter(
                    databaseLayout,
                    fileSystem,
                    PageCacheTracer.NULL,
                    smallBatchSizeConfig(),
                    NullLogService.getInstance(),
                    ExecutionMonitor.INVISIBLE,
                    DefaultAdditionalIds.EMPTY,
                    new EmptyLogTailMetadata(dbConfig),
                    dbConfig,
                    Monitor.NO_MONITOR,
                    scheduler,
                    Collector.EMPTY,
                    TransactionLogInitializer.getLogFilesInitializer(),
                    new IndexImporterFactoryImpl(),
                    INSTANCE,
                    NULL_CONTEXT_FACTORY,
                    DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
            Groups groups = new Groups();
            var group = groups.getOrCreate(null);
            List<InputEntity> nodeData = randomNodeData(group);
            List<InputEntity> relationshipData = randomRelationshipData(nodeData, group);

            // WHEN
            importer.doImport(
                    parquet(nodeDataAsFile(nodeData), relationshipDataAsFile(relationshipData), IdType.STRING, groups));
            // THEN
            verifyImportedData(nodeData, relationshipData);
        }
    }

    @Test
    void shouldImportParquetWithIntegerIdColumnsAndStringIdType() throws Exception {
        // Regression test for KRNL-1594: when nodes are registered with String input ids
        // and relationships carry INT64 ids (no per-column id-type) under a STRING global idType,
        // the IdMapper's strict node check would compare Long.equals(String) and report a missing
        // node. The fix in ParquetDataInputChunk converts numeric ids to their String
        // representation when the effective id type is STRING.
        Config dbConfig = Config.newBuilder()
                .set(db_timezone, LogTimeZone.SYSTEM)
                .set(dense_node_threshold, 5)
                .build();
        try (JobScheduler scheduler = new ThreadPoolJobScheduler();
                var outputStream = new ByteArrayOutputStream();
                var badCollector = BadCollector.create(outputStream, 0, 20)) {
            BatchImporter importer = new ParallelBatchImporter(
                    databaseLayout,
                    fileSystem,
                    PageCacheTracer.NULL,
                    strictSmallBatchSizeConfig(),
                    NullLogService.getInstance(),
                    ExecutionMonitor.INVISIBLE,
                    DefaultAdditionalIds.EMPTY,
                    new EmptyLogTailMetadata(dbConfig),
                    dbConfig,
                    Monitor.NO_MONITOR,
                    scheduler,
                    badCollector,
                    TransactionLogInitializer.getLogFilesInitializer(),
                    new IndexImporterFactoryImpl(),
                    INSTANCE,
                    NULL_CONTEXT_FACTORY,
                    DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
            Groups groups = new Groups();
            groups.getOrCreate(null);

            Path nodesFile = nodesWithStringIdAsFile();
            Path relationshipsFile = relationshipsWithIntegerIdsAsFile();

            importer.doImport(parquet(nodesFile, relationshipsFile, IdType.STRING, groups));

            try (DatabaseManagementService managementService =
                    new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build()) {
                GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
                try (Transaction tx = db.beginTx()) {
                    Map<String, Node> nodesById = new HashMap<>();
                    try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                        for (Node node : allNodes) {
                            nodesById.put((String) node.getProperty("inputId"), node);
                        }
                    }
                    assertEquals(3, nodesById.size());

                    Map<String, String> actualRelationships = new HashMap<>();
                    try (ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
                        for (Relationship relationship : allRelationships) {
                            String startInputId =
                                    (String) relationship.getStartNode().getProperty("inputId");
                            String endInputId =
                                    (String) relationship.getEndNode().getProperty("inputId");
                            actualRelationships.put(
                                    startInputId + "->" + endInputId,
                                    relationship.getType().name());
                        }
                    }

                    assertEquals(
                            Map.of(
                                    "9345850217180->6597069807267", "COMMENT_HAS_CREATOR",
                                    "6597069807267->1", "KNOWS"),
                            actualRelationships);
                    tx.commit();
                }
            }
        }
    }

    private Path nodesWithStringIdAsFile() throws IOException {
        Path file = testDirectory.file("nodes-string-id.parquet");
        PrimitiveType idType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("inputId:ID");
        PrimitiveType labelType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":LABEL");
        var writer = new BasicParquetWriterBuilder<>(new TestOutputFile(file))
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withType(new MessageType("Nodes", idType, labelType))
                .withDehydrator((record, valueWriter) -> {
                    var row = (Object[]) record;
                    valueWriter.write("inputId:ID", row[0]);
                    valueWriter.write(":LABEL", row[1]);
                })
                .build();
        writer.write(new Object[] {"9345850217180", "Comment"});
        writer.write(new Object[] {"6597069807267", "Person"});
        writer.write(new Object[] {"1", "Person"});
        writer.close();
        return file;
    }

    private Path relationshipsWithIntegerIdsAsFile() throws IOException {
        Path file = testDirectory.file("relationships-int-id.parquet");
        PrimitiveType startId =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":START_ID");
        PrimitiveType endId =
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(":END_ID");
        Type type = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":TYPE");
        var writer = new BasicParquetWriterBuilder<>(new TestOutputFile(file))
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withType(new MessageType("Relationships", startId, endId, type))
                .withDehydrator((record, valueWriter) -> {
                    var row = (Object[]) record;
                    valueWriter.write(":START_ID", row[0]);
                    valueWriter.write(":END_ID", row[1]);
                    valueWriter.write(":TYPE", row[2]);
                })
                .build();
        writer.write(new Object[] {9345850217180L, 6597069807267L, "COMMENT_HAS_CREATOR"});
        writer.write(new Object[] {6597069807267L, 1L, "KNOWS"});
        writer.close();
        return file;
    }

    @Test
    void shouldYieldCorrectGroupWarning() throws Exception {
        // GIVEN
        Config dbConfig = Config.newBuilder()
                .set(db_timezone, LogTimeZone.SYSTEM)
                .set(dense_node_threshold, 5)
                .build();
        try (JobScheduler scheduler = new ThreadPoolJobScheduler();
                var outputStream = new ByteArrayOutputStream();
                var badCollector = BadCollector.create(outputStream, 0, 20)) {

            BatchImporter importer = new ParallelBatchImporter(
                    databaseLayout,
                    fileSystem,
                    PageCacheTracer.NULL,
                    smallBatchSizeConfig(),
                    NullLogService.getInstance(),
                    ExecutionMonitor.INVISIBLE,
                    DefaultAdditionalIds.EMPTY,
                    new EmptyLogTailMetadata(dbConfig),
                    dbConfig,
                    Monitor.NO_MONITOR,
                    scheduler,
                    badCollector,
                    TransactionLogInitializer.getLogFilesInitializer(),
                    new IndexImporterFactoryImpl(),
                    INSTANCE,
                    NULL_CONTEXT_FACTORY,
                    DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
            Groups groups = new Groups();
            groups.getOrCreate(null);
            groups.getOrCreate("EndGroup");

            var message = assertThrows(
                            InputException.class,
                            () -> importer.doImport(parquet(
                                    Path.of(ParquetInputBatchImportIT.class
                                            .getResource("/org/neo4j/internal/batchimport/nodeGroup1.parquet")
                                            .toURI()),
                                    Path.of(ParquetInputBatchImportIT.class
                                            .getResource("/org/neo4j/internal/batchimport/nodeGroup2.parquet")
                                            .toURI()),
                                    Path.of(ParquetInputBatchImportIT.class
                                            .getResource("/org/neo4j/internal/batchimport/relationships.parquet")
                                            .toURI()),
                                    groups)))
                    .getMessage();

            assertThat(message.contains("123 (StartGroup)-[TYPE]->234 (EndGroup) referring to missing node 234"));
        }
    }

    static Input parquet(Path nodeGroup1, Path nodeGroup2, Path relationships, Groups groups) {
        return new ParquetInput(
                Map.of(
                        Set.of("STARTTHING"),
                        List.of(new FileGroup(new FileGroup.NumberedFile(0, nodeGroup1))),
                        Set.of("ENDTHING"),
                        List.of(new FileGroup(new FileGroup.NumberedFile(1, nodeGroup2)))),
                Map.of("", List.of(new FileGroup(new FileGroup.NumberedFile(2, relationships)))),
                ResolvedSchemaCommands.of(),
                INTEGER,
                Configuration.newBuilder().build(),
                groups,
                new ParquetMonitor(System.out));
    }

    static Input parquet(Path nodes, Path relationships, IdType idType, Groups groups) {
        FileGroup nodeFileGroup = new FileGroup(new FileGroup.NumberedFile(0, nodes));
        FileGroup relationshipFileGroup = new FileGroup(new FileGroup.NumberedFile(1, relationships));
        return new ParquetInput(
                Map.of(Set.of(""), Collections.singletonList(nodeFileGroup)),
                Map.of("", Collections.singletonList(relationshipFileGroup)),
                ResolvedSchemaCommands.of(),
                idType,
                Configuration.newBuilder().build(),
                groups,
                new ParquetMonitor(System.out));
    }

    // ======================================================
    // Below is code for generating import data
    // ======================================================

    private List<InputEntity> randomNodeData(Group group) {
        List<InputEntity> nodes = new ArrayList<>();
        for (int i = 0; i < GENERATED_NODE_COUNT; i++) {
            InputEntity node = new InputEntity();
            node.id(UUID.randomUUID().toString(), group);
            node.property("name", "Node " + i, false);
            node.property("pointA", "\"   { x : -4.2, y : " + i % 90 + ", crs: WGS-84 } \"", false);
            node.property("pointB", "\" { x : -8, y : " + i + " } \"", false);
            node.property("date", LocalDate.of(2018, i % 12 + 1, i % 28 + 1), false);
            node.property("time", OffsetTime.of(1, i % 60, 0, 0, ZoneOffset.ofHours(9)), false);
            node.property(
                    "dateTime", ZonedDateTime.of(2011, 9, 11, 8, i % 60, 0, 0, ZoneId.of("Europe/Stockholm")), false);
            node.property("dateTime2", LocalDateTime.of(2011, 9, 11, 8, i % 60, 0, 0), false); // No zone specified
            node.property("localTime", LocalTime.of(1, i % 60, 0), false);
            node.property("localDateTime", LocalDateTime.of(2011, 9, 11, 8, i % 60), false);
            node.property("duration", Period.of(2, -3, i % 30), false);
            node.property("floatArray", new float[] {1.0f, 2.0f, 3.0f}, false);
            node.property("dateArray", new LocalDate[] {LocalDate.of(2018, i % 12 + 1, i % 28 + 1)}, false);
            node.property("pointArray", "\" { x : -8, y : " + i + " } \"", false);
            node.labels(randomLabels(random.random()));
            nodes.add(node);
        }
        return nodes;
    }

    private static String[] randomLabels(Random random) {
        String[] labels = new String[random.nextInt(3)];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = "Label" + random.nextInt(4);
        }
        return labels;
    }

    private static org.neo4j.batchimport.api.Configuration smallBatchSizeConfig() {
        return org.neo4j.batchimport.api.Configuration.withBatchSize(
                org.neo4j.batchimport.api.Configuration.DEFAULT, 100);
    }

    private static org.neo4j.batchimport.api.Configuration strictSmallBatchSizeConfig() {
        return new org.neo4j.batchimport.api.Configuration.Overridden(smallBatchSizeConfig()) {
            @Override
            public boolean strictNodeCheck() {
                return true;
            }
        };
    }

    private Path relationshipDataAsFile(List<InputEntity> relationshipData) throws IOException {
        Path file = testDirectory.file("relationships.parquet");
        PrimitiveType startId = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":START_ID");
        PrimitiveType endId = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":END_ID");
        Type type = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":TYPE");
        var writer = new BasicParquetWriterBuilder<>(new TestOutputFile(file))
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withType(new MessageType("Some Data", startId, endId, type))
                .withDehydrator((record, valueWriter) -> {
                    var relationship = (Object[]) record;
                    valueWriter.write(":START_ID", relationship[0]);
                    valueWriter.write(":END_ID", relationship[1]);
                    valueWriter.write(":TYPE", relationship[2]);
                })
                .build();

        for (InputEntity relationshipDatum : relationshipData) {
            writer.write(
                    new Object[] {relationshipDatum.startId(), relationshipDatum.endId(), relationshipDatum.type()});
        }

        writer.close();
        return file;
    }

    private Path nodeDataAsFile(List<InputEntity> nodeData) throws IOException {
        Path file = testDirectory.file("nodes.parquet");
        var knownProperties = List.of("name");
        Type idType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":ID");
        Type labelsType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named(":LABEL");
        Type nameType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name");
        var writer = new BasicParquetWriterBuilder<>(new TestOutputFile(file))
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withType(new MessageType("Some Data", idType, labelsType, nameType))
                .withDehydrator((record, valueWriter) -> {
                    var node = (InputEntity) record;
                    valueWriter.write(":ID", node.id());
                    valueWriter.write(":LABEL", String.join(";", node.labels()));
                    node.propertiesAsMap().forEach((name, value) -> {
                        if (knownProperties.contains(name)) {
                            valueWriter.write(name, value);
                        }
                    });
                })
                .build();

        for (InputEntity nodeDatum : nodeData) {
            writer.write(nodeDatum);
        }
        writer.close();
        return file;
    }

    private List<InputEntity> randomRelationshipData(List<InputEntity> nodeData, Group group) {
        List<InputEntity> relationships = new ArrayList<>();
        for (int i = 0; i < GENERATED_RELATIONSHIP_COUNT; i++) {
            InputEntity relationship = new InputEntity();
            relationship.startId(nodeData.get(random.nextInt(nodeData.size())).id(), group);
            relationship.endId(nodeData.get(random.nextInt(nodeData.size())).id(), group);
            relationship.type("TYPE_" + random.nextInt(3));
            relationships.add(relationship);
        }
        return relationships;
    }

    // ======================================================
    // Below is code for verifying the imported data
    // ======================================================

    private void verifyImportedData(List<InputEntity> nodeData, List<InputEntity> relationshipData) {
        // Build up expected data for the verification below
        Map<String /*id*/, InputEntity> expectedNodes = new HashMap<>();
        Map<String, String[]> expectedNodeNames = new HashMap<>();
        Map<String, Map<String, Consumer<Object>>> expectedNodePropertyVerifiers = new HashMap<>();
        Map<String /*start node name*/, Map<String /*end node name*/, Map<String, AtomicInteger>>>
                expectedRelationships = new AutoCreatingHashMap<>(nested(nested(values(AtomicInteger.class))));
        Map<String, AtomicLong> expectedNodeCounts = new AutoCreatingHashMap<>(values(AtomicLong.class));
        Map<String, Map<String, Map<String, AtomicLong>>> expectedRelationshipCounts =
                new AutoCreatingHashMap<>(nested(nested(values(AtomicLong.class))));
        buildUpExpectedData(
                nodeData,
                relationshipData,
                expectedNodes,
                expectedNodeNames,
                expectedNodePropertyVerifiers,
                expectedRelationships,
                expectedNodeCounts,
                expectedRelationshipCounts);

        // Do the verification
        try (DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build()) {
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx();
                    ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                // Verify nodes
                for (Node node : allNodes) {
                    String name = (String) node.getProperty("name");
                    String[] labels = expectedNodeNames.remove(name);
                    assertEquals(asSet(labels), names(node.getLabels()));

                    // Verify node properties
                    Map<String, Consumer<Object>> expectedPropertyVerifiers =
                            expectedNodePropertyVerifiers.remove(name);
                    Map<String, Object> actualProperties = node.getAllProperties();
                    actualProperties.remove("id"); // The id does not exist in expected properties
                    for (Map.Entry actualProperty : actualProperties.entrySet()) {
                        Consumer v = expectedPropertyVerifiers.get(actualProperty.getKey());
                        if (v != null) {
                            v.accept(actualProperty.getValue());
                        }
                    }
                }
                assertEquals(0, expectedNodeNames.size());

                // Verify relationships
                try (ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
                    for (Relationship relationship : allRelationships) {
                        String startNodeName =
                                (String) relationship.getStartNode().getProperty("name");
                        Map<String, Map<String, AtomicInteger>> inner = expectedRelationships.get(startNodeName);
                        String endNodeName = (String) relationship.getEndNode().getProperty("name");
                        Map<String, AtomicInteger> innerInner = inner.get(endNodeName);
                        String type = relationship.getType().name();
                        int countAfterwards = innerInner.get(type).decrementAndGet();
                        assertThat(countAfterwards).isGreaterThanOrEqualTo(0);
                        if (countAfterwards == 0) {
                            innerInner.remove(type);
                            if (innerInner.isEmpty()) {
                                inner.remove(endNodeName);
                                if (inner.isEmpty()) {
                                    expectedRelationships.remove(startNodeName);
                                }
                            }
                        }
                    }
                }
                assertEquals(0, expectedRelationships.size());

                RecordStorageEngine storageEngine =
                        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(RecordStorageEngine.class);
                NeoStores neoStores = storageEngine.testAccessNeoStores();
                var counts = storageEngine.countsAccessor();
                Function<String, Integer> labelTranslationTable =
                        translationTable(neoStores.getLabelTokenStore(), ANY_LABEL, storageEngine);
                for (Pair<Integer, Long> count : allNodeCounts(labelTranslationTable, expectedNodeCounts)) {
                    assertEquals(
                            count.other().longValue(),
                            counts.nodeCount(count.first(), NULL_CONTEXT),
                            "Label count mismatch for label " + count.first());
                }

                Function<String, Integer> relationshipTypeTranslationTable = translationTable(
                        neoStores.getRelationshipTypeTokenStore(), ANY_RELATIONSHIP_TYPE, storageEngine);
                for (Pair<RelationshipCountKey, Long> count : allRelationshipCounts(
                        labelTranslationTable, relationshipTypeTranslationTable, expectedRelationshipCounts)) {
                    RelationshipCountKey key = count.first();
                    assertEquals(
                            count.other().longValue(),
                            counts.relationshipCount(key.startLabel, key.type, key.endLabel, NULL_CONTEXT),
                            "Label count mismatch for label " + key);
                }

                tx.commit();
            }
        }
    }

    private static class RelationshipCountKey {
        private final int startLabel;
        private final int type;
        private final int endLabel;

        RelationshipCountKey(int startLabel, int type, int endLabel) {
            this.startLabel = startLabel;
            this.type = type;
            this.endLabel = endLabel;
        }

        @Override
        public String toString() {
            return format("[start:%d, type:%d, end:%d]", startLabel, type, endLabel);
        }
    }

    private static Iterable<Pair<RelationshipCountKey, Long>> allRelationshipCounts(
            Function<String, Integer> labelTranslationTable,
            Function<String, Integer> relationshipTypeTranslationTable,
            Map<String, Map<String, Map<String, AtomicLong>>> counts) {
        Collection<Pair<RelationshipCountKey, Long>> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<String, AtomicLong>>> startLabel : counts.entrySet()) {
            for (Map.Entry<String, Map<String, AtomicLong>> type :
                    startLabel.getValue().entrySet()) {
                for (Map.Entry<String, AtomicLong> endLabel : type.getValue().entrySet()) {
                    RelationshipCountKey key = new RelationshipCountKey(
                            labelTranslationTable.apply(startLabel.getKey()),
                            relationshipTypeTranslationTable.apply(type.getKey()),
                            labelTranslationTable.apply(endLabel.getKey()));
                    result.add(Pair.of(key, endLabel.getValue().longValue()));
                }
            }
        }
        return result;
    }

    private static Iterable<Pair<Integer, Long>> allNodeCounts(
            Function<String, Integer> labelTranslationTable, Map<String, AtomicLong> counts) {
        Collection<Pair<Integer, Long>> result = new ArrayList<>();
        for (Map.Entry<String, AtomicLong> count : counts.entrySet()) {
            result.add(Pair.of(
                    labelTranslationTable.apply(count.getKey()),
                    count.getValue().get()));
        }
        counts.put(null, new AtomicLong(counts.size()));
        return result;
    }

    private static Function<String, Integer> translationTable(
            TokenStore<?> tokenStore, final int anyValue, RecordStorageEngine storageEngine) {
        final Map<String, Integer> translationTable = new HashMap<>();
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            for (NamedToken token : tokenStore.getTokens(storeCursors, EmptyMemoryTracker.INSTANCE)) {
                translationTable.put(token.name(), token.id());
            }
            return from -> from == null ? anyValue : translationTable.get(from);
        }
    }

    private static Set<String> names(Iterable<Label> labels) {
        Set<String> names = new HashSet<>();
        for (Label label : labels) {
            names.add(label.name());
        }
        return names;
    }

    private static void buildUpExpectedData(
            List<InputEntity> nodeData,
            List<InputEntity> relationshipData,
            Map<String, InputEntity> expectedNodes,
            Map<String, String[]> expectedNodeNames,
            Map<String, Map<String, Consumer<Object>>> expectedNodePropertyVerifiers,
            Map<String, Map<String, Map<String, AtomicInteger>>> expectedRelationships,
            Map<String, AtomicLong> nodeCounts,
            Map<String, Map<String, Map<String, AtomicLong>>> relationshipCounts) {
        for (InputEntity node : nodeData) {
            expectedNodes.put((String) node.id(), node);
            expectedNodeNames.put(nameOf(node), node.labels());

            // Build default verifiers for all the properties that compares the property value using equals
            Assertions.assertFalse(node.hasIntPropertyKeyIds);
            Map<String, Consumer<Object>> propertyVerifiers = new TreeMap<>();
            node.propertiesAsMap().forEach((key, expectedValue) -> {
                Consumer verify;
                if (expectedValue instanceof TemporalAmount) {
                    // Since there is no straightforward comparison for TemporalAmount we add it to a reference
                    // point in time and compare the result
                    verify = actualValue -> {
                        LocalDateTime referenceTemporal = LocalDateTime.of(0, 1, 1, 0, 0);
                        LocalDateTime expected = referenceTemporal.plus((TemporalAmount) expectedValue);
                        LocalDateTime actual = referenceTemporal.plus((TemporalAmount) actualValue);
                        assertEquals(expected, actual);
                    };
                } else if (expectedValue instanceof Temporal) {
                    final LocalDate expectedDate = ((Temporal) expectedValue).query(TemporalQueries.localDate());
                    final LocalTime expectedTime = ((Temporal) expectedValue).query(TemporalQueries.localTime());
                    final ZoneId expectedZoneId = ((Temporal) expectedValue).query(TemporalQueries.zone());

                    verify = actualValue -> {
                        LocalDate actualDate = ((Temporal) actualValue).query(TemporalQueries.localDate());
                        LocalTime actualTime = ((Temporal) actualValue).query(TemporalQueries.localTime());
                        ZoneId actualZoneId = ((Temporal) actualValue).query(TemporalQueries.zone());

                        assertEquals(expectedDate, actualDate);
                        assertEquals(expectedTime, actualTime);
                        if (expectedZoneId == null) {
                            if (actualZoneId != null) {
                                // If the actual value is zoned it should have the default zone
                                assertEquals(testDefaultTimeZone.get(), actualZoneId);
                            }
                        } else {
                            assertEquals(expectedZoneId, actualZoneId);
                        }
                    };
                } else if (expectedValue instanceof float[]) {
                    verify = actualValue -> assertArrayEquals((float[]) expectedValue, (float[]) actualValue);
                } else if (expectedValue.getClass().isArray()) {
                    verify = actualValue -> assertArrayEquals((Object[]) expectedValue, (Object[]) actualValue);
                } else {
                    verify = actualValue -> assertEquals(expectedValue, actualValue);
                }
                propertyVerifiers.put(key, verify);
            });

            // Special verifier for pointA property
            Consumer verifyPointA = actualValue -> {
                // The y-coordinate should match the node number modulo 90 (so we don't break wgs boundaries)
                PointValue v = (PointValue) actualValue;
                double actualY = v.getCoordinates().get(0).getCoordinate()[1];
                double expectedY = indexOf(node) % 90;
                String message = actualValue + " does not have y=" + expectedY;
                assertEquals(expectedY, actualY, 0.1, message);
                message = actualValue + " does not have crs=wgs-84";
                assertEquals(
                        CoordinateReferenceSystem.WGS_84.getName(),
                        v.getCoordinateReferenceSystem().getName(),
                        message);
            };
            propertyVerifiers.put("pointA", verifyPointA);

            // Special verifier for pointB property
            Consumer verifyPointB = actualValue -> {
                // The y-coordinate should match the node number
                PointValue v = (PointValue) actualValue;
                double actualY = v.getCoordinates().get(0).getCoordinate()[1];
                double expectedY = indexOf(node);
                String message = actualValue + " does not have y=" + expectedY;
                assertEquals(expectedY, actualY, 0.1, message);
                message = actualValue + " does not have crs=cartesian";
                assertEquals(
                        CoordinateReferenceSystem.CARTESIAN.getName(),
                        v.getCoordinateReferenceSystem().getName(),
                        message);
            };
            propertyVerifiers.put("pointB", verifyPointB);

            // Special verifier for pointArray property
            Consumer verifyPointArray = actualValue -> verifyPointB.accept(((PointValue[]) actualValue)[0]);
            propertyVerifiers.put("pointArray", verifyPointArray);

            expectedNodePropertyVerifiers.put(nameOf(node), propertyVerifiers);

            countNodeLabels(nodeCounts, node.labels());
        }
        for (InputEntity relationship : relationshipData) {
            // Expected relationship counts per node, type and direction
            InputEntity startNode = expectedNodes.get(relationship.startId());
            InputEntity endNode = expectedNodes.get(relationship.endId());
            {
                expectedRelationships
                        .get(nameOf(startNode))
                        .get(nameOf(endNode))
                        .get(relationship.stringType)
                        .incrementAndGet();
            }

            // Expected counts per start/end node label ids
            // Let's do what CountsState#addRelationship does, roughly
            relationshipCounts.get(null).get(null).get(null).incrementAndGet();
            relationshipCounts.get(null).get(relationship.stringType).get(null).incrementAndGet();
            for (String startNodeLabelName : asSet(startNode.labels())) {
                Map<String, Map<String, AtomicLong>> startLabelCounts = relationshipCounts.get(startNodeLabelName);
                startLabelCounts.get(null).get(null).incrementAndGet();
                Map<String, AtomicLong> typeCounts = startLabelCounts.get(relationship.stringType);
                typeCounts.get(null).incrementAndGet();
                if (COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS) {
                    for (String endNodeLabelName : asSet(endNode.labels())) {
                        startLabelCounts.get(null).get(endNodeLabelName).incrementAndGet();
                        typeCounts.get(endNodeLabelName).incrementAndGet();
                    }
                }
            }
            for (String endNodeLabelName : asSet(endNode.labels())) {
                relationshipCounts.get(null).get(null).get(endNodeLabelName).incrementAndGet();
                relationshipCounts
                        .get(null)
                        .get(relationship.stringType)
                        .get(endNodeLabelName)
                        .incrementAndGet();
            }
        }
    }

    private static void countNodeLabels(Map<String, AtomicLong> nodeCounts, String[] labels) {
        Set<String> seen = new HashSet<>();
        for (String labelName : labels) {
            if (seen.add(labelName)) {
                nodeCounts.get(labelName).incrementAndGet();
            }
        }
    }

    private static String nameOf(InputEntity node) {
        return (String) node.getProperty("name").value();
    }

    private static int indexOf(InputEntity node) {
        return Integer.parseInt(nameOf(node).split("\\s")[1]);
    }

    private static class TestOutputFile extends LocalOutputFile {

        TestOutputFile(Path file) {
            super(file);
        }

        @Override
        public long defaultBlockSize() {
            return 4096;
        }

        @Override
        public boolean supportsBlockSize() {
            return true;
        }
    }

    private static class BasicParquetWriterBuilder<T>
            extends org.apache.parquet.hadoop.ParquetWriter.Builder<T, BasicParquetWriterBuilder<T>> {

        private MessageType schema;
        private Dehydrator<T> dehydrator;

        BasicParquetWriterBuilder(OutputFile file) {
            super(file);
        }

        public BasicParquetWriterBuilder<T> withType(MessageType schema) {
            this.schema = schema;
            return this;
        }

        public BasicParquetWriterBuilder<T> withDehydrator(Dehydrator<T> dehydrator) {
            this.dehydrator = dehydrator;
            return this;
        }

        @Override
        protected BasicParquetWriterBuilder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(org.apache.hadoop.conf.Configuration conf) {
            return new BasicWriteSupport<>(schema, dehydrator);
        }
    }

    private static class BasicWriteSupport<T> extends WriteSupport<T> {
        private final MessageType schema;
        private final Dehydrator<T> dehydrator;
        private RecordConsumer recordConsumer;

        BasicWriteSupport(MessageType schema, Dehydrator<T> dehydrator) {
            this.schema = schema;
            this.dehydrator = dehydrator;
        }

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, Map.of(ParquetOutputFormat.BLOCK_SIZE, "4096"));
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(T record) {
            recordConsumer.startMessage();
            dehydrator.dehydrate(record, this::writeField);
            recordConsumer.endMessage();
        }

        private void writeField(String name, Object value) {
            var fieldIndex = schema.getFieldIndex(name);
            var type = schema.getType(fieldIndex).asPrimitiveType();

            recordConsumer.startField(name, fieldIndex);
            switch (type.getPrimitiveTypeName()) {
                case INT32 -> recordConsumer.addInteger(((Number) value).intValue());
                case INT64 -> recordConsumer.addLong(((Number) value).longValue());
                case BOOLEAN -> recordConsumer.addBoolean((Boolean) value);
                case FLOAT -> recordConsumer.addFloat(((Number) value).floatValue());
                case DOUBLE -> recordConsumer.addDouble(((Number) value).doubleValue());
                case BINARY -> {
                    if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                        recordConsumer.addBinary(
                                Binary.fromString(value instanceof String ? (String) value : value.toString()));
                    } else if (type.getLogicalTypeAnnotation()
                            instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                        recordConsumer.addBinary(
                                Binary.fromString(value instanceof String ? (String) value : value.toString()));
                    } else if (type.getLogicalTypeAnnotation()
                            instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        var decimal =
                                value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
                        var unscaled = decimal.unscaledValue().toByteArray();

                        recordConsumer.addBinary(Binary.fromConstantByteArray(unscaled));
                    } else {
                        throw new UnsupportedOperationException("writing of %s logical types is not supported."
                                .formatted(type.getLogicalTypeAnnotation()));
                    }
                }
                case FIXED_LEN_BYTE_ARRAY -> {
                    if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                        var uuid = value instanceof UUID ? (UUID) value : UUID.fromString(value.toString());
                        var buffer = ByteBuffer.allocate(16);
                        buffer.putLong(uuid.getMostSignificantBits());
                        buffer.putLong(uuid.getLeastSignificantBits());

                        recordConsumer.addBinary(Binary.fromConstantByteBuffer(buffer));
                    } else if (type.getLogicalTypeAnnotation()
                            instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        var decimal =
                                value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
                        var unscaled = decimal.unscaledValue().toByteArray();
                        var buffer = ByteBuffer.allocate(type.getTypeLength()).order(ByteOrder.BIG_ENDIAN);
                        buffer.position(type.getTypeLength() - unscaled.length);
                        buffer.put(unscaled);

                        recordConsumer.addBinary(Binary.fromConstantByteArray(buffer.array()));
                    } else {
                        throw new UnsupportedOperationException("writing of %s logical types is not supported."
                                .formatted(type.getLogicalTypeAnnotation()));
                    }
                }
                default ->
                    throw new UnsupportedOperationException(
                            "writing of %s primitive types is not supported.".formatted(type.getPrimitiveTypeName()));
            }
            recordConsumer.endField(name, fieldIndex);
        }
    }
}
