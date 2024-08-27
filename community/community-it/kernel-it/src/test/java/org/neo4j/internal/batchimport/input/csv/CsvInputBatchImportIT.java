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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.csv.reader.Configuration.COMMAS;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.io.fs.FileSystemUtils.writeString;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.nested;
import static org.neo4j.kernel.impl.util.AutoCreatingHashMap.values;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
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
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
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
import org.neo4j.internal.batchimport.DefaultAdditionalIds;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.InputEntityDecorators;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
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
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;

@Neo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class CsvInputBatchImportIT {
    /** Don't support these counts at the moment so don't compute them */
    private static final boolean COMPUTE_DOUBLE_SIDED_RELATIONSHIP_COUNTS = false;

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
    void shouldImportDataComingFromCsvFiles() throws Exception {
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
                    NULL_CONTEXT_FACTORY);
            Groups groups = new Groups();
            var group = groups.getOrCreate(null);
            List<InputEntity> nodeData = randomNodeData(group);
            List<InputEntity> relationshipData = randomRelationshipData(nodeData, group);

            // WHEN
            importer.doImport(csv(
                    nodeDataAsFile(nodeData),
                    relationshipDataAsFile(relationshipData),
                    IdType.STRING,
                    lowBufferSize(COMMAS),
                    groups));
            // THEN
            verifyImportedData(nodeData, relationshipData);
        }
    }

    static Input csv(Path nodes, Path relationships, IdType idType, Configuration configuration, Groups groups) {
        return new CsvInput(
                DataFactories.datas(DataFactories.data(InputEntityDecorators.NO_DECORATOR, defaultCharset(), nodes)),
                DataFactories.defaultFormatNodeFileHeader(testDefaultTimeZone, false),
                DataFactories.datas(
                        DataFactories.data(InputEntityDecorators.NO_DECORATOR, defaultCharset(), relationships)),
                DataFactories.defaultFormatRelationshipFileHeader(testDefaultTimeZone, false),
                idType,
                configuration,
                false,
                CsvInput.NO_MONITOR,
                groups,
                INSTANCE);
    }

    private static Configuration lowBufferSize(Configuration actual) {
        return actual.toBuilder().withBufferSize(10_000).build();
    }

    // ======================================================
    // Below is code for generating import data
    // ======================================================

    private List<InputEntity> randomNodeData(Group group) {
        List<InputEntity> nodes = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            InputEntity node = new InputEntity();
            node.id(UUID.randomUUID().toString(), group);
            node.property("name", "Node " + i);
            node.property("pointA", "\"   { x : -4.2, y : " + i % 90 + ", crs: WGS-84 } \"");
            node.property("pointB", "\" { x : -8, y : " + i + " } \"");
            node.property("date", LocalDate.of(2018, i % 12 + 1, i % 28 + 1));
            node.property("time", OffsetTime.of(1, i % 60, 0, 0, ZoneOffset.ofHours(9)));
            node.property("dateTime", ZonedDateTime.of(2011, 9, 11, 8, i % 60, 0, 0, ZoneId.of("Europe/Stockholm")));
            node.property("dateTime2", LocalDateTime.of(2011, 9, 11, 8, i % 60, 0, 0)); // No zone specified
            node.property("localTime", LocalTime.of(1, i % 60, 0));
            node.property("localDateTime", LocalDateTime.of(2011, 9, 11, 8, i % 60));
            node.property("duration", Period.of(2, -3, i % 30));
            node.property("floatArray", new float[] {1.0f, 2.0f, 3.0f});
            node.property("dateArray", new LocalDate[] {LocalDate.of(2018, i % 12 + 1, i % 28 + 1)});
            node.property("pointArray", "\" { x : -8, y : " + i + " } \"");
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

    private Path relationshipDataAsFile(List<InputEntity> relationshipData) throws IOException {
        Path file = testDirectory.file("relationships.csv");
        try (var stringWriter = new StringWriter()) {
            // Header
            println(stringWriter, ":start_id,:end_id,:type");
            // Data
            for (InputEntity relationship : relationshipData) {
                println(
                        stringWriter,
                        relationship.startId() + "," + relationship.endId() + "," + relationship.stringType);
            }
            writeString(fileSystem, file, stringWriter.toString(), INSTANCE);
        }
        return file;
    }

    private Path nodeDataAsFile(List<InputEntity> nodeData) throws IOException {
        Path file = testDirectory.file("nodes.csv");
        try (StringWriter writer = new StringWriter()) {
            // Header
            println(
                    writer,
                    "id:ID,name,pointA:Point{crs:WGS-84},pointB:Point,date:Date,time:Time,dateTime:DateTime,dateTime2:DateTime,localTime:LocalTime,"
                            + "localDateTime:LocalDateTime,duration:Duration,floatArray:float[],dateArray:date[],pointArray:point[],some-labels:LABEL");

            // Data
            for (InputEntity node : nodeData) {
                String csvLabels = csvLabels(node.labels());
                StringBuilder sb = new StringBuilder().append(node.id()).append(',');
                for (int i = 0; i < node.propertyCount(); i++) {
                    sb.append(serializePropertyValue(node.propertyValue(i))).append(',');
                }
                if (csvLabels != null && !csvLabels.isEmpty()) {
                    sb.append(csvLabels);
                }
                println(writer, sb.toString());
            }
            writeString(fileSystem, file, writer.toString(), INSTANCE);
        }
        return file;
    }

    private String serializePropertyValue(Object value) {
        if (value.getClass().isArray()) {
            if (value.getClass().getComponentType() == Float.TYPE) {
                return StringUtils.join((float[]) value, ';');
            }
            return StringUtils.join((Object[]) value, ';');
        }
        return value.toString();
    }

    private String csvLabels(String[] labels) {
        if (labels == null || labels.length == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (String label : labels) {
            builder.append(builder.length() > 0 ? ";" : "").append(label);
        }
        return builder.toString();
    }

    private static void println(Writer writer, String string) throws IOException {
        writer.write(string + "\n");
    }

    private List<InputEntity> randomRelationshipData(List<InputEntity> nodeData, Group group) {
        List<InputEntity> relationships = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
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
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            // Verify nodes
            for (Node node : allNodes) {
                String name = (String) node.getProperty("name");
                String[] labels = expectedNodeNames.remove(name);
                assertEquals(asSet(labels), names(node.getLabels()));

                // Verify node properties
                Map<String, Consumer<Object>> expectedPropertyVerifiers = expectedNodePropertyVerifiers.remove(name);
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
                    String startNodeName = (String) relationship.getStartNode().getProperty("name");
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

            Function<String, Integer> relationshipTypeTranslationTable =
                    translationTable(neoStores.getRelationshipTypeTokenStore(), ANY_RELATIONSHIP_TYPE, storageEngine);
            for (Pair<RelationshipCountKey, Long> count : allRelationshipCounts(
                    labelTranslationTable, relationshipTypeTranslationTable, expectedRelationshipCounts)) {
                RelationshipCountKey key = count.first();
                assertEquals(
                        count.other().longValue(),
                        counts.relationshipCount(key.startLabel, key.type, key.endLabel, NULL_CONTEXT),
                        "Label count mismatch for label " + key);
            }

            tx.commit();
        } finally {
            managementService.shutdown();
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
            for (int i = 0; i < node.propertyCount(); i++) {
                final Object expectedValue = node.propertyValue(i);
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
                propertyVerifiers.put((String) node.propertyKey(i), verify);
            }

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
        return (String) node.properties()[1];
    }

    private static int indexOf(InputEntity node) {
        return Integer.parseInt(((String) node.properties()[1]).split("\\s")[1]);
    }
}
