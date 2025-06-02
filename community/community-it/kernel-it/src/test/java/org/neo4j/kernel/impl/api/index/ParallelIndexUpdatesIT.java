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
package org.neo4j.kernel.impl.api.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.ValueType;

@ImpermanentDbmsExtension(configurationCallback = "configure")
@ExtendWith(RandomExtension.class)
class ParallelIndexUpdatesIT {
    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseService db;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private RandomSupport random;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseInternalSettings.parallel_index_updates_apply, true);
    }

    /**
     * This test first and foremost tests so that parallel index apply produces correct results,
     * but more specifically it exercises parallel application where each transaction updates multiple indexes -
     * where there was an issue in IndexingService#applyUpdates() causing deadlocks between index writers
     * due to writers for multiple being kept open and updated back and forth.
     */
    @Test
    void shouldApplyIndexUpdatesInParallelOnManyIndexes() throws ConsistencyCheckIncompleteException {
        // given
        int numTokens = 10;
        List<String> keys = IntStream.range(0, numTokens).mapToObj(i -> "p" + i).toList();
        List<Label> labels = IntStream.range(0, numTokens)
                .mapToObj(i -> Label.label("L" + i))
                .toList();
        List<RelationshipType> relationshipTypes = IntStream.range(0, numTokens)
                .mapToObj(i -> RelationshipType.withName("T" + i))
                .toList();
        createManyIndexes(keys, labels, relationshipTypes);

        // when
        createData(keys, labels, relationshipTypes);

        // then all should look good
        dbms.shutdown();
        var result = new ConsistencyCheckService(databaseLayout)
                .with(fs)
                .with(directory.file("report"))
                .runFullConsistencyCheck();
        assertThat(result.isSuccessful()).isTrue();
    }

    private void createData(List<String> keys, List<Label> labels, List<RelationshipType> relationshipTypes) {
        int numTransactionsPerThread = 1_000;
        int numNodesPerTransaction = 10;
        var race = new Race();
        race.addContestants(
                4,
                () -> {
                    try (var tx = db.beginTx()) {
                        List<Node> nodes = new ArrayList<>();
                        for (int i = 0; i < numNodesPerTransaction; i++) {
                            var node = tx.createNode(
                                    random.selection(labels.toArray(new Label[0]), 1, labels.size(), false));
                            setRandomProperties(keys, node);
                            nodes.add(node);
                        }
                        for (int i = 0; i < numNodesPerTransaction; i++) {
                            var startNode = random.among(nodes);
                            var endNode = random.among(nodes);
                            var relationship = startNode.createRelationshipTo(endNode, random.among(relationshipTypes));
                            setRandomProperties(keys, relationship);
                        }
                        tx.commit();
                    }
                },
                numTransactionsPerThread);
        race.goUnchecked();
    }

    private void setRandomProperties(List<String> keys, Entity entity) {
        var valueTypes = new ValueType[] {ValueType.INT, ValueType.STRING, ValueType.DATE_TIME};
        for (var key : random.selection(keys.toArray(new String[0]), 1, keys.size(), false)) {
            entity.setProperty(key, random.nextValue(random.among(valueTypes)).asObjectCopy());
        }
    }

    private void createManyIndexes(List<String> keys, List<Label> labels, List<RelationshipType> relationshipTypes) {
        int maxIndexes = keys.size() * labels.size() + keys.size() * relationshipTypes.size();
        int numIndexes = random.nextInt(maxIndexes / 2, maxIndexes);
        try (var tx = db.beginTx()) {
            for (int i = 0; i < numIndexes; i++) {
                var indexCreator = random.nextBoolean()
                        ? tx.schema().indexFor(random.among(labels))
                        : tx.schema().indexFor(random.among(relationshipTypes));
                indexCreator = indexCreator.withName("MyIndex" + i).on(random.among(keys));
                try {
                    indexCreator.create();
                } catch (ConstraintViolationException e) {
                    // This is OK, since rng is bound to produce some duplicates
                }
            }
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(10, TimeUnit.SECONDS);
        }
    }
}
