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
package org.neo4j.schema;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Values;

@DbmsExtension
@ExtendWith(RandomExtension.class)
class IndexPopulationFlipRaceIT {
    private static final int NODES_PER_INDEX = 10;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private Kernel kernel;

    @Inject
    private RandomSupport random;

    @Test
    void shouldAtomicallyFlipMultipleIndexes() throws Exception {
        // A couple of times since this is probabilistic, but also because there seems to be a difference
        // in timings between the first time and all others... which is perhaps super obvious to some, but not to me.
        for (int i = 0; i < 10; i++) {
            // GIVEN
            createIndexesButDontWaitForThemToFullyPopulate(i);

            // WHEN
            Pair<long[], long[]> data = createDataThatGoesIntoToThoseIndexes(i);

            // THEN
            awaitIndexes();
            verifyThatThereAreExactlyOneIndexEntryPerNodeInTheIndexes(i, data);
        }
    }

    private void awaitIndexes() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, MINUTES);
            tx.commit();
        }
    }

    private void createIndexesButDontWaitForThemToFullyPopulate(int i) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(labelA(i)).on(keyA(i)).create();

            if (random.nextBoolean()) {
                tx.schema().indexFor(labelB(i)).on(keyB(i)).create();
            } else {
                tx.schema()
                        .constraintFor(labelB(i))
                        .assertPropertyIsUnique(keyB(i))
                        .create();
            }
            tx.commit();
        }
    }

    private static String keyB(int i) {
        return "key_b" + i;
    }

    private static Label labelB(int i) {
        return label("Label_b" + i);
    }

    private static String keyA(int i) {
        return "key_a" + i;
    }

    private static Label labelA(int i) {
        return label("Label_a" + i);
    }

    private Pair<long[], long[]> createDataThatGoesIntoToThoseIndexes(int i) {
        long[] dataA = new long[NODES_PER_INDEX];
        long[] dataB = new long[NODES_PER_INDEX];
        for (int t = 0; t < NODES_PER_INDEX; t++) {
            try (Transaction tx = db.beginTx()) {
                Node nodeA = tx.createNode(labelA(i));
                nodeA.setProperty(keyA(i), dataA[t] = nodeA.getId());
                Node nodeB = tx.createNode(labelB(i));
                nodeB.setProperty(keyB(i), dataB[t] = nodeB.getId());
                tx.commit();
            }
        }
        return Pair.of(dataA, dataB);
    }

    private void verifyThatThereAreExactlyOneIndexEntryPerNodeInTheIndexes(int i, Pair<long[], long[]> data)
            throws Exception {
        try (KernelTransaction tx = kernel.beginTransaction(IMPLICIT, AnonymousContext.read())) {
            int labelAId = tx.tokenRead().nodeLabel(labelA(i).name());
            int keyAId = tx.tokenRead().propertyKey(keyA(i));
            int labelBId = tx.tokenRead().nodeLabel(labelB(i).name());
            int keyBId = tx.tokenRead().propertyKey(keyB(i));
            IndexDescriptor indexA = single(tx.schemaRead().index(SchemaDescriptors.forLabel(labelAId, keyAId)));
            IndexDescriptor indexB = single(tx.schemaRead().index(SchemaDescriptors.forLabel(labelBId, keyBId)));

            var indexingService = db.getDependencyResolver().resolveDependency(IndexingService.class);
            try (var valueIndexReaderA = indexingService.getIndexProxy(indexA).newValueReader();
                    var valueIndexReaderB =
                            indexingService.getIndexProxy(indexB).newValueReader()) {
                for (int j = 0; j < NODES_PER_INDEX; j++) {
                    long nodeAId = data.first()[j];
                    assertEquals(
                            1,
                            valueIndexReaderA.countIndexedEntities(
                                    nodeAId, NULL_CONTEXT, new int[] {keyAId}, Values.of(nodeAId)));
                    long nodeBId = data.other()[j];
                    assertEquals(
                            1,
                            valueIndexReaderB.countIndexedEntities(
                                    nodeBId, NULL_CONTEXT, new int[] {keyBId}, Values.of(nodeBId)));
                }
            }
        }
    }
}
