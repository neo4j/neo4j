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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexValueTestUtil;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.PointValue;

@DbmsExtension
class UniqueSpatialIndexIT {
    private static final String KEY = "prop";

    @Inject
    private GraphDatabaseService db;

    private PointValue point1;
    private PointValue point2;

    @BeforeEach
    void setup() {
        Pair<PointValue, PointValue> collidingPoints =
                SpatialIndexValueTestUtil.pointsWithSameValueOnSpaceFillingCurve(Config.defaults());
        point1 = collidingPoints.first();
        point2 = collidingPoints.other();
    }

    @Test
    void shouldPopulateIndexWithUniquePointsThatCollideOnSpaceFillingCurve() {
        // given
        Pair<Long, Long> nodeIds = createUniqueNodes();

        // when
        createUniquenessConstraint();

        // then
        assertBothNodesArePresent(nodeIds);
    }

    @Test
    void shouldAddPointsThatCollideOnSpaceFillingCurveToUniqueIndexInSameTx() {
        // given
        createUniquenessConstraint();

        // when
        Pair<Long, Long> nodeIds = createUniqueNodes();

        // then
        assertBothNodesArePresent(nodeIds);
    }

    @Test
    void shouldThrowWhenPopulatingWithNonUniquePoints() {
        // given
        createNonUniqueNodes();

        // then
        assertThrows(ConstraintViolationException.class, this::createUniquenessConstraint);
    }

    @Test
    void shouldThrowWhenAddingNonUniquePoints() {
        // given
        createUniquenessConstraint();

        // when
        assertThrows(ConstraintViolationException.class, this::createNonUniqueNodes);
    }

    private void createNonUniqueNodes() {
        try (Transaction tx = db.beginTx()) {
            Node originNode = tx.createNode(LABEL_ONE);
            originNode.setProperty(KEY, point1);
            Node centerNode = tx.createNode(LABEL_ONE);
            centerNode.setProperty(KEY, point1);
            tx.commit();
        }
    }

    private Pair<Long, Long> createUniqueNodes() {
        Pair<Long, Long> nodeIds;
        try (Transaction tx = db.beginTx()) {
            Node originNode = tx.createNode(LABEL_ONE);
            originNode.setProperty(KEY, point1);
            Node centerNode = tx.createNode(LABEL_ONE);
            centerNode.setProperty(KEY, point2);

            nodeIds = Pair.of(originNode.getId(), centerNode.getId());
            tx.commit();
        }
        return nodeIds;
    }

    private void assertBothNodesArePresent(Pair<Long, Long> nodeIds) {
        try (Transaction tx = db.beginTx()) {
            try (ResourceIterator<Node> origin = tx.findNodes(LABEL_ONE, KEY, point1)) {
                assertTrue(origin.hasNext());
                assertEquals(nodeIds.first().longValue(), origin.next().getId());
                assertFalse(origin.hasNext());
            }

            try (ResourceIterator<Node> center = tx.findNodes(LABEL_ONE, KEY, point2)) {
                assertTrue(center.hasNext());
                assertEquals(nodeIds.other().longValue(), center.next().getId());
                assertFalse(center.hasNext());
            }

            tx.commit();
        }
    }

    private void createUniquenessConstraint() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(LABEL_ONE).assertPropertyIsUnique(KEY).create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
    }
}
