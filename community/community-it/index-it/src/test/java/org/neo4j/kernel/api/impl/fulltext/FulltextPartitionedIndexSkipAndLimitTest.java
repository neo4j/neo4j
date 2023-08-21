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
package org.neo4j.kernel.api.impl.fulltext;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class FulltextPartitionedIndexSkipAndLimitTest extends FulltextProceduresTestSupport {

    public static final int ZEBRAS = 24;

    @Inject
    private RandomSupport random;

    private String topEntity;

    private int totalEntities;

    @ExtensionCallback
    @Override
    void configure(TestDatabaseManagementServiceBuilder builder) {
        super.configure(builder);
        builder.setConfig(GraphDatabaseInternalSettings.lucene_max_partition_size, 10);
    }

    private void setUp(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        try (var tx = db.beginTx()) {
            int before = random.nextInt(1, ZEBRAS);
            for (int i = 0; i < before; i++) {
                entityUtil.createEntityWithProperty(tx, "zebra donkey");
            }

            topEntity = entityUtil.createEntityWithProperty(tx, "zebra zebra zebra zebra donkey");

            int after = random.nextInt(1, ZEBRAS);
            for (int i = 0; i < after; i++) {
                entityUtil.createEntityWithProperty(tx, "zebra donkey");
            }
            totalEntities = before + after + 1;
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void canCollectAllHitsWithPartitionedIndex(EntityUtil entityUtil) {
        setUp(entityUtil);

        try (Transaction tx = db.beginTx()) {
            try (ResourceIterator<Entity> iterator = entityUtil.queryIndexWithOptions(tx, "zebra", "{}")) {
                var list = iterator.stream().collect(toList());
                assertSearchResults(list, totalEntities - 1);
            }
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void canFindTopHitInPartitionedIndex(EntityUtil entityUtil) {
        setUp(entityUtil);

        try (Transaction tx = db.beginTx()) {
            try (ResourceIterator<Entity> iterator = entityUtil.queryIndexWithOptions(tx, "zebra", "{limit:1}")) {
                var list = iterator.stream().collect(toList());
                assertSearchResults(list, 0);
            }
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void canCollectTopHitsWithPartitionedIndex(EntityUtil entityUtil) {
        setUp(entityUtil);

        try (Transaction tx = db.beginTx()) {
            var limit = random.nextInt(1, totalEntities + 1);
            try (ResourceIterator<Entity> iterator =
                    entityUtil.queryIndexWithOptions(tx, "zebra", "{limit: " + limit + "}")) {
                var list = iterator.stream().collect(toList());
                assertSearchResults(list, limit - 1);
            }
            tx.commit();
        }
    }

    private void assertSearchResults(List<Entity> list, int extraZebras) {
        // all zebras collected
        assertThat(list).hasSize(1 + extraZebras);
        // top zebra is first
        assertThat(list.get(0).getElementId()).isEqualTo(topEntity);
        // all zebras are unique
        assertThat(list.stream().map(Entity::getElementId).collect(Collectors.toSet()))
                .hasSize(list.size());
    }
}
