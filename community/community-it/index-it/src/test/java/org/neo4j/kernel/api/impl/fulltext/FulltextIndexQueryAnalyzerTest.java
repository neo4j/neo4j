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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;

class FulltextIndexQueryAnalyzerTest extends FulltextProceduresTestSupport {
    private String topEntity;
    private String middleEntity;
    private String bottomEntity;

    private void setUp(EntityUtil entityUtil) {
        try (Transaction tx = db.beginTx()) {
            entityUtil.createIndexWithAnalyzer(tx, "caseSensitive");
            tx.commit();
        }
        awaitIndexesOnline();

        try (Transaction tx = db.beginTx()) {
            topEntity = entityUtil.createEntityWithProperty(tx, "zebra zebra zebra zebra donkey");
            middleEntity = entityUtil.createEntityWithProperty(tx, "zebra zebra zebra donkey");
            bottomEntity = entityUtil.createEntityWithProperty(tx, "ZEBRA donkey");

            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldBePossibleToSelectAnalyzerAtQueryTime(EntityUtil entityUtil) {
        setUp(entityUtil);

        // With the case-sensitive analyzer we should only find the last entry
        try (Transaction tx = db.beginTx();
                ResourceIterator<Entity> iterator = entityUtil.queryIndexWithOptions(tx, "ZEBRA", "{}")) {
            assertThat(iterator.next().getElementId()).isEqualTo(bottomEntity);
            assertFalse(iterator.hasNext());
            tx.commit();
        }

        // Specifying another analyzer among the options should use it for the query
        // standard uses a lowercase filter and will actually search for 'zebra'
        try (Transaction tx = db.beginTx();
                ResourceIterator<Entity> iterator =
                        entityUtil.queryIndexWithOptions(tx, "ZEBRA", "{analyzer:'standard'}")) {
            assertThat(Iterators.asList(iterator.map(Entity::getElementId)))
                    .containsExactlyInAnyOrder(topEntity, middleEntity);
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void shouldFailOnNonExistingAnalyzerAtQueryTime(EntityUtil entityUtil) {
        setUp(entityUtil);

        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx();
                            ResourceIterator<Entity> iterator =
                                    entityUtil.queryIndexWithOptions(tx, "ZEBRA", "{analyzer:'hej'}")) {
                        assertFalse(iterator.hasNext());
                    }
                })
                .hasMessageContaining("Could not create fulltext analyzer: hej. Could not find service provider");
    }
}
