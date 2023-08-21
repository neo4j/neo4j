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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.internal.helpers.ArrayUtil.array;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class IndexingStringQueryAcceptanceTest {

    private static final String KEY = "name";
    private String tokenName;

    @Inject
    private GraphDatabaseService db;

    @BeforeEach
    void setup(TestInfo testInfo) {
        tokenName = "TOKEN1-" + testInfo.getDisplayName();
    }

    @ParameterizedTest(name = "shouldSupportIndexSeek using {0} match with {1} index on {2}")
    @MethodSource("data")
    void shouldSupportIndexSeek(DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        // GIVEN
        createIndex(entityControl, withIndex);
        createEntities(entityControl, db, tokenName, dataSet.nonMatching);
        LongSet expected = createEntities(entityControl, db, tokenName, dataSet.matching);

        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            found = entityControl.findEntities(tx, tokenName, KEY, dataSet.template, dataSet.searchMode);
        }

        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(name = "shouldIncludeEntitiesCreatedInSameTxInIndexSeek using {0} match with {1} index on {2}")
    @MethodSource("data")
    void shouldIncludeEntitiesCreatedInSameTxInIndexSeek(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        // GIVEN
        createIndex(entityControl, withIndex);
        createEntities(entityControl, db, tokenName, dataSet.nonMatching[0], dataSet.nonMatching[1]);
        MutableLongSet expected =
                createEntities(entityControl, db, tokenName, dataSet.matching[0], dataSet.matching[1]);
        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            expected.add(entityControl.createEntity(tx, tokenName, map(KEY, dataSet.matching[2])));
            entityControl.createEntity(tx, tokenName, map(KEY, dataSet.nonMatching[2]));

            found = entityControl.findEntities(tx, tokenName, KEY, dataSet.template, dataSet.searchMode);
        }
        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(
            name = "shouldNotIncludeEntitiesDeletedInSameTxInIndexSeek using {0} match with {1} index on {2}")
    @MethodSource("data")
    void shouldNotIncludeEntitiesDeletedInSameTxInIndexSeek(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        // GIVEN
        createIndex(entityControl, withIndex);
        createEntities(entityControl, db, tokenName, dataSet.nonMatching[0]);
        LongSet toDelete = createEntities(
                entityControl,
                db,
                tokenName,
                dataSet.matching[0],
                dataSet.nonMatching[1],
                dataSet.matching[1],
                dataSet.nonMatching[2]);
        LongSet expected = createEntities(entityControl, db, tokenName, dataSet.matching[2]);
        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            toDelete.each(id -> entityControl.deleteEntity(tx, id));
            found = entityControl.findEntities(tx, tokenName, KEY, dataSet.template, dataSet.searchMode);
        }
        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(name = "shouldConsiderEntitiesChangedInSameTxInIndexSeek using {0} match with {1} index on {2}")
    @MethodSource("data")
    void shouldConsiderEntitiesChangedInSameTxInIndexSeek(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        // GIVEN
        createIndex(entityControl, withIndex);
        createEntities(entityControl, db, tokenName, dataSet.nonMatching[0]);
        LongSet toChangeToMatch = createEntities(entityControl, db, tokenName, dataSet.nonMatching[1]);
        MutableLongSet toChangeToNotMatch = createEntities(entityControl, db, tokenName, dataSet.matching[0]);
        MutableLongSet expected = createEntities(entityControl, db, tokenName, dataSet.matching[1]);
        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            toChangeToMatch.each(id -> {
                entityControl.setProperty(tx, id, KEY, dataSet.matching[2]);
                expected.add(id);
            });
            toChangeToNotMatch.each(id -> {
                entityControl.setProperty(tx, id, KEY, dataSet.nonMatching[2]);
                expected.remove(id);
            });

            found = entityControl.findEntities(tx, tokenName, KEY, dataSet.template, dataSet.searchMode);
        }
        // THEN
        assertThat(found).isEqualTo(expected);
    }

    void createIndex(EntityControl entityControl, IndexingMode withIndex) {
        switch (withIndex) {
            case NONE:
                try (Transaction tx = db.beginTx()) {
                    // remove all indexes, including the token indexes
                    tx.schema().getIndexes().forEach(IndexDefinition::drop);
                    tx.commit();
                }
                break;
            case PROPERTY_RANGE:
                createAndWaitForIndex(entityControl, IndexType.RANGE);
                break;
            case TOKEN:
            default:
                break;
        }
    }

    private void createAndWaitForIndex(EntityControl entityControl, IndexType indexType) {
        try (Transaction tx = db.beginTx()) {
            entityControl.createIndex(tx, tokenName, KEY, indexType);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
    }

    private static MutableLongSet createEntities(
            EntityControl entityControl, GraphDatabaseService db, String token, String... propertyValues) {
        MutableLongSet expected = new LongHashSet();
        try (Transaction tx = db.beginTx()) {
            for (String value : propertyValues) {
                expected.add(entityControl.createEntity(tx, token, map(KEY, value)));
            }
            tx.commit();
        }
        return expected;
    }

    private static Stream<Arguments> generate(
            DataSet[] dataSets, IndexingMode[] indexingModes, EntityControl[] entityControls) {
        Stream.Builder<Arguments> builder = Stream.builder();
        for (DataSet dataSet : dataSets) {
            for (IndexingMode indexingMode : indexingModes) {
                for (EntityControl entityControl : entityControls) {
                    builder.add(arguments(dataSet, indexingMode, entityControl));
                }
            }
        }
        return builder.build();
    }

    public static Stream<Arguments> data() {
        return generate(DataSet.values(), IndexingMode.values(), EntityTypes.values());
    }

    enum IndexingMode {
        NONE, // No index, fallback to scan
        TOKEN, // NLI or RTI only
        PROPERTY_RANGE // property index
    }

    enum DataSet {
        EXACT(
                StringSearchMode.EXACT,
                "Johan",
                array("Johan", "Johan", "Johan"),
                array("Johanna", "Olivia", "InteJohan")),
        PREFIX(
                StringSearchMode.PREFIX,
                "Olivia",
                array("Olivia", "Olivia2", "OliviaYtterbrink"),
                array("Johan", "olivia", "InteOlivia")),
        SUFFIX(
                StringSearchMode.SUFFIX,
                "sson",
                array("Jansson", "Hansson", "Svensson"),
                array("Taverner", "Svensson-Averbuch", "Taylor")),
        CONTAINS(StringSearchMode.CONTAINS, "oo", array("good", "fool", "fooooood"), array("evil", "genius", "hungry"));

        private final StringSearchMode searchMode;
        private final String template;
        private final String[] matching;
        private final String[] nonMatching;

        DataSet(StringSearchMode searchMode, String template, String[] matching, String[] nonMatching) {
            this.searchMode = searchMode;
            this.template = template;
            this.matching = matching;
            this.nonMatching = nonMatching;
        }
    }

    interface EntityControl {

        void createIndex(Transaction tx, String token, String propertyName, IndexType indexType);

        long createEntity(Transaction tx, String token, Map<String, Object> properties);

        void deleteEntity(Transaction tx, long id);

        void setProperty(Transaction tx, long id, String key, String value);

        LongSet findEntities(
                Transaction tx, String token, String propertyName, String template, StringSearchMode searchMode);
    }

    enum EntityTypes implements EntityControl {
        NODE {
            @Override
            public void createIndex(Transaction tx, String token, String propertyName, IndexType indexType) {
                tx.schema()
                        .indexFor(Label.label(token))
                        .withIndexType(indexType)
                        .on(propertyName)
                        .create();
            }

            @Override
            public long createEntity(Transaction tx, String token, Map<String, Object> properties) {
                Node node = tx.createNode(Label.label(token));
                properties.forEach(node::setProperty);
                return node.getId();
            }

            @Override
            public void deleteEntity(Transaction tx, long id) {
                tx.getNodeById(id).delete();
            }

            @Override
            public void setProperty(Transaction tx, long id, String key, String value) {
                tx.getNodeById(id).setProperty(key, value);
            }

            @Override
            public LongSet findEntities(
                    Transaction tx, String token, String propertyName, String template, StringSearchMode searchMode) {
                MutableLongSet found = new LongHashSet();
                try (var nodes = tx.findNodes(Label.label(token), propertyName, template, searchMode).stream()) {
                    nodes.mapToLong(Node::getId).forEach(found::add);
                }
                return found;
            }
        },

        RELATIONSHIP {
            @Override
            public void createIndex(Transaction tx, String token, String propertyName, IndexType indexType) {
                tx.schema()
                        .indexFor(RelationshipType.withName(token))
                        .withIndexType(indexType)
                        .on(propertyName)
                        .create();
            }

            @Override
            public long createEntity(Transaction tx, String token, Map<String, Object> properties) {
                Node from = tx.createNode();
                Node to = tx.createNode();
                Relationship relationship = from.createRelationshipTo(to, RelationshipType.withName(token));

                properties.forEach(relationship::setProperty);
                return relationship.getId();
            }

            @Override
            public void deleteEntity(Transaction tx, long id) {
                tx.getRelationshipById(id).delete();
            }

            @Override
            public void setProperty(Transaction tx, long id, String key, String value) {
                tx.getRelationshipById(id).setProperty(key, value);
            }

            @Override
            public LongSet findEntities(
                    Transaction tx, String token, String propertyName, String template, StringSearchMode searchMode) {
                MutableLongSet found = new LongHashSet();
                try (var relationships = tx
                        .findRelationships(RelationshipType.withName(token), propertyName, template, searchMode)
                        .stream()) {
                    relationships.mapToLong(Relationship::getId).forEach(found::add);
                }
                return found;
            }
        }
    }
}
