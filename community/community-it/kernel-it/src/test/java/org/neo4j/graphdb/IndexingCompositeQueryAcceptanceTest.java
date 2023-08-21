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
import static org.neo4j.internal.helpers.collection.Iterators.array;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class IndexingCompositeQueryAcceptanceTest {
    @Inject
    private GraphDatabaseAPI db;

    public static Stream<Arguments> data() {
        return generate(DataSet.values(), IndexingMode.values(), EntityTypes.values());
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

    public static final String TOKEN_NAME = "TOKEN1";

    public void createIndex(IndexingMode withIndex, String[] keys, EntityControl entityControl) {
        switch (withIndex) {
            case NONE:
                try (Transaction tx = db.beginTx()) {
                    // remove all indexes, including the token indexes
                    tx.schema().getIndexes().forEach(IndexDefinition::drop);
                    tx.commit();
                }
                break;
            case PROPERTY_RANGE:
                createAndWaitForIndex(keys, entityControl, IndexType.RANGE);
                break;
            case TOKEN:
            default:
                break;
        }
    }

    private void createAndWaitForIndex(String[] keys, EntityControl entityControl, IndexType indexType) {
        String indexName;
        try (Transaction tx = db.beginTx()) {
            indexName = entityControl.createIndex(tx, TOKEN_NAME, keys, indexType);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(indexName, 5, TimeUnit.MINUTES);
            tx.commit();
        }
    }

    @ParameterizedTest(name = "shouldSupportIndexSeek using {0} with {1} index for {2}")
    @MethodSource("data")
    public void shouldSupportIndexSeek(DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        createIndex(withIndex, dataSet.keys, entityControl);

        // GIVEN
        createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.nonMatching);
        LongSet expected = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.values);

        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            found = dataSet.indexSeek.findEntities(tx, dataSet.keys, dataSet.values, entityControl);
        }

        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(name = "shouldSupportIndexSeekBackwardsOrder using {0} with {1} index for {2}")
    @MethodSource("data")
    public void shouldSupportIndexSeekBackwardsOrder(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        createIndex(withIndex, dataSet.keys, entityControl);

        // GIVEN
        createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.nonMatching);
        LongSet expected = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.values);

        // WHEN
        LongSet found;
        String[] reversedKeys = new String[dataSet.keys.length];
        Object[] reversedValues = new Object[dataSet.keys.length];
        for (int i = 0; i < dataSet.keys.length; i++) {
            reversedValues[dataSet.keys.length - 1 - i] = dataSet.values[i];
            reversedKeys[dataSet.keys.length - 1 - i] = dataSet.keys[i];
        }
        try (Transaction tx = db.beginTx()) {
            found = dataSet.indexSeek.findEntities(tx, reversedKeys, reversedValues, entityControl);
        }

        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(name = "shouldIncludeEntitiesCreatedInSameTxInIndexSeek using {0} with {1} index for {2}")
    @MethodSource("data")
    public void shouldIncludeEntitiesCreatedInSameTxInIndexSeek(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        createIndex(withIndex, dataSet.keys, entityControl);

        // GIVEN
        createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.nonMatching[0], dataSet.nonMatching[1]);
        MutableLongSet expected = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.values);
        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            expected.add(entityControl.createEntity(tx, TOKEN_NAME, propertyMap(dataSet.keys, dataSet.values)));
            entityControl.createEntity(tx, TOKEN_NAME, propertyMap(dataSet.keys, dataSet.nonMatching[2]));

            found = dataSet.indexSeek.findEntities(tx, dataSet.keys, dataSet.values, entityControl);
        }
        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(name = "shouldNotIncludeEntitiesDeletedInSameTxInIndexSeek using {0} with {1} index for {2}")
    @MethodSource("data")
    public void shouldNotIncludeEntitiesDeletedInSameTxInIndexSeek(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        createIndex(withIndex, dataSet.keys, entityControl);

        // GIVEN
        createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.nonMatching[0]);
        LongSet toDelete = createEntities(
                db,
                entityControl,
                TOKEN_NAME,
                dataSet.keys,
                dataSet.values,
                dataSet.nonMatching[1],
                dataSet.nonMatching[2]);
        MutableLongSet expected = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.values);
        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            LongIterator deleting = toDelete.longIterator();
            while (deleting.hasNext()) {
                long id = deleting.next();
                entityControl.deleteEntity(tx, id);
                expected.remove(id);
            }

            found = dataSet.indexSeek.findEntities(tx, dataSet.keys, dataSet.values, entityControl);
        }
        // THEN
        assertThat(found).isEqualTo(expected);
    }

    @ParameterizedTest(name = "shouldConsiderEntitiesChangedInSameTxInIndexSeek using {0} with {1} index for {2}")
    @MethodSource("data")
    public void shouldConsiderEntitiesChangedInSameTxInIndexSeek(
            DataSet dataSet, IndexingMode withIndex, EntityControl entityControl) {
        createIndex(withIndex, dataSet.keys, entityControl);

        // GIVEN
        createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.nonMatching[0]);
        LongSet toChangeToMatch = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.nonMatching[1]);
        LongSet toChangeToNotMatch = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.values);
        MutableLongSet expected = createEntities(db, entityControl, TOKEN_NAME, dataSet.keys, dataSet.values);
        // WHEN
        LongSet found;
        try (Transaction tx = db.beginTx()) {
            LongIterator toMatching = toChangeToMatch.longIterator();
            while (toMatching.hasNext()) {
                long id = toMatching.next();
                entityControl.setProperties(tx, id, dataSet.keys, dataSet.values);
                expected.add(id);
            }
            LongIterator toNotMatching = toChangeToNotMatch.longIterator();
            while (toNotMatching.hasNext()) {
                long id = toNotMatching.next();
                entityControl.setProperties(tx, id, dataSet.keys, dataSet.nonMatching[2]);
                expected.remove(id);
            }

            found = dataSet.indexSeek.findEntities(tx, dataSet.keys, dataSet.values, entityControl);
        }
        // THEN
        assertThat(found).isEqualTo(expected);
    }

    private static MutableLongSet createEntities(
            GraphDatabaseService db,
            EntityControl entityControl,
            String label,
            String[] keys,
            Object[]... propertyValueTuples) {
        MutableLongSet expected = new LongHashSet();
        try (Transaction tx = db.beginTx()) {
            for (Object[] valueTuple : propertyValueTuples) {
                expected.add(entityControl.createEntity(tx, label, propertyMap(keys, valueTuple)));
            }
            tx.commit();
        }
        return expected;
    }

    private static Map<String, Object> propertyMap(String[] keys, Object[] valueTuple) {
        Map<String, Object> propertyValues = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            propertyValues.put(keys[i], valueTuple[i]);
        }
        return propertyValues;
    }

    private static Object[] plus(Integer[] values, int offset) {
        Object[] result = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] + offset;
        }
        return result;
    }

    private interface IndexSeek {
        LongSet findEntities(Transaction tx, String[] keys, Object[] values, EntityControl entityControl);
    }

    private static final IndexSeek biIndexSeek = (tx, keys, values, entityControl) -> {
        assert keys.length == 2;
        assert values.length == 2;
        return entityControl.findEntities(tx, TOKEN_NAME, keys[0], values[0], keys[1], values[1]);
    };

    private static final IndexSeek triIndexSeek = (tx, keys, values, entityControl) -> {
        assert keys.length == 3;
        assert values.length == 3;
        return entityControl.findEntities(tx, TOKEN_NAME, keys[0], values[0], keys[1], values[1], keys[2], values[2]);
    };

    private static final IndexSeek mapIndexSeek =
            (tx, keys, values, entityControl) -> entityControl.findEntities(tx, TOKEN_NAME, propertyMap(keys, values));

    enum IndexingMode {
        NONE, // No index, fallback to scan
        TOKEN, // NLI or RTI only
        PROPERTY_RANGE // property index
    }

    enum DataSet {
        BI_SEEK(array(2, 3), biIndexSeek),
        TRI_SEEK(array(2, 3, 4), triIndexSeek),
        MAP_SEEK(array(2, 3, 4, 5, 6), mapIndexSeek);

        String[] keys;
        Object[] values;
        Object[][] nonMatching;
        IndexSeek indexSeek;

        DataSet(Integer[] values, IndexSeek indexSeek) {
            this.values = values;
            this.indexSeek = indexSeek;
            this.nonMatching = array(plus(values, 1), plus(values, 2), plus(values, 3));
            this.keys = Arrays.stream(values).map(v -> "key" + v).toArray(String[]::new);
        }
    }

    interface EntityControl {

        String createIndex(Transaction tx, String tokenName, String[] keys, IndexType indexType);

        long createEntity(Transaction tx, String token, Map<String, Object> properties);

        void deleteEntity(Transaction tx, long id);

        void setProperties(Transaction tx, long id, String[] keys, Object[] values);

        LongSet findEntities(Transaction tx, String token, String key1, Object value1, String key2, Object value2);

        LongSet findEntities(
                Transaction tx,
                String token,
                String key1,
                Object value1,
                String key2,
                Object value2,
                String key3,
                Object value3);

        LongSet findEntities(Transaction tx, String token, Map<String, Object> propertyValues);
    }

    private enum EntityTypes implements EntityControl {
        NODE {
            @Override
            public String createIndex(Transaction tx, String tokenName, String[] keys, IndexType indexType) {
                IndexCreator indexCreator =
                        tx.schema().indexFor(Label.label(tokenName)).withIndexType(indexType);
                for (String key : keys) {
                    indexCreator = indexCreator.on(key);
                }
                IndexDefinition indexDefinition = indexCreator.create();
                return indexDefinition.getName();
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
            public void setProperties(Transaction tx, long id, String[] keys, Object[] values) {
                Node node = tx.getNodeById(id);
                for (int i = 0; i < keys.length; i++) {
                    node.setProperty(keys[i], values[i]);
                }
            }

            @Override
            public LongSet findEntities(
                    Transaction tx, String token, String key1, Object value1, String key2, Object value2) {
                return mapToIds(tx.findNodes(Label.label(token), key1, value1, key2, value2));
            }

            @Override
            public LongSet findEntities(
                    Transaction tx,
                    String token,
                    String key1,
                    Object value1,
                    String key2,
                    Object value2,
                    String key3,
                    Object value3) {
                return mapToIds(tx.findNodes(Label.label(token), key1, value1, key2, value2, key3, value3));
            }

            @Override
            public LongSet findEntities(Transaction tx, String token, Map<String, Object> propertyValues) {
                return mapToIds(tx.findNodes(Label.label(token), propertyValues));
            }
        },
        RELATIONSHIP {
            @Override
            public String createIndex(Transaction tx, String tokenName, String[] keys, IndexType indexType) {
                IndexCreator indexCreator = tx.schema()
                        .indexFor(RelationshipType.withName(tokenName))
                        .withIndexType(indexType);
                for (String key : keys) {
                    indexCreator = indexCreator.on(key);
                }
                return indexCreator.create().getName();
            }

            @Override
            public long createEntity(Transaction tx, String token, Map<String, Object> properties) {
                Node from = tx.createNode(Label.label("node"));
                Node to = tx.createNode(Label.label("node"));
                Relationship rel = from.createRelationshipTo(to, RelationshipType.withName(token));
                properties.forEach(rel::setProperty);
                return rel.getId();
            }

            @Override
            public void deleteEntity(Transaction tx, long id) {
                tx.getRelationshipById(id).delete();
            }

            @Override
            public void setProperties(Transaction tx, long id, String[] keys, Object[] values) {
                Relationship rel = tx.getRelationshipById(id);
                for (int i = 0; i < keys.length; i++) {
                    rel.setProperty(keys[i], values[i]);
                }
            }

            @Override
            public LongSet findEntities(
                    Transaction tx, String token, String key1, Object value1, String key2, Object value2) {
                return mapToIds(tx.findRelationships(RelationshipType.withName(token), key1, value1, key2, value2));
            }

            @Override
            public LongSet findEntities(
                    Transaction tx,
                    String token,
                    String key1,
                    Object value1,
                    String key2,
                    Object value2,
                    String key3,
                    Object value3) {
                return mapToIds(tx.findRelationships(
                        RelationshipType.withName(token), key1, value1, key2, value2, key3, value3));
            }

            @Override
            public LongSet findEntities(Transaction tx, String token, Map<String, Object> propertyValues) {
                return mapToIds(tx.findRelationships(RelationshipType.withName(token), propertyValues));
            }
        }
    }

    private static LongSet mapToIds(ResourceIterator<? extends Entity> nodes) {
        try (nodes) {
            MutableLongSet found = new LongHashSet();
            nodes.stream().mapToLong(Entity::getId).forEach(found::add);
            return found;
        }
    }
}
