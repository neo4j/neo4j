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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian_3D;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84_3D;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.mockito.mock.SpatialMocks;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

@ImpermanentDbmsExtension
abstract class IndexingAcceptanceTestBase<TOKEN, ENTITY extends Entity> {
    protected static final String LONG_STRING = "a long string that has to be stored in dynamic records";

    @Inject
    protected GraphDatabaseAPI db;

    protected TOKEN TOKEN1;
    protected TOKEN TOKEN2;
    protected TOKEN TOKEN3;

    @BeforeEach
    void setupLabels(TestInfo testInfo) {
        TOKEN1 = createToken("TOKEN1-" + testInfo.getDisplayName());
        TOKEN2 = createToken("TOKEN2-" + testInfo.getDisplayName());
        TOKEN3 = createToken("TOKEN3-" + testInfo.getDisplayName());
    }

    /* This test is a bit interesting. It tests a case where we've got a property that sits in one
     * property block and the value is of a long type. So given that plus that there's an index for that
     * label/property, do an update that changes the long value into a value that requires two property blocks.
     * This is interesting because the transaction logic compares before/after views per property record and
     * not per node as a whole.
     *
     * In this case this change will be converted into one "add" and one "remove" property updates instead of
     * a single "change" property update. At the very basic level it's nice to test for this corner-case so
     * that the externally observed behavior is correct, even if this test doesn't assert anything about
     * the underlying add/remove vs. change internal details.
     */
    @Test
    void shouldInterpretPropertyAsChangedEvenIfPropertyMovesFromOneRecordToAnother() {
        // GIVEN
        long smallValue = 10L;
        long bigValue = 1L << 62;
        ENTITY entity;
        try (Transaction tx = db.beginTx()) {
            entity = createEntity(tx, TOKEN1);
            entity.setProperty("pad0", true);
            entity.setProperty("pad1", true);
            entity.setProperty("pad2", true);
            // Use a small long here which will only occupy one property block
            entity.setProperty("key", smallValue);

            tx.commit();
        }

        createIndex(db, indexType(), TOKEN1, "key");

        // WHEN
        try (Transaction tx = db.beginTx()) {
            // A big long value which will occupy two property blocks

            getEntity(tx, entity.getId()).setProperty("key", bigValue);
            tx.commit();
        }

        try (Transaction transaction = db.beginTx()) {
            // THEN
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "key", bigValue))
                    .containsOnly(entity);
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "key", smallValue))
                    .isEmpty();
        }
    }

    @Test
    void searchingForEntityByPropertyShouldWorkWithoutIndex() {
        // Given
        var entity = createEntity(db, map("name", "Hawking"), TOKEN1);

        // When
        try (Transaction transaction = db.beginTx()) {
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Hawking"))
                    .containsOnly(entity);
        }
    }

    @Test
    void searchingUsesIndexWhenItExists() {
        // Given
        var entity = createEntity(db, map("name", "Hawking"), TOKEN1);
        createIndex(db, indexType(), TOKEN1, "name");

        // When
        try (Transaction transaction = db.beginTx()) {
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Hawking"))
                    .containsOnly(entity);
        }
    }

    @Test
    void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty() {
        // When/Then
        try (Transaction transaction = db.beginTx()) {
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Hawking"))
                    .isEmpty();
        }
    }

    @Test
    void shouldSeeIndexUpdatesWhenQueryingOutsideTransaction() {
        // GIVEN
        createIndex(db, indexType(), TOKEN1, "name");
        var firstEntity = createEntity(db, map("name", "Mattias"), TOKEN1);

        // WHEN THEN
        try (Transaction transaction = db.beginTx()) {
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Mattias"))
                    .containsOnly(firstEntity);
        }
        var secondEntity = createEntity(db, map("name", "Taylor"), TOKEN1);
        try (Transaction transaction = db.beginTx()) {
            assertThat(findEntitiesByTokenAndProperty(transaction, TOKEN1, "name", "Taylor"))
                    .containsOnly(secondEntity);
        }
    }

    @Test
    void createdEntityShouldShowUpWithinTransaction() {
        // GIVEN
        createIndex(db, indexType(), TOKEN1, "name");

        // WHEN
        long sizeBeforeDelete;
        long sizeAfterDelete;
        try (Transaction tx = db.beginTx()) {
            var entity = createEntity(db, map("name", "Mattias"), TOKEN1);
            sizeBeforeDelete = count(findEntities(tx, TOKEN1, "name", "Mattias"));
            deleteEntity(tx, entity.getId());
            sizeAfterDelete = count(findEntities(tx, TOKEN1, "name", "Mattias"));
            tx.commit();
        }

        // THEN
        assertThat(sizeBeforeDelete).isOne();
        assertThat(sizeAfterDelete).isZero();
    }

    @Test
    void deletedEntityShouldShowUpWithinTransaction() {
        // GIVEN
        createIndex(db, indexType(), TOKEN1, "name");
        var entity = createEntity(db, map("name", "Mattias"), TOKEN1);

        // WHEN
        long sizeBeforeDelete;
        long sizeAfterDelete;
        try (Transaction tx = db.beginTx()) {
            sizeBeforeDelete = count(findEntities(tx, TOKEN1, "name", "Mattias"));
            deleteEntity(tx, entity.getId());
            sizeAfterDelete = count(findEntities(tx, TOKEN1, "name", "Mattias"));
            tx.commit();
        }

        // THEN
        assertThat(sizeBeforeDelete).isOne();
        assertThat(sizeAfterDelete).isZero();
    }

    @Test
    void createdEntityShouldShowUpInIndexQuery() {
        // GIVEN
        createIndex(db, indexType(), TOKEN1, "name");
        createEntity(db, map("name", "Mattias"), TOKEN1);

        // WHEN
        long sizeBeforeCreate;
        long sizeAfterCreate;
        try (Transaction transaction = db.beginTx()) {
            sizeBeforeCreate = count(findEntities(transaction, TOKEN1, "name", "Mattias"));
        }
        createEntity(db, map("name", "Mattias"), TOKEN1);
        try (Transaction transaction = db.beginTx()) {
            sizeAfterCreate = count(findEntities(transaction, TOKEN1, "name", "Mattias"));
        }

        // THEN
        assertThat(sizeBeforeCreate).isOne();
        assertThat(sizeAfterCreate).isEqualTo(2L);
    }

    @Test
    void shouldBeAbleToQuerySupportedPropertyTypes() {
        // GIVEN
        String property = "name";
        createIndex(db, indexType(), TOKEN1, property);

        // WHEN & THEN
        assertCanCreateAndFind(db, TOKEN1, property, "A String");
        assertCanCreateAndFind(db, TOKEN1, property, true);
        assertCanCreateAndFind(db, TOKEN1, property, false);
        assertCanCreateAndFind(db, TOKEN1, property, (byte) 56);
        assertCanCreateAndFind(db, TOKEN1, property, 'z');
        assertCanCreateAndFind(db, TOKEN1, property, (short) 12);
        assertCanCreateAndFind(db, TOKEN1, property, 12);
        assertCanCreateAndFind(db, TOKEN1, property, 12L);
        assertCanCreateAndFind(db, TOKEN1, property, (float) 12.);
        assertCanCreateAndFind(db, TOKEN1, property, 12.);
        assertCanCreateAndFind(db, TOKEN1, property, SpatialMocks.mockPoint(12.3, 45.6, mockWGS84()));
        assertCanCreateAndFind(db, TOKEN1, property, SpatialMocks.mockPoint(123, 456, mockCartesian()));
        assertCanCreateAndFind(db, TOKEN1, property, SpatialMocks.mockPoint(12.3, 45.6, 100.0, mockWGS84_3D()));
        assertCanCreateAndFind(db, TOKEN1, property, SpatialMocks.mockPoint(123, 456, 789, mockCartesian_3D()));
        assertCanCreateAndFind(db, TOKEN1, property, Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6));
        assertCanCreateAndFind(db, TOKEN1, property, Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 123, 456));
        assertCanCreateAndFind(
                db, TOKEN1, property, Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.3, 45.6, 100.0));
        assertCanCreateAndFind(
                db, TOKEN1, property, Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 123, 456, 789));

        assertCanCreateAndFind(db, TOKEN1, property, new String[] {"A String"});
        assertCanCreateAndFind(db, TOKEN1, property, new boolean[] {true});
        assertCanCreateAndFind(db, TOKEN1, property, new Boolean[] {false});
        assertCanCreateAndFind(db, TOKEN1, property, new byte[] {56});
        assertCanCreateAndFind(db, TOKEN1, property, new Byte[] {57});
        assertCanCreateAndFind(db, TOKEN1, property, new char[] {'a'});
        assertCanCreateAndFind(db, TOKEN1, property, new Character[] {'b'});
        assertCanCreateAndFind(db, TOKEN1, property, new short[] {12});
        assertCanCreateAndFind(db, TOKEN1, property, new Short[] {13});
        assertCanCreateAndFind(db, TOKEN1, property, new int[] {14});
        assertCanCreateAndFind(db, TOKEN1, property, new Integer[] {15});
        assertCanCreateAndFind(db, TOKEN1, property, new long[] {16L});
        assertCanCreateAndFind(db, TOKEN1, property, new Long[] {17L});
        assertCanCreateAndFind(db, TOKEN1, property, new float[] {(float) 18.});
        assertCanCreateAndFind(db, TOKEN1, property, new Float[] {(float) 19.});
        assertCanCreateAndFind(db, TOKEN1, property, new double[] {20.});
        assertCanCreateAndFind(db, TOKEN1, property, new Double[] {21.});
        assertCanCreateAndFind(db, TOKEN1, property, new Point[] {SpatialMocks.mockPoint(12.3, 45.6, mockWGS84())});
        assertCanCreateAndFind(db, TOKEN1, property, new Point[] {SpatialMocks.mockPoint(123, 456, mockCartesian())});
        assertCanCreateAndFind(
                db, TOKEN1, property, new Point[] {SpatialMocks.mockPoint(12.3, 45.6, 100.0, mockWGS84_3D())});
        assertCanCreateAndFind(
                db, TOKEN1, property, new Point[] {SpatialMocks.mockPoint(123, 456, 789, mockCartesian_3D())});
        assertCanCreateAndFind(
                db, TOKEN1, property, new PointValue[] {Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6)
                });
        assertCanCreateAndFind(
                db, TOKEN1, property, new PointValue[] {Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 123, 456)
                });
        assertCanCreateAndFind(db, TOKEN1, property, new PointValue[] {
            Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.3, 45.6, 100.0)
        });
        assertCanCreateAndFind(db, TOKEN1, property, new PointValue[] {
            Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 123, 456, 789)
        });
    }

    @Test
    void shouldRetrieveMultipleEntitiesWithSameValueFromIndex() {
        // this test was included here for now as a precondition for the following test

        // given
        createIndex(db, indexType(), TOKEN1, "name");

        ENTITY entity1;
        ENTITY entity2;
        try (Transaction tx = db.beginTx()) {
            entity1 = createEntity(tx, TOKEN1);
            entity1.setProperty("name", "Stefan");

            entity2 = createEntity(tx, TOKEN1);
            entity2.setProperty("name", "Stefan");
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            var result = findEntities(tx, TOKEN1, "name", "Stefan");
            assertEquals(asSet(entity1, entity2), asSet(result));

            tx.commit();
        }
    }

    @Test
    void shouldThrowWhenMultipleResultsForSingleEntities() {
        // given
        createIndex(db, indexType(), TOKEN1, "name");

        ENTITY entity1;
        ENTITY entity2;
        try (Transaction tx = db.beginTx()) {
            entity1 = createEntity(tx, TOKEN1);
            entity1.setProperty("name", "Stefan");

            entity2 = createEntity(tx, TOKEN1);
            entity2.setProperty("name", "Stefan");
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            var e = assertThrows(MultipleFoundException.class, () -> findEntity(tx, TOKEN1, "name", "Stefan"));
            assertThat(e).hasMessage(format(getMultipleEntitiesMessageTemplate(), TOKEN1));
        }
    }

    protected void assertCanCreateAndFind(GraphDatabaseService db, TOKEN label, String propertyKey, Object value) {
        var created = createEntity(db, map(propertyKey, value), label);

        try (Transaction tx = db.beginTx()) {
            var found = findEntity(tx, label, propertyKey, value);
            assertThat(found).isEqualTo(created);
            deleteEntity(tx, found.getId());
            tx.commit();
        }
    }

    protected abstract TOKEN createToken(String name);

    protected abstract List<ENTITY> findEntitiesByTokenAndProperty(
            Transaction tx, TOKEN label, String propertyName, Object value);

    protected abstract ENTITY createEntity(GraphDatabaseService db, Map<String, Object> properties, TOKEN label);

    protected abstract ENTITY createEntity(Transaction tx, TOKEN token);

    protected abstract void deleteEntity(Transaction tx, long id);

    protected abstract ENTITY getEntity(Transaction tx, long id);

    protected abstract IndexDefinition createIndex(
            GraphDatabaseService db, IndexType indexType, TOKEN token, String... properties);

    protected abstract ResourceIterator<ENTITY> findEntities(Transaction tx, TOKEN token, String key, Object value);

    protected abstract ENTITY findEntity(Transaction tx, TOKEN token, String key, Object value);

    protected abstract String getMultipleEntitiesMessageTemplate();

    protected abstract IndexType indexType();
}
