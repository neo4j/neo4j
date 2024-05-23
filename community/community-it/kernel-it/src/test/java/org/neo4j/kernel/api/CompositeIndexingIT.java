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
package org.neo4j.kernel.api;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

@ImpermanentDbmsExtension
@Timeout(20)
class CompositeIndexingIT {

    @Inject
    private GraphDatabaseAPI graphDatabaseAPI;

    private IndexDescriptor index;
    private int labelId;
    private int relTypeId;
    private int[] propIds;

    @BeforeEach
    void setup() throws Exception {
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenWrite tokenWrite = ktx.tokenWrite();
            tokenWrite.labelGetOrCreateForName("Label0");
            labelId = tokenWrite.labelGetOrCreateForName("Label1");

            tokenWrite.relationshipTypeGetOrCreateForName("Type0");
            tokenWrite.relationshipTypeGetOrCreateForName("Type1");
            relTypeId = tokenWrite.relationshipTypeGetOrCreateForName("Type2");

            propIds = new int[10];
            for (int i = 0; i < propIds.length; i++) {
                propIds[i] = tokenWrite.propertyKeyGetOrCreateForName("prop" + i);
            }
            tx.commit();
        }
    }

    void setup(PrototypeFactory prototypeFactory) throws Exception {
        var prototype = prototypeFactory.build(labelId, relTypeId, propIds);
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            if (prototype.isUnique()) {
                ConstraintDescriptor constraint = ktx.schemaWrite().uniquePropertyConstraintCreate(prototype);
                index = ktx.schemaRead().indexGetForName(constraint.getName());
            } else {
                index = ktx.schemaWrite().indexCreate(forSchema(prototype.schema()));
            }
            tx.commit();
        }

        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
            tx.commit();
        }
    }

    @AfterEach
    void clean() throws Exception {
        if (index == null) {
            // setup no happened
            return;
        }
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            if (index.isUnique()) {
                Iterator<ConstraintDescriptor> constraints = ktx.schemaRead().constraintsGetForSchema(index.schema());
                while (constraints.hasNext()) {
                    ktx.schemaWrite().constraintDrop(constraints.next(), false);
                }
            } else {
                ktx.schemaWrite().indexDrop(index);
            }
            tx.commit();
        }

        try (Transaction tx = graphDatabaseAPI.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                Iterables.forEach(node.getRelationships(), Relationship::delete);
                node.delete();
            }
            tx.commit();
        }
    }

    private static Stream<Params> params() {
        return Stream.of(
                new Params(
                        (labelId, relTypeId, propIds) -> forSchema(forLabel(labelId, propIds[1])), EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) -> forSchema(forLabel(labelId, propIds[1], propIds[2])),
                        EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) ->
                                forSchema(forLabel(labelId, propIds[1], propIds[2], propIds[3], propIds[4])),
                        EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) -> forSchema(forLabel(
                                labelId,
                                propIds[1],
                                propIds[2],
                                propIds[3],
                                propIds[4],
                                propIds[5],
                                propIds[6],
                                propIds[7])),
                        EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) -> uniqueForSchema(forLabel(labelId, propIds[1])),
                        EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) -> uniqueForSchema(forLabel(labelId, propIds[1], propIds[2])),
                        EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) -> uniqueForSchema(forLabel(
                                labelId,
                                propIds[1],
                                propIds[2],
                                propIds[3],
                                propIds[4],
                                propIds[5],
                                propIds[6],
                                propIds[7])),
                        EntityControl.NODE),
                new Params(
                        (labelId, relTypeId, propIds) -> forSchema(forRelType(relTypeId, propIds[1])),
                        EntityControl.RELATIONSHIP),
                new Params(
                        (labelId, relTypeId, propIds) -> forSchema(forRelType(relTypeId, propIds[1], propIds[2])),
                        EntityControl.RELATIONSHIP),
                new Params(
                        (labelId, relTypeId, propIds) ->
                                forSchema(forRelType(relTypeId, propIds[1], propIds[2], propIds[3], propIds[4])),
                        EntityControl.RELATIONSHIP),
                new Params(
                        (labelId, relTypeId, propIds) -> forSchema(forRelType(
                                relTypeId,
                                propIds[1],
                                propIds[2],
                                propIds[3],
                                propIds[4],
                                propIds[5],
                                propIds[6],
                                propIds[7])),
                        EntityControl.RELATIONSHIP),
                new Params(
                        (labelId, relTypeId, propIds) -> uniqueForSchema(forRelType(relTypeId, propIds[1])),
                        EntityControl.RELATIONSHIP),
                new Params(
                        (labelId, relTypeId, propIds) -> uniqueForSchema(forRelType(relTypeId, propIds[1], propIds[2])),
                        EntityControl.RELATIONSHIP),
                new Params(
                        (labelId, relTypeId, propIds) -> uniqueForSchema(forRelType(
                                relTypeId,
                                propIds[1],
                                propIds[2],
                                propIds[3],
                                propIds[4],
                                propIds[5],
                                propIds[6],
                                propIds[7])),
                        EntityControl.RELATIONSHIP));
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldSeeEntityAddedByPropertyToIndexInTranslation(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            var entity = entityControl.createEntity(ktx, index);

            var found = entityControl.seek(ktx, index);
            assertThat(found).containsExactly(entity);
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldSeeEntityAddedByTokenToIndexInTransaction(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            var entity = entityControl.createEntityReverse(ktx, index);

            var found = entityControl.seek(ktx, index);
            assertThat(found).containsExactly(entity);
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldNotSeeEntityThatWasDeletedInTransaction(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        long entity = createEntity(entityControl);
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.deleteEntity(ktx, entity);

            assertThat(entityControl.seek(ktx, index)).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldNotSeeEntityThatHasItsTokenRemovedInTransaction(Params params) throws Exception {
        // EntityControl::removeToken not supported
        var entityControl = params.entityControl;
        if (entityControl == EntityControl.RELATIONSHIP) {
            return;
        }

        setup(params.prototypeFactory);

        long entity = createEntity(entityControl);
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.removeToken(ktx, entity, labelId);

            assertThat(entityControl.seek(ktx, index)).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldNotSeeEntityThatHasAPropertyRemovedInTransaction(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        long entity = createEntity(entityControl);
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.removeProperty(ktx, entity, index.schema().getPropertyIds()[0]);

            assertThat(entityControl.seek(ktx, index)).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldSeeAllEntitiesAddedInTransaction(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        if (!index.isUnique()) // this test does not make any sense for UNIQUE indexes
        {
            try (Transaction tx = graphDatabaseAPI.beginTx()) {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

                long entity1 = entityControl.createEntity(ktx, index);
                long entity2 = entityControl.createEntity(ktx, index);
                long entity3 = entityControl.createEntity(ktx, index);

                assertThat(entityControl.seek(ktx, index)).contains(entity1, entity2, entity3);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldSeeAllEntitiesAddedBeforeTransaction(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        if (!index.isUnique()) // this test does not make any sense for UNIQUE indexes
        {
            long entity1 = createEntity(entityControl);
            long entity2 = createEntity(entityControl);
            long entity3 = createEntity(entityControl);
            try (Transaction tx = graphDatabaseAPI.beginTx()) {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

                assertThat(entityControl.seek(ktx, index)).contains(entity1, entity2, entity3);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldNotSeeEntitiesLackingOneProperty(Params params) throws Exception {
        setup(params.prototypeFactory);
        var entityControl = params.entityControl;
        long entity = createEntity(entityControl);
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.createEntity(ktx, index, true);

            assertThat(entityControl.seek(ktx, index)).containsExactly(entity);
        }
    }

    @FunctionalInterface
    private interface PrototypeFactory {
        IndexPrototype build(int labelId, int relTypeId, int[] propIds);
    }

    private static class Params {
        PrototypeFactory prototypeFactory;
        EntityControl entityControl;

        Params(PrototypeFactory prototypeFactory, EntityControl entityControl) {
            this.prototypeFactory = prototypeFactory;
            this.entityControl = entityControl;
        }
    }

    private long createEntity(EntityControl entityControl) throws KernelException {
        long id;
        try (Transaction tx = graphDatabaseAPI.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            id = entityControl.createEntity(ktx, index);
            tx.commit();
        }
        return id;
    }

    private static PropertyIndexQuery[] exactQuery(IndexDescriptor index) {
        int[] propertyIds = index.schema().getPropertyIds();
        PropertyIndexQuery[] query = new PropertyIndexQuery[propertyIds.length];
        for (int i = 0; i < query.length; i++) {
            int propID = propertyIds[i];
            query[i] = PropertyIndexQuery.exact(propID, Values.of(propID));
        }
        return query;
    }

    enum EntityControl {
        NODE {
            @Override
            long createEntity(KernelTransaction ktx, IndexDescriptor index, boolean excludeFirstProperty)
                    throws KernelException {
                Write write = ktx.dataWrite();
                var nodeID = write.nodeCreate();
                write.nodeAddLabel(nodeID, index.schema().getLabelId());
                for (int propID : index.schema().getPropertyIds()) {
                    if (excludeFirstProperty) {
                        excludeFirstProperty = false;
                        continue;
                    }
                    write.nodeSetProperty(nodeID, propID, Values.intValue(propID));
                }
                return nodeID;
            }

            @Override
            long createEntityReverse(KernelTransaction ktx, IndexDescriptor index) throws KernelException {
                Write write = ktx.dataWrite();
                var nodeID = write.nodeCreate();
                for (int propID : index.schema().getPropertyIds()) {
                    write.nodeSetProperty(nodeID, propID, Values.intValue(propID));
                }
                write.nodeAddLabel(nodeID, index.schema().getLabelId());
                return nodeID;
            }

            @Override
            public void deleteEntity(KernelTransaction ktx, long id) throws InvalidTransactionTypeKernelException {
                ktx.dataWrite().nodeDelete(id);
            }

            @Override
            public void removeToken(KernelTransaction ktx, long entityId, int labelId)
                    throws InvalidTransactionTypeKernelException, EntityNotFoundException {
                ktx.dataWrite().nodeRemoveLabel(entityId, labelId);
            }

            @Override
            public void removeProperty(KernelTransaction ktx, long entity, int propertyId) throws KernelException {
                ktx.dataWrite().nodeRemoveProperty(entity, propertyId);
            }

            @Override
            Set<Long> seek(KernelTransaction ktx, IndexDescriptor index) throws KernelException {
                IndexReadSession indexSession = ktx.dataRead().indexReadSession(index);
                Set<Long> result = new HashSet<>();
                try (var cursor =
                        ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                    ktx.dataRead()
                            .nodeIndexSeek(
                                    ktx.queryContext(), indexSession, cursor, unconstrained(), exactQuery(index));
                    while (cursor.next()) {
                        result.add(cursor.nodeReference());
                    }
                }
                return result;
            }
        },
        RELATIONSHIP {
            @Override
            long createEntity(KernelTransaction ktx, IndexDescriptor index, boolean excludeFirstProperty)
                    throws KernelException {
                Write write = ktx.dataWrite();
                var from = write.nodeCreate();
                var to = write.nodeCreate();
                var rel = write.relationshipCreate(from, index.schema().getRelTypeId(), to);
                for (int propID : index.schema().getPropertyIds()) {
                    if (excludeFirstProperty) {
                        excludeFirstProperty = false;
                        continue;
                    }
                    write.relationshipSetProperty(rel, propID, Values.intValue(propID));
                }
                return rel;
            }

            @Override
            long createEntityReverse(KernelTransaction ktx, IndexDescriptor index) throws KernelException {
                return createEntity(ktx, index, false);
            }

            @Override
            public void deleteEntity(KernelTransaction ktx, long id) throws InvalidTransactionTypeKernelException {
                ktx.dataWrite().relationshipDelete(id);
            }

            @Override
            public void removeToken(KernelTransaction ktx, long entityId, int relTypeId) {
                throw new IllegalStateException("Not supported");
            }

            @Override
            public void removeProperty(KernelTransaction ktx, long entity, int propertyId) throws KernelException {
                ktx.dataWrite().relationshipRemoveProperty(entity, propertyId);
            }

            @Override
            Set<Long> seek(KernelTransaction ktx, IndexDescriptor index) throws KernelException {
                IndexReadSession indexSession = ktx.dataRead().indexReadSession(index);
                Set<Long> result = new HashSet<>();
                try (var cursor =
                        ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                    ktx.dataRead()
                            .relationshipIndexSeek(
                                    ktx.queryContext(), indexSession, cursor, unconstrained(), exactQuery(index));
                    while (cursor.next()) {
                        result.add(cursor.relationshipReference());
                    }
                }
                return result;
            }
        };

        long createEntity(KernelTransaction ktx, IndexDescriptor index) throws KernelException {
            return createEntity(ktx, index, false);
        }

        abstract long createEntity(KernelTransaction ktx, IndexDescriptor index, boolean excludeFirstProperty)
                throws KernelException;

        abstract long createEntityReverse(KernelTransaction ktx, IndexDescriptor index) throws KernelException;

        abstract Set<Long> seek(KernelTransaction transaction, IndexDescriptor index) throws KernelException;

        public abstract void deleteEntity(KernelTransaction ktx, long id) throws KernelException;

        public abstract void removeToken(KernelTransaction ktx, long entityId, int tokenId) throws KernelException;

        public abstract void removeProperty(KernelTransaction ktx, long entity, int propertyId) throws KernelException;
    }
}
