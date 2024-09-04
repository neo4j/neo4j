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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.longValue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class IndexingServiceIntegrationTest {
    private static final String FOOD_LABEL = "food";
    private static final String CLOTHES_LABEL = "clothes";
    private static final String WEATHER_LABEL = "weather";
    private static final String PROPERTY_NAME = "name";
    private static final int NUMBER_OF_NODES = 100;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private GraphDatabaseService database;
    private DatabaseLayout layout;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() {
        startDbms();
        createData(database);
    }

    @AfterEach
    void tearDown() {
        managementService.shutdown();
    }

    @Test
    void tracePageCacheAccessOnIndexUpdatesApply() throws KernelException {
        var marker = Label.label("marker");
        var propertyName = "property";
        var testConstraint = "testConstraint";
        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .constraintFor(marker)
                    .withName(testConstraint)
                    .assertPropertyIsUnique(propertyName)
                    .create();
            transaction.commit();
        }

        var dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        var indexingService = dependencyResolver.resolveDependency(IndexingService.class);
        var contextFactory = dependencyResolver.resolveDependency(CursorContextFactory.class);

        try (Transaction transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var indexDescriptor = kernelTransaction.schemaRead().indexGetForName(testConstraint);
            try (var cursorContext = contextFactory.create("tracePageCacheAccessOnIndexUpdatesApply")) {
                Iterable<IndexEntryUpdate<IndexDescriptor>> updates = List.of(add(1, indexDescriptor, longValue(4)));
                indexingService.applyUpdates(updates, cursorContext, false);

                PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
                assertEquals(4L, cursorTracer.pins());
                assertEquals(4L, cursorTracer.unpins());
                assertEquals(2L, cursorTracer.hits());
                assertEquals(2L, cursorTracer.faults());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"LOOKUP"})
    void testManualIndexPopulation(IndexType indexType) throws InterruptedException, IndexNotFoundKernelException {
        IndexDescriptor index;
        try (Transaction tx = database.beginTx()) {
            IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) tx.schema()
                    .indexFor(Label.label(FOOD_LABEL))
                    .on(PROPERTY_NAME)
                    .withIndexType(indexType)
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(indexType))
                    .create();
            index = indexDefinition.getIndexReference();
            tx.commit();
        }

        IndexingService indexingService = getIndexingService(database);
        IndexProxy indexProxy = indexingService.getIndexProxy(index);

        waitIndexOnline(indexProxy);
        assertEquals(ONLINE, indexProxy.getState());
        PopulationProgress progress = indexProxy.getIndexPopulationProgress();
        assertEquals(progress.getCompleted(), progress.getTotal());
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"LOOKUP", "VECTOR"})
    void testManualRelationshipIndexPopulation(IndexType indexType) throws Exception {
        IndexDescriptor index;
        try (Transaction tx = database.beginTx()) {
            IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) tx.schema()
                    .indexFor(RelationshipType.withName(FOOD_LABEL))
                    .on(PROPERTY_NAME)
                    .withIndexType(indexType)
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(indexType))
                    .create();
            index = indexDefinition.getIndexReference();
            tx.commit();
        }

        IndexingService indexingService = getIndexingService(database);
        IndexProxy indexProxy = indexingService.getIndexProxy(index);

        waitIndexOnline(indexProxy);
        assertEquals(ONLINE, indexProxy.getState());
        PopulationProgress progress = indexProxy.getIndexPopulationProgress();
        assertEquals(progress.getCompleted(), progress.getTotal());
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"LOOKUP"})
    void testSchemaIndexMatchIndexingService(IndexType indexType) throws IndexNotFoundKernelException {
        String constraintName = "MyConstraint";
        String indexName = "MyIndex";
        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .constraintFor(Label.label(CLOTHES_LABEL))
                    .assertPropertyIsUnique(PROPERTY_NAME)
                    .withName(constraintName)
                    .create();
            transaction
                    .schema()
                    .indexFor(Label.label(WEATHER_LABEL))
                    .on(PROPERTY_NAME)
                    .withIndexType(indexType)
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(indexType))
                    .withName(indexName)
                    .create();

            transaction.commit();
        }

        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }

        IndexingService indexingService = getIndexingService(database);

        IndexProxy clothesIndex = indexingService.getIndexProxy(getIndexByName(constraintName));
        IndexProxy weatherIndex = indexingService.getIndexProxy(getIndexByName(indexName));
        assertEquals(ONLINE, clothesIndex.getState());
        assertEquals(ONLINE, weatherIndex.getState());
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"LOOKUP"})
    void dropIndexDirectlyOnIndexingServiceRaceWithCheckpoint(IndexType indexType) throws Throwable {
        IndexingService indexingService = getIndexingService(database);
        CheckPointer checkPointer = getCheckPointer(database);

        IndexDescriptor indexDescriptor;
        try (Transaction tx = database.beginTx()) {
            IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) tx.schema()
                    .indexFor(Label.label("label"))
                    .on("prop")
                    .withIndexType(indexType)
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(indexType))
                    .create();
            indexDescriptor = indexDefinition.getIndexReference();
            tx.commit();
        }
        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }

        Race race = new Race();
        race.addContestant(Race.throwing(() -> checkPointer.forceCheckPoint(new SimpleTriggerInfo("Test force"))));

        race.addContestant(Race.throwing(() -> indexingService.dropIndex(indexDescriptor)));
        race.go();
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"LOOKUP"})
    void dropIndexRaceWithCheckpoint(IndexType indexType) throws Throwable {
        CheckPointer checkPointer = getCheckPointer(database);

        int nbrOfIndexes = 10;
        try (Transaction tx = database.beginTx()) {
            for (int i = 0; i < nbrOfIndexes; i++) {
                tx.schema()
                        .indexFor(Label.label("label"))
                        .on("prop" + i)
                        .withIndexType(indexType)
                        .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(indexType))
                        .create();
            }
            tx.commit();
        }
        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }

        AtomicBoolean allIndexesDropped = new AtomicBoolean();
        Race race = new Race();
        race.addContestant(Race.throwing(() -> {
            while (!allIndexesDropped.get()) {
                checkPointer.forceCheckPoint(new SimpleTriggerInfo("Test force"));
            }
        }));
        race.addContestant(Race.throwing(() -> {
            try (Transaction tx = database.beginTx()) {
                tx.schema().getIndexes().forEach(IndexDefinition::drop);
                tx.commit();
            } finally {
                allIndexesDropped.set(true);
            }
        }));
        race.go();
    }

    @Test
    void testOnlineOrphanUniquenessIndexOnRestart() throws Exception {
        IndexDescriptor index = createOrphanUniquenessConstraintIndex();

        // Should end up in tentative state after population.
        IndexingService indexingService = getIndexingService(database);
        awaitStoreScan(indexingService, index);
        assertThat(getIndexState(indexingService, index)).isEqualTo(POPULATING);

        // Should still be in tentative state after restart.
        stopDbms();
        startDbms();
        indexingService = getIndexingService(database);
        assertThat(getIndexState(indexingService, index)).isEqualTo(POPULATING);

        // Should be brought online on activation.
        indexingService.activateIndex(index);
        assertThat(getIndexState(indexingService, index)).isEqualTo(ONLINE);
    }

    @Test
    void testRebuildingOrphanUniquenessIndexOnRestart() throws Exception {
        IndexDescriptor index = createOrphanUniquenessConstraintIndex();

        stopDbms();
        deleteIndexFiles(index);
        startDbms();

        // Should end up in tentative state after re-population.
        IndexingService indexingService = getIndexingService(database);
        awaitStoreScan(indexingService, index);
        assertThat(getIndexState(indexingService, index)).isEqualTo(POPULATING);

        // Should be brought online on activation.
        indexingService.activateIndex(index);
        assertThat(getIndexState(indexingService, index)).isEqualTo(ONLINE);
    }

    private IndexDescriptor createOrphanUniquenessConstraintIndex() throws KernelException {
        IndexDescriptor index;
        try (Transaction tx = database.beginTx()) {
            final var ktx = ((InternalTransaction) tx).kernelTransaction();
            final int labelId = ktx.tokenRead().nodeLabel(FOOD_LABEL);
            final int propertyKeyId = ktx.tokenRead().propertyKey(PROPERTY_NAME);

            final IndexPrototype uniqueIndex = uniqueForSchema(forLabel(labelId, propertyKeyId))
                    .withIndexProvider(AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                    .withName("constraint");
            index = ktx.indexUniqueCreate(uniqueIndex);
            tx.commit();
        }
        return index;
    }

    private void deleteIndexFiles(IndexDescriptor index) throws IOException {
        final var directoryStructure = IndexDirectoryStructure.directoriesByProvider(layout.databaseDirectory())
                .forProvider(index.getIndexProvider());
        final var path = directoryStructure.directoryForIndex(index.getId());
        fs.deleteRecursively(path);
    }

    private void stopDbms() {
        managementService.shutdown();
    }

    private void startDbms() {
        managementService = new TestDatabaseManagementServiceBuilder(directory.homePath()).build();
        database = managementService.database(DEFAULT_DATABASE_NAME);
        layout = ((GraphDatabaseFacade) database).databaseLayout();
    }

    private InternalIndexState getIndexState(IndexingService service, IndexDescriptor index)
            throws IndexNotFoundKernelException {
        return service.getIndexProxy(index).getState();
    }

    private void awaitStoreScan(IndexingService service, IndexDescriptor index)
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException, InterruptedException {
        service.getIndexProxy(index).awaitStoreScanCompleted(10, TimeUnit.MINUTES);
    }

    private static void waitIndexOnline(IndexProxy indexProxy) throws InterruptedException {
        while (ONLINE != indexProxy.getState()) {
            Thread.sleep(10);
        }
    }

    private static IndexingService getIndexingService(GraphDatabaseService database) {
        return getDependencyResolver(database).resolveDependency(IndexingService.class);
    }

    private static CheckPointer getCheckPointer(GraphDatabaseService database) {
        return getDependencyResolver(database).resolveDependency(CheckPointer.class);
    }

    private static DependencyResolver getDependencyResolver(GraphDatabaseService database) {
        return ((GraphDatabaseAPI) database).getDependencyResolver();
    }

    private static void createData(GraphDatabaseService database) {
        for (int i = 0; i < NUMBER_OF_NODES; i++) {
            try (Transaction transaction = database.beginTx()) {
                Node node = transaction.createNode(
                        Label.label(FOOD_LABEL), Label.label(CLOTHES_LABEL), Label.label(WEATHER_LABEL));
                node.setProperty(PROPERTY_NAME, "Node" + i);
                Relationship relationship = node.createRelationshipTo(node, RelationshipType.withName(FOOD_LABEL));
                relationship.setProperty(PROPERTY_NAME, "Relationship" + i);
                transaction.commit();
            }
        }
    }

    private IndexDescriptor getIndexByName(String name) {
        try (Transaction tx = database.beginTx()) {
            KernelTransaction transaction = ((InternalTransaction) tx).kernelTransaction();
            return transaction.schemaRead().indexGetForName(name);
        }
    }
}
