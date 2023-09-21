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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

// TODO VECTOR: using ephemeral fs, indexes are weird on restart
@TestDirectoryExtension
class DatabaseIndexStatsIT {
    private final Label NODE_LABEL = Label.label("Label");
    private final String PROPERTY_KEY = "prop";

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    private final AssertableLogProvider logProvider = new AssertableLogProvider(true);
    private GraphDatabaseFacade db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before() {
        startDb();
    }

    @AfterEach
    void after() {
        managementService.shutdown();
    }

    @ParameterizedTest
    @EnumSource(
            value = IndexType.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"LOOKUP", "FULLTEXT"})
    void shouldTrackIndexCreationAndQueriesSinceStart(IndexType indexType) {
        var stats = db.getDependencyResolver().resolveDependency(DatabaseIndexStats.class);
        forceReporting();
        assertThat(stats.getPopulationCount(indexType)).isEqualTo(0);
        assertThat(stats.getQueryCount(indexType)).isEqualTo(0);

        final var index = createIndex(indexType);
        forceReporting();
        assertThat(stats.getPopulationCount(indexType)).isEqualTo(1);
        assertThat(stats.getQueryCount(indexType)).isEqualTo(0);

        query(index);
        forceReporting();
        assertThat(stats.getPopulationCount(indexType)).isEqualTo(1);
        assertThat(stats.getQueryCount(indexType)).isEqualTo(1);

        query(index);
        forceReporting();
        assertThat(stats.getPopulationCount(indexType)).isEqualTo(1);
        assertThat(stats.getQueryCount(indexType)).isEqualTo(2);

        restart();
        stats = db.getDependencyResolver().resolveDependency(DatabaseIndexStats.class);
        forceReporting();
        assertThat(stats.getPopulationCount(indexType)).isEqualTo(0);
        assertThat(stats.getQueryCount(indexType)).isEqualTo(0);
    }

    private String createIndex(IndexType indexType) {
        final String indexName;
        try (final var tx = db.beginTx()) {
            indexName = tx.schema()
                    .indexFor(NODE_LABEL)
                    .on(PROPERTY_KEY)
                    .withIndexType(indexType.toPublicApi())
                    .withIndexConfiguration(IndexSettingUtil.defaultSettingsForTesting(indexType.toPublicApi()))
                    .create()
                    .getName();
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(indexName, 1, TimeUnit.MINUTES);
        }

        return indexName;
    }

    private void forceReporting() {
        final var indexingService = db.getDependencyResolver().resolveDependency(IndexingService.class);
        indexingService.reportUsageStatistics();
    }

    private void query(String indexName) {
        try (final var tx = db.beginTx()) {
            final var ktx = ((TransactionImpl) tx).kernelTransaction();
            final var index = ktx.schemaRead().indexGetForName(indexName);
            final var session = ktx.dataRead().indexReadSession(index);
            try (final var nodes = ktx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, ktx.memoryTracker())) {
                ktx.dataRead().nodeIndexScan(session, nodes, IndexQueryConstraints.unconstrained());
            }
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void startDb() {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setInternalLogProvider(logProvider)
                .setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fs))
                .setConfig(index_background_sampling_enabled, false)
                .build();
        db = (GraphDatabaseFacade) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void restart() {
        managementService.shutdown();
        startDb();
    }
}
