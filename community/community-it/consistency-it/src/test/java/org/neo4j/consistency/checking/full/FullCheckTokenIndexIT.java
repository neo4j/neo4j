/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.LookupAccessorsFromRunningDb;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checker.NodeBasedMemoryLimiter;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.TokenHolders;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
public class FullCheckTokenIndexIT
{
    @Inject
    private TestDirectory testDirectory;

    private final ByteArrayOutputStream logStream = new ByteArrayOutputStream();
    private final Log4jLogProvider logProvider = new Log4jLogProvider( logStream );
    private DatabaseManagementService managementService;
    private Config config;

    @BeforeEach
    void setUp()
    {
        config = Config.newBuilder()
                .set( allow_single_automatic_upgrade, false )
                .set( allow_upgrade, true )
                .set( neo4j_home, testDirectory.homePath() )
                .set( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true ).build();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
            managementService = null;
        }
    }

    @Test
    void shouldBeConsistentWithCompleteInjectedTokenIndex() throws Throwable
    {
        // Starting an existing database on 4.3 or newer binaries should make the old label scan store
        // into a token index - but there is no schema rule for it in store until after upgrade transaction has been run
        // (there is just an injected indexRule in the cache).
        GraphDatabaseAPI database = createDatabaseOfOlderVersion();
        verifyInjectedNLIExistAndOnline( database );

        ConsistencySummaryStatistics check = check( database, config );
        assertTrue( check.isConsistent() );
    }

    @Test
    void shouldBeInconsistentWithInjectedTokenIndexNotMatchingNodeStore() throws Throwable
    {
        // Starting an existing database on 4.3 or newer binaries should make the old label scan store
        // into a token index - but there is no schema rule for it in store until after upgrade transaction has been run
        // (there is just an injected indexRule in the cache).
        GraphDatabaseAPI database = createDatabaseOfOlderVersion();
        verifyInjectedNLIExistAndOnline( database );

        // Add a node that doesn't exist to the index.
        updateNodeLabelIndex( database );

        ConsistencySummaryStatistics check = check( database, config );
        assertFalse( check.isConsistent() );
        assertThat( logStream.toString() ).contains( "refers to a node record that is not in use" );
        assertThat( check.getTotalInconsistencyCount() ).isEqualTo( 1 );
        assertThat( check.getInconsistencyCountForRecordType( RecordType.LABEL_SCAN_DOCUMENT ) ).isEqualTo( 1 );
    }

    void updateNodeLabelIndex( GraphDatabaseAPI database ) throws IOException, IndexEntryConflictException
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        IndexingService indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        IndexAccessors.IndexAccessorLookup indexAccessorLookup = new LookupAccessorsFromRunningDb( indexingService );
        IndexAccessor accessor = indexAccessorLookup.apply( IndexDescriptor.INJECTED_NLI );

        try ( IndexUpdater indexUpdater = accessor.newUpdater( IndexUpdateMode.ONLINE, CursorContext.NULL ) )
        {
            indexUpdater.process( IndexEntryUpdate.change( 100, IndexDescriptor.INJECTED_NLI, new long[0], new long[]{1} ) );
        }
    }

    private GraphDatabaseAPI createDatabaseOfOlderVersion() throws IOException
    {
        Unzip.unzip( getClass(), "4-2-data-10-nodes-rels.zip", testDirectory.homePath() );

        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() ).setConfig( config ).build();
        return (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private ConsistencySummaryStatistics check( GraphDatabaseAPI database, Config config )
            throws ConsistencyCheckIncompleteException
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        RecordStorageEngine storageEngine = dependencyResolver.resolveDependency( RecordStorageEngine.class );
        NeoStores neoStores = storageEngine.testAccessNeoStores();
        IndexingService indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        DirectStoreAccess directStoreAccess = new DirectStoreAccess( neoStores,
                dependencyResolver.resolveDependency( LabelScanStore.class ),
                dependencyResolver.resolveDependency( IndexProviderMap.class ),
                dependencyResolver.resolveDependency( TokenHolders.class ),
                dependencyResolver.resolveDependency( IndexStatisticsStore.class ),
                dependencyResolver.resolveDependency( IdGeneratorFactory.class ) );
        CountsAccessor countsStore = storageEngine.countsAccessor();
        RelationshipGroupDegreesStore groupDegreesStore = storageEngine.relationshipGroupDegreesStore();
        PageCache pageCache = dependencyResolver.resolveDependency( PageCache.class );
        IndexAccessors.IndexAccessorLookup indexAccessorLookup = new LookupAccessorsFromRunningDb( indexingService );

        FullCheck checker = new FullCheck( ProgressMonitorFactory.NONE, defaultConsistencyCheckThreadsNumber(), ConsistencyFlags.DEFAULT,
                config, false, NodeBasedMemoryLimiter.DEFAULT );
        return checker.execute( pageCache, directStoreAccess, () -> (CountsStore) countsStore, () -> groupDegreesStore, indexAccessorLookup,
                PageCacheTracer.NULL, INSTANCE, logProvider.getLog( "test" ) );
    }

    private static void awaitIndexesOnline( GraphDatabaseAPI database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 10, MINUTES );
            transaction.commit();
        }
    }

    private void verifyInjectedNLIExistAndOnline( GraphDatabaseAPI db )
    {
        awaitIndexesOnline( db );
        try ( Transaction tx = db.beginTx() )
        {
            var indexes = StreamSupport.stream( tx.schema().getIndexes().spliterator(), false )
                    .map( IndexDefinitionImpl.class::cast )
                    .map( IndexDefinitionImpl::getIndexReference )
                    .collect( Collectors.toList() );
            assertThat( indexes ).hasSize( 1 );
            assertThat( indexes.get( 0 ) ).isEqualTo( IndexDescriptor.INJECTED_NLI );
        }
    }
}
