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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.index.SetInitialStateInNativeIndex;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;
import static org.neo4j.test.TestLabels.LABEL_ONE;

@Neo4jLayoutExtension
public class IndexCleanupIT
{
    private static final String propertyKey = "key";

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;
    private DatabaseManagementService managementService;
    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void mustClearIndexDirectoryOnDropWhileOnline( SchemaIndex schemaIndex ) throws IOException
    {
        configureDb( schemaIndex );
        createIndex( db, true );

        Path[] providerDirectories = providerDirectories( fs, db );
        for ( Path providerDirectory : providerDirectories )
        {
            assertTrue( fs.listFiles( providerDirectory ).length > 0, "expected there to be at least one index per existing provider map" );
        }

        dropAllIndexes();

        assertNoIndexFilesExisting( providerDirectories );
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void mustClearIndexDirectoryOnDropWhileFailed( SchemaIndex schemaIndex ) throws IOException
    {
        configureDb( schemaIndex );
        createIndex( db, true );
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( schemaIndex.providerKey(), schemaIndex.providerVersion() );
        SetInitialStateInNativeIndex setInitialStateInNativeIndex = new SetInitialStateInNativeIndex( BYTE_FAILED, providerDescriptor );
        restartDatabase( schemaIndex, setInitialStateInNativeIndex );
        // Index should be failed at this point

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                // ignore the lookup indexes which are there by default and have nothing to do with this test
                if ( index.getIndexType() == IndexType.LOOKUP )
                {
                    continue;
                }
                IndexState indexState = tx.schema().getIndexState( index );
                assertEquals( IndexState.FAILED, indexState, "expected index state to be " + IndexState.FAILED );
            }
            tx.commit();
        }

        // when
        dropAllIndexes();

        // then
        assertNoIndexFilesExisting( providerDirectories( fs, db ) );
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void mustClearIndexDirectoryOnDropWhilePopulating( SchemaIndex schemaIndex ) throws InterruptedException, IOException
    {
        // given
        Barrier.Control midPopulation = new Barrier.Control();
        IndexingService.MonitorAdapter trappingMonitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanStarting()
            {
                midPopulation.reached();
            }
        };
        configureDb( schemaIndex );
        createSomeData();
        Monitors monitors = db.getDependencyResolver().resolveDependency( Monitors.class );
        monitors.addMonitorListener( trappingMonitor );
        createIndex( db, false );

        midPopulation.await();
        Path[] providerDirectories = providerDirectories( fs, db );
        for ( Path providerDirectory : providerDirectories )
        {
            assertTrue( fs.listFiles( providerDirectory ).length > 0, "expected there to be at least one index per existing provider map" );
        }

        // when
        dropAllIndexes();
        midPopulation.release();

        assertNoIndexFilesExisting( providerDirectories );
    }

    private void assertNoIndexFilesExisting( Path[] providerDirectories ) throws IOException
    {
        for ( Path providerDirectory : providerDirectories )
        {
            assertEquals( 0, fs.listFiles( providerDirectory ).length, "expected there to be no index files" );
        }
    }

    private void dropAllIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    private void restartDatabase( SchemaIndex index, SetInitialStateInNativeIndex action ) throws IOException
    {
        managementService.shutdown();
        action.run( fs, databaseLayout );
        configureDb( index );
    }

    private void configureDb( SchemaIndex schemaIndex )
    {
        managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setConfig( default_schema_provider, schemaIndex.providerName() ).build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void createIndex( GraphDatabaseService db, boolean awaitOnline )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition indexDefinition = tx.schema().indexFor( LABEL_ONE ).on( propertyKey ).create();
            tx.commit();
        }
        if ( awaitOnline )
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
                tx.commit();
            }
        }
    }

    private static Path[] providerDirectories( FileSystemAbstraction fs, GraphDatabaseAPI db ) throws IOException
    {
        DatabaseLayout databaseLayout = db.databaseLayout();
        Path dbDir = databaseLayout.databaseDirectory();
        Path schemaDir = dbDir.resolve( "schema" );
        Path indexDir = schemaDir.resolve( "index" );
        return fs.listFiles( indexDir );
    }

    private void createSomeData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().setProperty( propertyKey, "abc" );
            tx.commit();
        }
    }
}
