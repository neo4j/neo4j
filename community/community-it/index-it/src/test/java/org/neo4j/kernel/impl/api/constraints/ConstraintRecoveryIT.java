/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.internal.helpers.collection.Iterables.single;

@EphemeralTestDirectoryExtension
class ConstraintRecoveryIT
{
    private static final String KEY = "prop";
    private static final Label LABEL = Label.label( "label1" );

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;

    @Test
    void shouldHaveAvailableOrphanedConstraintIndexIfUniqueConstraintCreationFails()
    {
        // given
        File pathToDb = testDirectory.storeDir();

        TestDatabaseManagementServiceBuilder dbFactory = new TestDatabaseManagementServiceBuilder( pathToDb );
        dbFactory.setFileSystem( fs );

        final EphemeralFileSystemAbstraction[] storeInNeedOfRecovery = new EphemeralFileSystemAbstraction[1];
        final AtomicBoolean monitorCalled = new AtomicBoolean( false );

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanComplete()
            {
                monitorCalled.set( true );
                db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores()
                        .getSchemaStore().flush();
                storeInNeedOfRecovery[0] = fs.snapshot();
            }
        } );
        dbFactory.setMonitors( monitors );

        // This test relies on behaviour that is specific to the Lucene populator, where uniqueness is controlled
        // after index has been populated, which is why we're using NATIVE20 and index booleans (they end up in Lucene)
        DatabaseManagementService managementService = dbFactory.impermanent()
                .setConfig( default_schema_provider, NATIVE30.providerName() ).build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 2; i++ )
            {
                db.createNode( LABEL ).setProperty( KEY, "true" );
            }

            tx.commit();
        }

        assertThrows( ConstraintViolationException.class, () ->
        {
            try ( Transaction ignored = db.beginTx() )
            {
                db.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).create();
            }
        } );
        managementService.shutdown();

        assertTrue( monitorCalled.get() );

        // when
        dbFactory = new TestDatabaseManagementServiceBuilder( pathToDb );
        dbFactory.setFileSystem( storeInNeedOfRecovery[0] );
        DatabaseManagementService secondManagementService = dbFactory.impermanent().build();
        db = (GraphDatabaseAPI) secondManagementService.database( DEFAULT_DATABASE_NAME );

        // then
        try ( Transaction ignore = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertEquals( 2, Iterables.count( db.getAllNodes() ) );
        }

        try ( Transaction ignore = db.beginTx() )
        {
            assertEquals( 0, Iterables.count( Iterables.asList( db.schema().getConstraints() ) ) );
        }

        try ( Transaction ignore = db.beginTx() )
        {
            IndexDefinition orphanedConstraintIndex = single( db.schema().getIndexes() );
            assertEquals( LABEL.name(), single( orphanedConstraintIndex.getLabels() ).name() );
            assertEquals( KEY, single( orphanedConstraintIndex.getPropertyKeys() ) );
        }

        secondManagementService.shutdown();
    }
}
