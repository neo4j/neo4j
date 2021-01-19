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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Integer.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

@EphemeralTestDirectoryExtension
class DatabaseUpgradeTransactionIT
{
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;

    @BeforeEach
    void setUp()
    {
        restartDbms();
    }

    @AfterEach
    void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransaction() throws Exception
    {
        assertThat( KernelVersion.V4_2 ).isLessThan( KernelVersion.LATEST );

        //Given
        setKernelVersion( KernelVersion.V4_2 );
        restartDbms();
        long startTransaction = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( KernelVersion.V4_2 );

        //When
        createReadTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( KernelVersion.V4_2 );

        //When
        createWriteTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( KernelVersion.LATEST );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
    }

    @Test
    void shouldUpgradeDatabaseToLatestVersionOnFirstWriteTransactionStressTest() throws Throwable
    {
        //Given
        setKernelVersion( KernelVersion.V4_2 );
        setDbmsRuntime( DbmsRuntimeVersion.V4_2 );
        restartDbms();
        long startTransaction = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( KernelVersion.V4_2 );
        assertThat( getDbmsRuntime() ).isEqualTo( DbmsRuntimeVersion.V4_2 );

        //When
        Race race = new Race().withRandomStartDelays().withEndCondition( () -> KernelVersion.LATEST.equals( getKernelVersion() ) );
        race.addContestant( () ->
        {
            dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ).executeTransactionally( "CALL dbms.upgrade()" );
        }, 1 );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), Race.throwing( () -> {
            createWriteTransaction();
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 2 ) );
        } ) );
        race.go( 1, TimeUnit.MINUTES );

        //Then
        assertThat( getKernelVersion() ).isEqualTo( KernelVersion.LATEST );
        assertThat( getDbmsRuntime() ).isEqualTo( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
    }

    @Test
    void shouldNotUpgradePastDbmsRuntime()
    {
        //Given
        setKernelVersion( KernelVersion.V4_2 );
        restartDbms();

        setDbmsRuntime( DbmsRuntimeVersion.V4_2 );

        //When
        createWriteTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( KernelVersion.V4_2 );
    }

    private void createReadTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.getAllNodes().stream().count() ).isEqualTo( 0 );
            tx.commit();
        }
    }

    private void createWriteTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }
    }

    private void restartDbms()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
        }
        dbms = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( allow_single_automatic_upgrade, false )
                .build();
        db = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private void setKernelVersion( KernelVersion version )
    {
        MetaDataStore metaDataStore = db.getDependencyResolver().resolveDependency( MetaDataStore.class );
        metaDataStore.setKernelVersion( version, PageCursorTracer.NULL );
    }

    private KernelVersion getKernelVersion()
    {
        return db.getDependencyResolver().resolveDependency( KernelVersionRepository.class ).kernelVersion();
    }

    private void setDbmsRuntime( DbmsRuntimeVersion runtimeVersion )
    {
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        try ( var tx = system.beginTx() )
        {
            tx.findNodes( VERSION_LABEL ).stream()
                    .forEach( dbmsRuntimeNode -> dbmsRuntimeNode.setProperty( DBMS_RUNTIME_COMPONENT, runtimeVersion.getVersion() ) );
            tx.commit();
        }
        DbmsRuntimeRepository dbmsRuntimeRepository = system.getDependencyResolver().resolveDependency( DbmsRuntimeRepository.class );
        dbmsRuntimeRepository.setVersion( runtimeVersion );
    }

    private DbmsRuntimeVersion getDbmsRuntime()
    {
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        return system.getDependencyResolver().resolveDependency( DbmsRuntimeRepository.class ).getVersion();
    }

    private void assertUpgradeTransactionInOrder( KernelVersion from, KernelVersion to, long fromTxId ) throws Exception
    {
        LogicalTransactionStore lts = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        ArrayList<KernelVersion> transactionVersions = new ArrayList<>();
        ArrayList<CommittedTransactionRepresentation> transactions = new ArrayList<>();
        try ( TransactionCursor transactionCursor = lts.getTransactions( fromTxId + 1 ) )
        {
            while ( transactionCursor.next() )
            {
                CommittedTransactionRepresentation representation = transactionCursor.get();
                transactions.add( representation );
                transactionVersions.add( representation.getStartEntry().getVersion() );
            }
        }
        assertThat( transactionVersions ).hasSizeGreaterThanOrEqualTo( 2 ); //at least upgrade transaction and the triggering transaction
        assertThat( transactionVersions ).isSortedAccordingTo( Comparator.comparingInt( KernelVersion::version ) ); //Sorted means everything is in order
        assertThat( transactionVersions.get( 0 ) ).isEqualTo( from ); //First should be "from" version
        assertThat( transactionVersions.get( transactionVersions.size() - 1 ) ).isEqualTo( to ); //And last the "to" version

        CommittedTransactionRepresentation upgradeTransaction = transactions.get( transactionVersions.indexOf( to ) - 1 );
        PhysicalTransactionRepresentation physicalRep = (PhysicalTransactionRepresentation) upgradeTransaction.getTransactionRepresentation();
        physicalRep.accept( element ->
        {
            assertThat( element ).isInstanceOf( Command.MetaDataCommand.class );
            return true;
        } );
    }
}
