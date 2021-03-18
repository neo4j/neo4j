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
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.kernel.KernelVersion.LATEST;
import static org.neo4j.kernel.KernelVersion.V4_2;

@TestDirectoryExtension
class TokenIndexesOnDatabaseUpgradeTransactionIT
{
    @Inject
    private TestDirectory testDirectory;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;

    private static final IndexPrototype prototypeNLI =
            IndexPrototype.forSchema( SchemaDescriptor.forAllEntityTokens( EntityType.NODE ), TokenIndexProvider.DESCRIPTOR )
                    .withName( IndexDescriptor.NLI_GENERATED_NAME ).withIndexType( IndexType.LOOKUP );
    private static final IndexDescriptor expectedInjectedNLI = prototypeNLI.materialise( IndexDescriptor.INJECTED_NLI_ID );

    @AfterEach
    void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldHaveInjectedNLIOnOldVersion() throws Exception
    {
        prepareDbms( "4-2-data-empty.zip" );

        verifyInjectedNLIExist();
    }

    @Test
    void shouldHaveInjectedNLIOnOldVersionWhenSchemaStoreIsNotEmpty() throws Exception
    {
        prepareDbms( "4-2-data-one-index.zip" );

        verifyInjectedNLIExist();
    }

    @Test
    void shouldHaveReplacedInjectedNLIAfterUpgrade() throws Exception
    {
        prepareDbms( "4-2-data-empty.zip" );

        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME );
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            assertThat( indexState ).isEqualTo( Schema.IndexState.ONLINE );
        }

        upgradeDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinitionImpl indexByName = (IndexDefinitionImpl) tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME );
            assertThat( indexByName.getIndexReference() ).isEqualTo( prototypeNLI.materialise( 1 ) );
            assertThat( Iterators.count( tx.schema().getIndexes().iterator() ) ).isOne();
            assertThat( tx.schema().getIndexState( indexByName ) ).isEqualTo( Schema.IndexState.ONLINE );
        }
    }

    @Test
    void proxyCorrectlySwitchedOnUpgradeToRealNLI() throws Exception
    {
        prepareDbms( "4-2-data-empty.zip" );

        IndexingService indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        IndexProxy indexProxyBefore = indexingService.getIndexProxy( expectedInjectedNLI );

        upgradeDatabase();

        IndexDescriptor indexDescriptor;
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinitionImpl indexByName = (IndexDefinitionImpl) tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME );
            indexDescriptor = indexByName.getIndexReference();
        }

        IndexProxy indexProxyAfter = indexingService.getIndexProxy( indexDescriptor );
        assertThat( indexProxyBefore ).isEqualTo( indexProxyAfter );
        assertThat( indexProxyAfter.getDescriptor() ).isEqualTo( indexDescriptor );
        assertThrows( IndexNotFoundKernelException.class, () -> indexingService.getIndexProxy( expectedInjectedNLI ) );
    }

    @Test
    void NLIShouldBePersistedToStoreAfterUpgrade() throws Exception
    {
        prepareDbms( "4-2-data-empty.zip" );

        upgradeDatabase();

        IndexDefinitionImpl indexByName;
        try ( Transaction tx = db.beginTx() )
        {
            indexByName = (IndexDefinitionImpl) tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME );
            assertThat( indexByName.getIndexReference() ).isEqualTo( prototypeNLI.materialise( 1 ) );
            assertThat( Iterators.count( tx.schema().getIndexes().iterator() ) ).isOne();
           assertThat( tx.schema().getIndexState( indexByName ) ).isEqualTo( Schema.IndexState.ONLINE );
        }

        restartDbms();

        // Index should still be there with same id as before restart
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinitionImpl indexAfterRestart = (IndexDefinitionImpl) tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME );
            assertThat( indexByName ).isEqualTo( indexAfterRestart );
            assertThat( Iterators.count( tx.schema().getIndexes().iterator() ) ).isOne();
            assertThat( tx.schema().getIndexState( indexAfterRestart ) ).isEqualTo( Schema.IndexState.ONLINE );
        }
    }

    @Test
    void shouldHaveNotInjectedNLIOnNewVersion()
    {
        restartDbms();

        try ( Transaction tx = db.beginTx() )
        {
            assertThatThrownBy( () -> tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME ) )
                    .isInstanceOf( IllegalArgumentException.class ).hasMessageContaining( "No index found with the name" );
        }
    }

    @Test
    void dbShouldNotStartWhenIndexWithReservedNameIsPresent() throws IOException
    {
        prepareDbms( "4-2-data-illegal-index-name.zip" );

        // The simplest way how to verify Db availability is trying to start a transaction
        var e = assertThrows( DatabaseShutdownException.class, () -> db.beginTx() );
        assertThat( e ).hasRootCauseMessage( "Index 'Index( id=2, name='__org_neo4j_schema_index_label_scan_store_converted_to_token_index', " +
                "type='GENERAL BTREE', schema=(:Person {age}), indexProvider='native-btree-1.0' )' is using a reserved name: " +
                "'__org_neo4j_schema_index_label_scan_store_converted_to_token_index'. This index must be removed on an earlier version to be able to use " +
                "binaries for version 4.3 or newer." );
    }

    @Test
    void shouldBeBlockedFromCreatingIndexWithReservedNameOnOldVersion() throws IOException
    {
        prepareDbms( "4-2-data-empty.zip" );

        // The injected NLI will exist and block the reserved name
        ConstraintViolationException e = assertThrows( ConstraintViolationException.class, this::createIndexWithReservedName );
        assertThat( e ).hasMessageContaining( "There already exists an index called" );
    }

    @Test
    void shouldBeBlockedFromCreatingIndexWithReservedNameOnNewVersion()
    {
        restartDbms();

        // No NLI with the reserved name, but we should still be blocked from using it
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class, this::createIndexWithReservedName );
        assertThat( e ).hasMessageContaining( "is a reserved name" );
    }

    @Test
    void shouldBeBlockedFromCreatingIndexWithReservedNameIfIndexExist() throws Exception
    {
        prepareDbms( "4-2-data-empty.zip" );

        upgradeDatabase();

        // The persisted NLI will exist and block the reserved name
        ConstraintViolationException e = assertThrows( ConstraintViolationException.class, this::createIndexWithReservedName );
        assertThat( e ).hasMessageContaining( "There already exists an index called" );
    }

    private void prepareDbms( String databaseArchiveName ) throws IOException
    {
        Unzip.unzip( getClass(), databaseArchiveName, testDirectory.homePath() );
        restartDbms();
        if ( db.isAvailable( 0 ) )
        {
            assertThat( getKernelVersion() ).isEqualTo( V4_2 );
        }
    }

    private void verifyInjectedNLIExist()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinitionImpl indexByName = (IndexDefinitionImpl) tx.schema().getIndexByName( IndexDescriptor.NLI_GENERATED_NAME );
            assertThat( indexByName.getIndexReference() ).isEqualTo( expectedInjectedNLI );
            tx.commit();
        }
    }

    private void createIndexWithReservedName()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( "label" ) ).on( "prop" ).withName( IndexDescriptor.NLI_GENERATED_NAME ).create();
            tx.commit();
        }
    }

    private void upgradeDatabase()
    {
        //Then
        assertThat( getKernelVersion() ).isEqualTo( V4_2 );
        setDbmsRuntime( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );

        //When
        createWriteTransaction();

        //Then
        assertThat( getKernelVersion() ).isEqualTo( LATEST );
    }

    private long createWriteTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = tx.createNode().getId();
            tx.commit();
            return nodeId;
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
                .setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true )
                .setConfig( allow_upgrade, true )
                .setInternalLogProvider( logProvider )
                .build();
        db = (GraphDatabaseAPI) dbms.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
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
    }
}
