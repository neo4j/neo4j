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
package org.neo4j.consistency;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith( SuppressOutputExtension.class )
public class ConsistencyCheckServiceIntegrationTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private GraphStoreFixture fixture;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        fixture = new GraphStoreFixture( getRecordFormatName(), pageCache, testDirectory )
        {
            @Override
            protected void generateInitialData( GraphDatabaseService graphDb )
            {
                try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
                {
                    Node node1 = set( tx.createNode() );
                    Node node2 = set( tx.createNode(), property( "key", "exampleValue" ) );
                    node1.createRelationshipTo( node2, RelationshipType.withName( "C" ) );
                    tx.commit();
                }
            }

            @Override
            protected Map<Setting<?>,Object> getConfig()
            {
                return settings();
            }
        };
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fixture.close();
    }

    @Test
    void reportNotUsedRelationshipReferencedInChain() throws Exception
    {
        prepareDbWithDeletedRelationshipPartOfTheChain();

        Date timestamp = new Date();
        ConsistencyCheckService service = new ConsistencyCheckService( timestamp );
        Config configuration = Config.defaults( settings() );

        ConsistencyCheckService.Result result = runFullConsistencyCheck( service, configuration );

        assertFalse( result.isSuccessful() );

        File reportFile = result.reportFile();
        assertTrue( reportFile.exists(), "Consistency check report file should be generated." );
        assertThat( Files.readString( reportFile.toPath() ) ).as(
                "Expected to see report about not deleted relationship record present as part of a chain" ).contains(
                "The relationship record is not in use, but referenced from relationships chain." );
    }

    @Test
    void shouldFailOnDatabaseInNeedOfRecovery() throws IOException
    {
        nonRecoveredDatabase();
        ConsistencyCheckService service = new ConsistencyCheckService();
        try
        {
            Config defaults = Config.defaults( settings() );
            runFullConsistencyCheck( service, defaults );
            fail();
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            assertEquals( e.getCause().getMessage(),
                    Strings.joinAsLines( "Active logical log detected, this might be a source of inconsistencies.", "Please recover database.",
                            "To perform recovery please start database in single mode and perform clean shutdown." ) );
        }
    }

    @Test
    void ableToDeleteDatabaseDirectoryAfterConsistencyCheckRun() throws ConsistencyCheckIncompleteException, IOException
    {
        prepareDbWithDeletedRelationshipPartOfTheChain();
        ConsistencyCheckService service = new ConsistencyCheckService();
        Result consistencyCheck = runFullConsistencyCheck( service, Config.defaults( settings() ) );
        assertFalse( consistencyCheck.isSuccessful() );
        // using commons file utils since they do not forgive not closed file descriptors on windows
        org.apache.commons.io.FileUtils.deleteDirectory( fixture.databaseLayout().databaseDirectory() );
    }

    @Test
    void shouldSucceedIfStoreIsConsistent() throws Exception
    {
        // given
        Date timestamp = new Date();
        ConsistencyCheckService service = new ConsistencyCheckService( timestamp );
        Config configuration = Config.defaults( settings() );

        // when
        ConsistencyCheckService.Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertTrue( result.isSuccessful() );
        File reportFile = result.reportFile();
        assertFalse( reportFile.exists(), "Unexpected generation of consistency check report file: " + reportFile );
    }

    @Test
    void shouldFailIfTheStoreInNotConsistent() throws Exception
    {
        // given
        breakNodeStore();
        Date timestamp = new Date();
        ConsistencyCheckService service = new ConsistencyCheckService( timestamp );
        Path logsDir = testDirectory.homeDir().toPath();
        Config configuration = Config.newBuilder()
                .set( settings() )
                .set( GraphDatabaseSettings.logs_directory, logsDir )
                .build();

        // when
        ConsistencyCheckService.Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertFalse( result.isSuccessful() );
        String reportFile = format( "inconsistencies-%s.report",
                new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( timestamp ) );
        assertEquals( new File( logsDir.toString(), reportFile ), result.reportFile() );
        assertTrue( result.reportFile().exists(), "Inconsistency report file not generated" );
    }

    @Test
    void shouldNotReportDuplicateForHugeLongValues() throws Exception
    {
        // given
        ConsistencyCheckService service = new ConsistencyCheckService();
        Config configuration = Config.defaults( settings() );
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setConfig( settings() ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        String propertyKey = "itemId";
        Label label = Label.label( "Item" );
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            set( tx.createNode( label ), property( propertyKey, 973305894188596880L ) );
            set( tx.createNode( label ), property( propertyKey, 973305894188596864L ) );
            tx.commit();
        }
        managementService.shutdown();

        // when
        Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertTrue( result.isSuccessful() );
    }

    @Test
    void shouldReportMissingSchemaIndex() throws Exception
    {
        // given

        GraphDatabaseService gds = getGraphDatabaseService( testDirectory.homeDir() );

        Label label = Label.label( "label" );
        String propKey = "propKey";
        createIndex( gds, label, propKey );

        managementService.shutdown();

        // when
        File schemaDir = findFile( databaseLayout, "schema" );
        FileUtils.deleteRecursively( schemaDir );

        ConsistencyCheckService service = new ConsistencyCheckService();
        Config configuration = Config.defaults( settings() );
        Result result = runFullConsistencyCheck( service, configuration, databaseLayout );

        // then
        assertTrue( result.isSuccessful() );
        File reportFile = result.reportFile();
        assertTrue( reportFile.exists(), "Consistency check report file should be generated." );
        assertThat( Files.readString( reportFile.toPath() ) ).as( "Expected to see report about schema index not being online" ).contains(
                "schema rule" ).contains( "not online" );
    }

    @Test
    void oldLuceneSchemaIndexShouldBeConsideredConsistentWithFusionProvider() throws Exception
    {
        Label label = Label.label( "label" );
        String propKey = "propKey";

        // Given a lucene index
        GraphDatabaseService db =
                getGraphDatabaseService( databaseLayout.databaseDirectory(), Map.of( GraphDatabaseSettings.default_schema_provider, NATIVE30.providerName() ) );
        createIndex( db, label, propKey );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label ).setProperty( propKey, 1 );
            tx.createNode( label ).setProperty( propKey, "string" );
            tx.commit();
        }
        managementService.shutdown();

        ConsistencyCheckService service = new ConsistencyCheckService();
        Config configuration = Config.newBuilder()
                .set( settings() )
                .set( GraphDatabaseSettings.default_schema_provider, NATIVE_BTREE10.providerName() )
                .build();
        Result result = runFullConsistencyCheck( service, configuration, databaseLayout );
        assertTrue( result.isSuccessful() );
    }

    private static void createIndex( GraphDatabaseService gds, Label label, String propKey )
    {
        IndexDefinition indexDefinition;

        try ( Transaction tx = gds.beginTx() )
        {
            indexDefinition = tx.schema().indexFor( label ).on( propKey ).create();
            tx.commit();
        }

        try ( Transaction tx = gds.beginTx() )
        {
            tx.schema().awaitIndexOnline( indexDefinition, 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private static File findFile( DatabaseLayout databaseLayout, String targetFile )
    {
        File file = databaseLayout.file( targetFile );
        if ( !file.exists() )
        {
            fail( "Could not find file " + targetFile );
        }
        return file;
    }

    private GraphDatabaseService getGraphDatabaseService( File homeDir )
    {
        return getGraphDatabaseService( homeDir, Map.of() );
    }

    private GraphDatabaseService getGraphDatabaseService( File homeDir, Map<Setting<?>, Object> settings )
    {
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( homeDir );
        builder.setConfig( settings() );
        builder.setConfig( settings );

        managementService = builder.build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void prepareDbWithDeletedRelationshipPartOfTheChain()
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setConfig( record_format, getRecordFormatName() ).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {

            RelationshipType relationshipType = RelationshipType.withName( "testRelationshipType" );
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = set( tx.createNode() );
                Node node2 = set( tx.createNode(), property( "key", "value" ) );
                node1.createRelationshipTo( node2, relationshipType );
                node1.createRelationshipTo( node2, relationshipType );
                node1.createRelationshipTo( node2, relationshipType );
                node1.createRelationshipTo( node2, relationshipType );
                node1.createRelationshipTo( node2, relationshipType );
                node1.createRelationshipTo( node2, relationshipType );
                tx.commit();
            }

            RecordStorageEngine recordStorageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );

            NeoStores neoStores = recordStorageEngine.testAccessNeoStores();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
            RelationshipRecord record = relationshipStore.getRecord( 4, relationshipRecord, RecordLoad.FORCE );
            record.setInUse( false );
            relationshipStore.updateRecord( relationshipRecord );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private void nonRecoveredDatabase() throws IOException
    {
        File tmpLogDir = new File( testDirectory.homeDir(), "logs" );
        fs.mkdir( tmpLogDir );
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setConfig( settings() ).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );

        RelationshipType relationshipType = RelationshipType.withName( "testRelationshipType" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = set( tx.createNode() );
            Node node2 = set( tx.createNode(), property( "key", "value" ) );
            node1.createRelationshipTo( node2, relationshipType );
            tx.commit();
        }
        File[] txLogs = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fs ).build().logFiles();
        for ( File file : txLogs )
        {
            fs.copyToDirectory( file, tmpLogDir );
        }
        managementService.shutdown();
        for ( File txLog : txLogs )
        {
            fs.deleteFile( txLog );
        }

        for ( File file : LogFilesBuilder.logFilesBasedOnlyBuilder( tmpLogDir, fs ).build().logFiles() )
        {
            fs.moveToDirectory( file, databaseLayout.getTransactionLogsDirectory() );
        }
    }

    protected Map<Setting<?>,Object> settings()
    {
        Map<Setting<?>, Object> defaults = new HashMap<>();
        defaults.put( GraphDatabaseSettings.pagecache_memory, "8m" );
        defaults.put( GraphDatabaseSettings.logs_directory, databaseLayout.databaseDirectory().toPath() );
        defaults.put( record_format, getRecordFormatName() );
        return defaults;
    }

    private void breakNodeStore() throws KernelException
    {
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), false, next.relationship(), -1 ) );
            }
        } );
    }

    private Result runFullConsistencyCheck( ConsistencyCheckService service, Config configuration )
            throws ConsistencyCheckIncompleteException
    {
        return runFullConsistencyCheck( service, configuration, fixture.databaseLayout() );
    }

    private static Result runFullConsistencyCheck( ConsistencyCheckService service, Config configuration, DatabaseLayout databaseLayout )
            throws ConsistencyCheckIncompleteException
    {
        return service.runFullConsistencyCheck( databaseLayout,
                configuration, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false );
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }
}
