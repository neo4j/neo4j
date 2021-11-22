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
package org.neo4j.consistency;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

@TestDirectoryExtension
@Neo4jLayoutExtension
public class ConsistencyCheckServiceIntegrationTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private GraphStoreFixture fixture;

    @BeforeEach
    void setUp()
    {
        fixture = new GraphStoreFixture( getRecordFormatName(), testDirectory )
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
    void tearDown()
    {
        fixture.close();
    }

    @Test
    void reportNotUsedRelationshipReferencedInChain() throws Exception
    {
        prepareDbWithDeletedRelationshipPartOfTheChain();

        ConsistencyCheckService.Result result = consistencyCheckService().runFullConsistencyCheck();

        assertFalse( result.isSuccessful() );

        Path reportFile = result.reportFile();
        assertTrue( exists( reportFile ), "Consistency check report file should be generated." );
        assertThat( Files.readString( reportFile ) ).as(
                "Expected to see report about not deleted relationship record present as part of a chain" ).contains(
                "The relationship record is not in use, but referenced from relationships chain." );
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck() throws ConsistencyCheckIncompleteException
    {
        prepareDbWithDeletedRelationshipPartOfTheChain();
        var pageCacheTracer = new DefaultPageCacheTracer();
        fixture.close();
        JobScheduler jobScheduler = JobSchedulerFactory.createScheduler();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( testDirectory.getFileSystem(),
                Config.defaults( GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes( 8 ) ), pageCacheTracer, NullLog.getInstance(), jobScheduler,
                Clocks.nanoClock(), new MemoryPools( false ) );
        try ( Lifespan life = new Lifespan( jobScheduler );
              PageCache pageCache = pageCacheFactory.getOrCreatePageCache() )
        {
            var result = consistencyCheckService()
                    .with( pageCache )
                    .with( pageCacheTracer )
                    .runFullConsistencyCheck();

            assertFalse( result.isSuccessful() );
            assertThat( pageCacheTracer.pins() ).isGreaterThanOrEqualTo( 74 );
            assertThat( pageCacheTracer.unpins() ).isEqualTo( pageCacheTracer.pins() );
            assertThat( pageCacheTracer.hits() ).isGreaterThanOrEqualTo( 35 );
            assertThat( pageCacheTracer.faults() ).isGreaterThanOrEqualTo( 39 );
        }
    }

    @Test
    void shouldFailOnDatabaseInNeedOfRecovery() throws IOException
    {
        nonRecoveredDatabase();
        var e = assertThrows( ConsistencyCheckIncompleteException.class, () -> consistencyCheckService().runFullConsistencyCheck() );
        assertEquals( e.getCause().getMessage(),
                    Strings.joinAsLines( "Active logical log detected, this might be a source of inconsistencies.", "Please recover database.",
                            "To perform recovery please start database in single mode and perform clean shutdown." ) );
    }

    @Test
    void ableToDeleteDatabaseDirectoryAfterConsistencyCheckRun() throws ConsistencyCheckIncompleteException, IOException
    {
        prepareDbWithDeletedRelationshipPartOfTheChain();
        Result consistencyCheck = consistencyCheckService().runFullConsistencyCheck();
        assertFalse( consistencyCheck.isSuccessful() );
        // using commons file utils since they do not forgive not closed file descriptors on windows
        org.apache.commons.io.FileUtils.deleteDirectory( fixture.databaseLayout().databaseDirectory().toFile() );
    }

    @Test
    void shouldSucceedIfStoreIsConsistent() throws Exception
    {
        // when
        ConsistencyCheckService.Result result = consistencyCheckService().runFullConsistencyCheck();

        // then
        assertTrue( result.isSuccessful() );
        Path reportFile = result.reportFile();
        assertFalse( exists( reportFile ), "Unexpected generation of consistency check report file: " + reportFile );
    }

    @Test
    void shouldFailIfTheStoreInNotConsistent() throws Exception
    {
        // given
        breakNodeStore();
        Date timestamp = new Date();
        Path logsDir = testDirectory.homePath();
        Config configuration = Config.newBuilder()
                .set( settings() )
                .set( GraphDatabaseSettings.logs_directory, logsDir )
                .build();

        // when
        ConsistencyCheckService.Result result = consistencyCheckService()
                .with( configuration )
                .with( timestamp )
                .runFullConsistencyCheck();

        // then
        assertFalse( result.isSuccessful() );
        String reportFile = format( "inconsistencies-%s.report",
                new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( timestamp ) );
        assertEquals( logsDir.resolve( reportFile ), result.reportFile() );
        assertTrue( exists( result.reportFile() ), "Inconsistency report file not generated" );
    }

    @Test
    void shouldNotReportDuplicateForHugeLongValues() throws Exception
    {
        // given
        String propertyKey = "itemId";
        Label label = Label.label( "Item" );
        fixture.apply( tx -> tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create() );
        fixture.apply( tx ->
        {
            set( tx.createNode( label ), property( propertyKey, 973305894188596880L ) );
            set( tx.createNode( label ), property( propertyKey, 973305894188596864L ) );
        } );

        // when
        Result result = consistencyCheckService().runFullConsistencyCheck();

        // then
        assertTrue( result.isSuccessful() );
    }

    @Test
    void shouldReportMissingSchemaIndex() throws Exception
    {
        // given
        Label label = Label.label( "label" );
        String propKey = "propKey";
        createIndex( label, propKey );
        fixture.close();

        // when
        Path schemaDir = findFile( databaseLayout, "schema" );
        FileUtils.deleteDirectory( schemaDir );

        Result result = consistencyCheckService().runFullConsistencyCheck();

        // then
        assertTrue( result.isSuccessful() );
        Path reportFile = result.reportFile();
        assertTrue( exists( reportFile ), "Consistency check report file should be generated." );
        assertThat( Files.readString( reportFile ) ).as( "Expected to see report about schema index not being online" ).contains(
                "schema rule" ).contains( "not online" );
    }

    @Test
    void oldLuceneSchemaIndexShouldBeConsideredConsistentWithFusionProvider() throws Exception
    {
        Label label = Label.label( "label" );
        String propKey = "propKey";

        // Given a lucene index
        createIndex( label, propKey );
        fixture.apply( tx ->
        {
            tx.createNode( label ).setProperty( propKey, 1 );
            tx.createNode( label ).setProperty( propKey, "string" );
        } );

        Config configuration = Config.newBuilder()
                .set( settings() )
                .set( GraphDatabaseSettings.default_schema_provider, NATIVE_BTREE10.providerName() )
                .build();
        Result result = consistencyCheckService()
                .with( configuration )
                .runFullConsistencyCheck();
        assertTrue( result.isSuccessful() );
    }

    private void createIndex( Label label, String propKey )
    {
        fixture.apply( tx -> tx.schema().indexFor( label ).on( propKey ).create() );
        fixture.apply( tx -> tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES ) );
    }

    private static Path findFile( DatabaseLayout databaseLayout, String targetFile )
    {
        Path file = databaseLayout.file( targetFile );
        if ( Files.notExists( file ) )
        {
            fail( "Could not find file " + targetFile );
        }
        return file;
    }

    private void prepareDbWithDeletedRelationshipPartOfTheChain()
    {
        RelationshipType relationshipType = RelationshipType.withName( "testRelationshipType" );
        fixture.apply( tx ->
        {
            Node node1 = set( tx.createNode() );
            Node node2 = set( tx.createNode(), property( "key", "value" ) );
            node1.createRelationshipTo( node2, relationshipType );
            node1.createRelationshipTo( node2, relationshipType );
            node1.createRelationshipTo( node2, relationshipType );
            node1.createRelationshipTo( node2, relationshipType );
            node1.createRelationshipTo( node2, relationshipType );
            node1.createRelationshipTo( node2, relationshipType );
        } );

        NeoStores neoStores = fixture.neoStores();
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        var storeCursors = fixture.getStoreCursors();
        try ( var cursor = storeCursors.readCursor( RELATIONSHIP_CURSOR ) )
        {
            relationshipStore.getRecordByCursor( 4, relationshipRecord, RecordLoad.FORCE, cursor );
        }
        relationshipRecord.setInUse( false );
        try ( var storeCursor = storeCursors.writeCursor( RELATIONSHIP_CURSOR ) )
        {
            relationshipStore.updateRecord( relationshipRecord, storeCursor, NULL, storeCursors );
        }
    }

    private void nonRecoveredDatabase() throws IOException
    {
        Path tmpLogDir = testDirectory.homePath().resolve( "logs" );
        fs.mkdir( tmpLogDir );

        RelationshipType relationshipType = RelationshipType.withName( "testRelationshipType" );
        fixture.apply( tx ->
        {
            Node node1 = set( tx.createNode() );
            Node node2 = set( tx.createNode(), property( "key", "value" ) );
            node1.createRelationshipTo( node2, relationshipType );
        } );
        Path[] txLogs = fs.listFiles( LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fs )
                                                      .build().logFilesDirectory() );
        for ( Path file : txLogs )
        {
            fs.copyToDirectory( file, tmpLogDir );
        }
        fixture.close();
        for ( Path txLog : txLogs )
        {
            fs.deleteFile( txLog );
        }

        for ( Path file : LogFilesBuilder.logFilesBasedOnlyBuilder( tmpLogDir, fs )
                .build().logFiles() )
        {
            fs.moveToDirectory( file, databaseLayout.getTransactionLogsDirectory() );
        }
    }

    protected Map<Setting<?>,Object> settings()
    {
        Map<Setting<?>, Object> defaults = new HashMap<>();
        defaults.put( GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes( 8 ) );
        defaults.put( GraphDatabaseSettings.logs_directory, databaseLayout.databaseDirectory() );
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
                tx.create( new NodeRecord( next.node() ).initialize( false, -1, false, next.relationship(), 0 ) );
            }
        } );
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }

    private ConsistencyCheckService consistencyCheckService()
    {
        fixture.close();
        return new ConsistencyCheckService( fixture.databaseLayout() )
                .with( testDirectory.getFileSystem() )
                .with( Config.defaults( settings() ) );
    }
}
