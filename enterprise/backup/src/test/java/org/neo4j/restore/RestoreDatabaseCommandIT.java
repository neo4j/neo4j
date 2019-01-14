/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.restore;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class RestoreDatabaseCommandIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void forceShouldRespectStoreLock()
    {
        String databaseName = "to";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( GraphDatabaseSettings.database_path );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromPath, fromNodeCount );
        createDbAt( toPath, toNodeCount );

        FileSystemAbstraction fs = fileSystemRule.get();
        try ( StoreLocker storeLocker = new StoreLocker( fs, toPath ) )
        {
            storeLocker.checkLock();

            new RestoreDatabaseCommand( fs, fromPath, config, databaseName, true ).execute();
            fail( "expected exception" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), equalTo( "the database is in use -- stop Neo4j and try again" ) );
        }
    }

    @Test
    public void shouldNotCopyOverAndExistingDatabase() throws Exception
    {
        // given
        String databaseName = "to";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( GraphDatabaseSettings.database_path );

        createDbAt( fromPath, 0 );
        createDbAt( toPath, 0 );

        try
        {
            // when

            new RestoreDatabaseCommand( fileSystemRule.get(), fromPath, config, databaseName, false ).execute();
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException exception )
        {
            // then
            assertTrue( exception.getMessage(),
                    exception.getMessage().contains( "Database with name [to] already exists" ) );
        }
    }

    @Test
    public void shouldThrowExceptionIfBackupDirectoryDoesNotExist() throws Exception
    {
        // given
        String databaseName = "to";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( GraphDatabaseSettings.database_path );

        createDbAt( toPath, 0 );

        try
        {
            // when

            new RestoreDatabaseCommand( fileSystemRule.get(), fromPath, config, databaseName, false ).execute();
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException exception )
        {
            // then
            assertTrue( exception.getMessage(), exception.getMessage().contains( "Source directory does not exist" ) );
        }
    }

    @Test
    public void shouldThrowExceptionIfBackupDirectoryDoesNotHaveStoreFiles() throws Exception
    {
        // given
        String databaseName = "to";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        assertTrue( fromPath.mkdirs() );

        try
        {
            // when
            new RestoreDatabaseCommand( fileSystemRule.get(), fromPath, config, databaseName, false ).execute();
            fail( "Should have thrown exception" );
        }
        catch ( IllegalArgumentException exception )
        {
            // then
            assertTrue( exception.getMessage(), exception.getMessage()
                    .contains( "Source directory is not a database backup" ) );
        }
    }

    @Test
    public void shouldAllowForcedCopyOverAnExistingDatabase() throws Exception
    {
        // given
        String databaseName = "to";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( GraphDatabaseSettings.database_path );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromPath, fromNodeCount );
        createDbAt( toPath, toNodeCount );

        // when
        new RestoreDatabaseCommand( fileSystemRule.get(), fromPath, config, databaseName, true ).execute();

        // then
        GraphDatabaseService copiedDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( toPath )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();

        try ( Transaction ignored = copiedDb.beginTx() )
        {
            assertEquals( fromNodeCount, Iterables.count( copiedDb.getAllNodes() ) );
        }

        copiedDb.shutdown();
    }

    @Test
    public void restoreExplicitIndexesFromBackup() throws IOException, CommandFailed
    {
        String databaseName = "destination";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );
        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( GraphDatabaseSettings.database_path );

        createDbWithExplicitIndexAt( fromPath, 100 );

        new RestoreDatabaseCommand( fileSystemRule.get(), fromPath, config, databaseName, true ).execute();

        GraphDatabaseService restoredDatabase = createDatabase( toPath, toPath.getAbsolutePath() );

        try ( Transaction transaction = restoredDatabase.beginTx() )
        {
            IndexManager indexManager = restoredDatabase.index();
            String[] nodeIndexNames = indexManager.nodeIndexNames();
            String[] relationshipIndexNames = indexManager.relationshipIndexNames();

            for ( String nodeIndexName : nodeIndexNames )
            {
                countNodesByKeyValue( indexManager, nodeIndexName, "a", "b" );
                countNodesByKeyValue( indexManager, nodeIndexName, "c", "d" );
            }

            for ( String relationshipIndexName : relationshipIndexNames )
            {
                countRelationshipByKeyValue( indexManager, relationshipIndexName, "x", "y" );
            }
        }
        restoredDatabase.shutdown();
    }

    @Test
    public void restoreTransactionLogsInCustomDirectoryForTargetDatabaseWhenConfigured()
            throws IOException, CommandFailed
    {
        String databaseName = "to";
        Config config = configWith( databaseName, directory.absolutePath().getAbsolutePath() );
        File customTxLogDirectory = directory.directory( "customLogicalLog" );
        String customTransactionLogDirectory = customTxLogDirectory.getAbsolutePath();
        config.augmentDefaults( GraphDatabaseSettings.logical_logs_location, customTransactionLogDirectory );

        File fromPath = new File( directory.absolutePath(), "from" );
        File toPath = config.get( GraphDatabaseSettings.database_path );
        int fromNodeCount = 10;
        int toNodeCount = 20;
        createDbAt( fromPath, fromNodeCount );

        GraphDatabaseService db = createDatabase( toPath, customTransactionLogDirectory );
        createTestData( toNodeCount, db );
        db.shutdown();

        // when
        new RestoreDatabaseCommand( fileSystemRule.get(), fromPath, config, databaseName, true ).execute();

        LogFiles fromStoreLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromPath, fileSystemRule.get() ).build();
        LogFiles toStoreLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( toPath, fileSystemRule.get() ).build();
        LogFiles customLogLocationLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( customTxLogDirectory, fileSystemRule.get() ).build();
        assertThat( toStoreLogFiles.logFiles(), emptyArray() );
        assertThat( customLogLocationLogFiles.logFiles(), arrayWithSize( 1 ) );
        assertEquals( fromStoreLogFiles.getLogFileForVersion( 0 ).length(),
                customLogLocationLogFiles.getLogFileForVersion( 0 ).length() );
    }

    @Test
    public void doNotRemoveRelativeTransactionDirectoryAgain() throws IOException, CommandFailed
    {
        FileSystemAbstraction fileSystem = Mockito.spy( fileSystemRule.get() );
        File fromPath = directory.directory( "from" );
        File databaseFile = directory.directory();
        File relativeLogDirectory = directory.directory( "relativeDirectory" );

        Config config = Config.defaults( GraphDatabaseSettings.database_path, databaseFile.getAbsolutePath() );
        config.augment( GraphDatabaseSettings.logical_logs_location, relativeLogDirectory.getAbsolutePath() );

        createDbAt( fromPath, 10 );

        new RestoreDatabaseCommand( fileSystem, fromPath, config, "testDatabase", true ).execute();

        verify( fileSystem ).deleteRecursively( eq( databaseFile ) );
        verify( fileSystem, never() ).deleteRecursively( eq( relativeLogDirectory ) );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new RestoreDatabaseCliProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin restore --from=<backup-directory> [--database=<name>]%n" +
                            "                           [--force[=<true|false>]]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Restore a backed up database.%n" +
                            "%n" +
                            "options:%n" +
                            "  --from=<backup-directory>   Path to backup to restore from.%n" +
                            "  --database=<name>           Name of database. [default:graph.db]%n" +
                            "  --force=<true|false>        If an existing database should be replaced.%n" +
                            "                              [default:false]%n" ),
                    baos.toString() );
        }
    }

    private static void countRelationshipByKeyValue( IndexManager indexManager, String indexName, String key, String value )
    {
        try ( IndexHits<Relationship> nodes = indexManager.forRelationships( indexName ).get( key, value ) )
        {
            assertEquals( 50, nodes.size() );
        }
    }

    private static void countNodesByKeyValue( IndexManager indexManager, String indexName, String key, String value )
    {
        try ( IndexHits<Node> nodes = indexManager.forNodes( indexName ).get( key, value ) )
        {
            assertEquals( 50, nodes.size() );
        }
    }

    private static Config configWith( String databaseName, String dataDirectory )
    {
        return Config.defaults( stringMap( GraphDatabaseSettings.active_database.name(), databaseName,
                GraphDatabaseSettings.data_directory.name(), dataDirectory ) );
    }

    private void createDbAt( File fromPath, int nodesToCreate )
    {
        GraphDatabaseService db = createDatabase( fromPath, fromPath.getAbsolutePath() );

        createTestData( nodesToCreate, db );

        db.shutdown();
    }

    private GraphDatabaseService createDatabase( File fromPath, String absolutePath )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( fromPath )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.logical_logs_location, absolutePath )
                .newGraphDatabase();
    }

    private void createDbWithExplicitIndexAt( File fromPath, int pairNumberOfNodesToCreate )
    {
        GraphDatabaseService db = createDatabase( fromPath, fromPath.getAbsolutePath() );

        Index<Node> explicitNodeIndex;
        RelationshipIndex explicitRelationshipIndex;
        try ( Transaction transaction = db.beginTx() )
        {
            explicitNodeIndex = db.index().forNodes( "explicitNodeIndex" );
            explicitRelationshipIndex = db.index().forRelationships( "explicitRelationshipIndex" );
            transaction.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < pairNumberOfNodesToCreate; i += 2 )
            {
                Node node = db.createNode();
                Node otherNode = db.createNode();
                Relationship relationship = node.createRelationshipTo( otherNode, RelationshipType.withName( "rel" ) );

                explicitNodeIndex.add( node, "a", "b" );
                explicitNodeIndex.add( otherNode, "c", "d" );
                explicitRelationshipIndex.add( relationship, "x", "y" );
            }
            tx.success();
        }
        db.shutdown();
    }

    private void createTestData( int nodesToCreate, GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodesToCreate; i++ )
            {
                db.createNode();
            }
            tx.success();
        }
    }
}
