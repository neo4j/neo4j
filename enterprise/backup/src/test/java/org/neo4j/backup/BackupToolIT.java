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
package org.neo4j.backup;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.backup.impl.BackupProtocolService;
import org.neo4j.backup.impl.ConsistencyCheck;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

public class BackupToolIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass());
    @Rule
    public ExpectedException expected = ExpectedException.none();
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();

    private DefaultFileSystemAbstraction fs;
    private PageCache pageCache;
    private Path backupDir;
    private BackupTool backupTool;

    @Before
    public void setUp()
    {
        backupDir = testDirectory.directory( "backups/graph.db" ).toPath();
        fs = new DefaultFileSystemAbstraction();
        pageCache = StandalonePageCacheFactory.createPageCache( fs );
        PrintStream out = mock( PrintStream.class );
        BackupProtocolService service = new BackupProtocolService( () -> fs, NullLogProvider.getInstance(), out, new Monitors(), pageCache );
        backupTool = new BackupTool( service, out );
    }

    @After
    public void tearDown() throws Exception
    {
        pageCache.close();
        fs.close();
    }

    @Test
    public void oldIncompatibleBackupsThrows() throws Exception
    {
        // Prepare an "old" backup
        prepareNeoStoreFile( StandardV2_3.STORE_VERSION );

        // Start database to backup
        int backupPort = PortAuthority.allocatePort();
        GraphDatabaseService db = startGraphDatabase( backupPort );
        try
        {
            expected.expect( BackupTool.ToolFailureException.class );
            expected.expectMessage( "Failed to perform backup because existing backup is from a different version." );

            // Perform backup
            backupTool.executeBackup( new HostnamePort( "localhost", backupPort ), backupDir,
                    ConsistencyCheck.NONE, Config.defaults( GraphDatabaseSettings.record_format, StandardV3_4.NAME ),
                    20L * 60L * 1000L, false );
        }
        finally
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService startGraphDatabase( int backupPort )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.directory() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1:" + backupPort )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, StandardV2_3.NAME )
                .newGraphDatabase();
    }

    private void prepareNeoStoreFile( String storeVersion ) throws Exception
    {
        File neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
    }

    private File createNeoStoreFile() throws Exception
    {
        fs.mkdirs( backupDir.toFile() );
        File neoStoreFile = new File( backupDir.toFile(), MetaDataStore.DEFAULT_NAME );
        fs.create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
