/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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

import org.neo4j.com.ports.allocation.PortAuthority;
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
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
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
    public void setUp() throws Exception
    {
        backupDir = testDirectory.directory( "backups/graph.db" ).toPath();
        fs = new DefaultFileSystemAbstraction();
        pageCache = StandalonePageCacheFactory.createPageCache( fs );
        backupTool = new BackupTool( new BackupProtocolService(), mock( PrintStream.class ) );
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
            backupTool.executeBackup( new HostnamePort( "localhost", backupPort ), backupDir.toFile(),
                    ConsistencyCheck.NONE, Config.defaults(), 20L * 60L * 1000L, false );
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
