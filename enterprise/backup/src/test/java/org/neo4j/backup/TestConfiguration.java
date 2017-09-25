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

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.fail;

public class TestConfiguration
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private static final File SOURCE_DIR = new File( "target/db" );
    private static final String BACKUP_DIR = "target/full-backup";
    private static final String HOST_ADDRESS = "127.0.0.1";

    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( SOURCE_DIR );
        FileUtils.deleteDirectory( new File( BACKUP_DIR ) );
    }

    @Test
    public void testOnByDefault() throws Exception
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port ).newGraphDatabase();
        OnlineBackup.from( HOST_ADDRESS, port ).full( BACKUP_DIR );
        db.shutdown();
    }

    @Test
    public void testOffByConfig() throws Exception
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS, port ).full( BACKUP_DIR );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }
        db.shutdown();
    }

    @Test
    public void testEnableDefaultsInConfig() throws Exception
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port )
                .newGraphDatabase();

        OnlineBackup.from( HOST_ADDRESS, port ).full( BACKUP_DIR );
        db.shutdown();
    }

    @Test
    public void testEnableCustomPortInConfig() throws Exception
    {
        int customPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, ":" + customPort )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS, PortAuthority.allocatePort() ).full( BACKUP_DIR );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }

        OnlineBackup.from( HOST_ADDRESS, customPort ).full( BACKUP_DIR );
        db.shutdown();
    }
}
