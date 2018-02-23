/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
public class TestConfiguration
{
    @Resource
    public SuppressOutput suppressOutput;
    @Resource
    public TestDirectory dir;

    private static final String HOST_ADDRESS = "127.0.0.1";

    private File sourceDir;
    private String backupDir;

    @BeforeEach
    public void before() throws Exception
    {
        sourceDir = dir.makeGraphDbDir();
        backupDir = dir.cleanDirectory( "full-backup" ).getAbsolutePath();
    }

    @Test
    public void testOnByDefault()
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourceDir )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port ).newGraphDatabase();
        OnlineBackup.from( HOST_ADDRESS, port ).full( backupDir );
        db.shutdown();
    }

    @Test
    public void testOffByConfig()
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourceDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS, port ).full( backupDir );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }
        db.shutdown();
    }

    @Test
    public void testEnableDefaultsInConfig()
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourceDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port )
                .newGraphDatabase();

        OnlineBackup.from( HOST_ADDRESS, port ).full( backupDir );
        db.shutdown();
    }

    @Test
    public void testEnableCustomPortInConfig()
    {
        int customPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourceDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, ":" + customPort )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS, PortAuthority.allocatePort() ).full( backupDir );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }

        OnlineBackup.from( HOST_ADDRESS, customPort ).full( backupDir );
        db.shutdown();
    }
}
