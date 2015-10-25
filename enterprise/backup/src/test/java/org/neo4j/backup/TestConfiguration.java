/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;

import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Settings;

import static org.junit.Assert.fail;

public class TestConfiguration
{
    private static final File SOURCE_DIR = new File( "target/db" );
    private static final File BACKUP_DIR = new File( "target/full-backup" );
    
    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( SOURCE_DIR );
        FileUtils.deleteDirectory( BACKUP_DIR );
    }
    
    @Test
    public void testOnByDefault() throws Exception
    {
        GraphDatabaseService db = TestGraphDatabase.open( SOURCE_DIR );
        OnlineBackup.from( InetAddress.getLocalHost().getHostAddress() ).backup( BACKUP_DIR );
        db.shutdown();
    }
    
    @Test
    public void testOffByConfig() throws Exception
    {
        GraphDatabaseService db = TestGraphDatabase.build()
                .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .open( SOURCE_DIR );
        try
        {
            OnlineBackup.from( InetAddress.getLocalHost().getHostAddress() ).backup( BACKUP_DIR );
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
        GraphDatabaseService db = TestGraphDatabase.build()
            .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
            .open( SOURCE_DIR );

        OnlineBackup.from( InetAddress.getLocalHost().getHostAddress() ).backup( BACKUP_DIR );
        db.shutdown();
    }

    @Test
    public void testEnableCustomPortInConfig() throws Exception
    {
        String customPort = "12345";
        GraphDatabaseService db = TestGraphDatabase.build()
                .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .withSetting( OnlineBackupSettings.online_backup_server, ":" + customPort )
                .open( SOURCE_DIR );
        try
        {
            OnlineBackup.from( InetAddress.getLocalHost().getHostAddress() ).backup( BACKUP_DIR );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }
        
        OnlineBackup.from( InetAddress.getLocalHost().getHostAddress(), Integer.parseInt(customPort) ).backup( BACKUP_DIR );
        db.shutdown();
    }
}
