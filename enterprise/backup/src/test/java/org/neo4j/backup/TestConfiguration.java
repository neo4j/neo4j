/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
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
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( SOURCE_DIR );
        OnlineBackup.from( HOST_ADDRESS ).full( BACKUP_DIR );
        db.shutdown();
    }

    @Test
    public void testOffByConfig() throws Exception
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS ).full( BACKUP_DIR );
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
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .newGraphDatabase();

        OnlineBackup.from( HOST_ADDRESS ).full( BACKUP_DIR );
        db.shutdown();
    }

    @Test
    public void testEnableCustomPortInConfig() throws Exception
    {
        String customPort = "12345";
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( SOURCE_DIR )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, ":" + customPort )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS ).full( BACKUP_DIR );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }

        OnlineBackup.from( HOST_ADDRESS, Integer.parseInt(customPort) ).full( BACKUP_DIR );
        db.shutdown();
    }
}
