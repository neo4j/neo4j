/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestConfiguration
{
    private static final String SOURCE_DIR = "target/db";
    private static final String BACKUP_DIR = "target/full-backup";
    
    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( new File( SOURCE_DIR ) );
        FileUtils.deleteDirectory( new File( BACKUP_DIR ) );
    }
    
    private GraphDatabaseService newDb( String onlineBackupConfig )
    {
        String path = SOURCE_DIR;
        return onlineBackupConfig == null ?
                new EmbeddedGraphDatabase( path ) :
                new EmbeddedGraphDatabase( path, stringMap( ENABLE_ONLINE_BACKUP, onlineBackupConfig ) );
    }
    
    @Test
    public void testOffByDefault() throws Exception
    {
        GraphDatabaseService db = newDb( null );
        try
        {
            OnlineBackup.from( "localhost" ).full( BACKUP_DIR );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }
        db.shutdown();
    }
    
    @Test
    public void testOffByConfig() throws Exception
    {
        GraphDatabaseService db = newDb( "false" );
        try
        {
            OnlineBackup.from( "localhost" ).full( BACKUP_DIR );
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
        GraphDatabaseService db = newDb( "true" );
        OnlineBackup.from( "localhost" ).full( BACKUP_DIR );
        db.shutdown();
    }

    @Test
    public void testEnableCustomPortInConfig() throws Exception
    {
        int customPort = 12345;
        GraphDatabaseService db = newDb( "port=" + customPort );
        try
        {
            OnlineBackup.from( "localhost" ).full( BACKUP_DIR );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }
        
        OnlineBackup.from( "localhost", customPort ).full( BACKUP_DIR );
        db.shutdown();
    }
}
