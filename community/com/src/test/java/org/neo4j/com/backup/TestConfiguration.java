package org.neo4j.com.backup;

import static org.junit.Assert.fail;
import static org.neo4j.com.backup.OnlineBackupExtension.ENABLE_ONLINE_BACKUP;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestConfiguration
{
    private static final String BACKUP_DIR = "target/full-backup";
    
    @Before
    public void before() throws Exception
    {
        FileUtils.deleteDirectory( new File( BACKUP_DIR ) );
    }
    
    private GraphDatabaseService newDb( String onlineBackupConfig )
    {
        String path = "target/db";
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
