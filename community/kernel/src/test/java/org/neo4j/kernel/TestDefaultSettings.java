package org.neo4j.kernel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

public class TestDefaultSettings
{

    private EmbeddedGraphDatabase db;

    @After
    public void shutdownDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }
        db = null;
    }

    @Test
    public void testDefaults()
    {
        db = new EmbeddedGraphDatabase( "target" + File.separator + "defaults" );

        Config config = db.getConfig();
        assertTrue( config.getInputParams().entrySet().isEmpty() );
        Map<Object, Object> params = config.getParams();
        boolean mmaped = Boolean.parseBoolean( (String) params.get(
                Config.USE_MEMORY_MAPPED_BUFFERS ) );
        boolean isWin = Config.osIsWindows();

        // Memory map is on for non win, off for win
        if ( isWin ) assertFalse( mmaped );
        if ( !isWin ) assertTrue( mmaped );

        assertEquals( "20M", params.get( Config.NODE_STORE_MMAP_SIZE ) );
        assertEquals( "90M", params.get( Config.PROPERTY_STORE_MMAP_SIZE ) );
        assertEquals( "1M", params.get( Config.PROPERTY_INDEX_STORE_MMAP_SIZE ) );
        assertEquals( "1M",
                params.get(
                        Config.PROPERTY_INDEX_KEY_STORE_MMAP_SIZE ) );
        assertEquals( "130M",
                params.get( Config.STRING_PROPERTY_STORE_MMAP_SIZE ) );
        assertEquals( "130M",
                params.get( Config.ARRAY_PROPERTY_STORE_MMAP_SIZE ) );
        assertEquals( "100M", params.get( Config.RELATIONSHIP_STORE_MMAP_SIZE ) );
    }

    @Test
    public void testOverrides()
    {
        Map<String, String> overrides = new HashMap<String, String>();
        if ( Config.osIsWindows() )
        {
            overrides.put( Config.USE_MEMORY_MAPPED_BUFFERS, "true" );
        }
        else
        {
            overrides.put( Config.USE_MEMORY_MAPPED_BUFFERS, "false" );
        }

        overrides.put( Config.NODE_STORE_MMAP_SIZE, "10M" );
        overrides.put( Config.PROPERTY_STORE_MMAP_SIZE, "10M" );
        overrides.put( Config.PROPERTY_INDEX_STORE_MMAP_SIZE, "10M" );
        overrides.put( Config.PROPERTY_INDEX_KEY_STORE_MMAP_SIZE, "10M" );
        overrides.put( Config.STRING_PROPERTY_STORE_MMAP_SIZE, "10M" );
        overrides.put( Config.ARRAY_PROPERTY_STORE_MMAP_SIZE, "10M" );
        overrides.put( Config.RELATIONSHIP_STORE_MMAP_SIZE, "10M" );

        overrides.put( Config.ALLOW_STORE_UPGRADE, "true" );
        overrides.put( Config.DUMP_CONFIGURATION, "true" );

        db = new EmbeddedGraphDatabase( "target" + File.separator + "defaults",
                overrides );

        Config config = db.getConfig();
        assertEquals( 10, config.getInputParams().entrySet().size() );
        Map<Object, Object> params = config.getParams();

        boolean mmaped = Boolean.parseBoolean( (String) params.get(
                Config.USE_MEMORY_MAPPED_BUFFERS ) );
        boolean isWin = Config.osIsWindows();

        if ( isWin ) assertTrue( mmaped );
        if ( !isWin ) assertFalse( mmaped );
        
        assertEquals( "10M", params.get( Config.NODE_STORE_MMAP_SIZE ) );
        assertEquals( "10M", params.get( Config.PROPERTY_STORE_MMAP_SIZE ) );
        assertEquals( "10M", params.get( Config.PROPERTY_INDEX_STORE_MMAP_SIZE ) );
        assertEquals( "10M",
                params.get( Config.PROPERTY_INDEX_KEY_STORE_MMAP_SIZE ) );
        assertEquals( "10M",
                params.get( Config.STRING_PROPERTY_STORE_MMAP_SIZE ) );
        assertEquals( "10M", params.get( Config.ARRAY_PROPERTY_STORE_MMAP_SIZE ) );
        assertEquals( "10M", params.get( Config.RELATIONSHIP_STORE_MMAP_SIZE ) );
        assertEquals( "true", params.get( Config.ALLOW_STORE_UPGRADE ) );
        assertEquals( "true", params.get( Config.DUMP_CONFIGURATION ) );
    }
}
