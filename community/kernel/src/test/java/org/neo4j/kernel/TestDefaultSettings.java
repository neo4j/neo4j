/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

public class TestDefaultSettings
{
    private static final File DB_PATH = new File( "target", "defaults" );

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
    
    @BeforeClass
    public static void deleteDb() throws Exception
    {
        FileUtils.deleteRecursively( DB_PATH );
    }

    @Test
    public void testDefaults()
    {
        db = new EmbeddedGraphDatabase( DB_PATH.getAbsolutePath());

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

        db = new EmbeddedGraphDatabase( DB_PATH.getAbsolutePath(), overrides );

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
