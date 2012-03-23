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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;

public class TestCacheTypes extends AbstractNeo4jTestCase
{
    private static final String PATH = getStorePath( "cache-db" );

    @BeforeClass
    public static void clear()
    {
        deleteFileOrDirectory( new File( PATH ) );
    }
    
    private EmbeddedGraphDatabase newDb( String cacheType )
    {
        return new EmbeddedGraphDatabase( PATH, MapUtil.stringMap( Config.CACHE_TYPE, cacheType ) );
    }

    @Test
    public void testDefaultCache()
    {
        EmbeddedGraphDatabase db = newDb( null );
        try
        {
            assertEquals( CacheType.soft, db.getNodeManager().getCacheType() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void testWeakRefCache()
    {
        EmbeddedGraphDatabase db = newDb( "weak" );
        assertEquals( CacheType.weak, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testSoftRefCache()
    {
        EmbeddedGraphDatabase db = newDb( "soft" );
        assertEquals( CacheType.soft, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testNoCache()
    {
        EmbeddedGraphDatabase db = newDb( "none" );
        assertEquals( CacheType.none, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testStrongCache()
    {
        EmbeddedGraphDatabase db = newDb( "strong" );
        assertEquals( CacheType.strong, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testArrayCache()
    {
        EmbeddedGraphDatabase db = newDb( "array" );
        assertEquals( CacheType.array, db.getNodeManager().getCacheType() );
        db.shutdown();
    }
    
    @Test
    public void testInvalidCache()
    {
        // invalid cache type should use default and print a warning
        EmbeddedGraphDatabase db = newDb( "whatever" );
        assertEquals( CacheType.soft, db.getNodeManager().getCacheType() );
        db.shutdown();
    }
}
