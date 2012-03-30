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
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
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
    
    private GraphDatabaseAPI newDb( String cacheType )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( PATH ).setConfig( GraphDatabaseSettings.cache_type.name(), cacheType ).newGraphDatabase();
    }

    @Test
    public void testDefaultCache()
    {
        GraphDatabaseAPI db = newDb( null );
        assertEquals( CacheType.gcr, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testWeakRefCache()
    {
        GraphDatabaseAPI db = newDb( CacheType.weak.name() );
        assertEquals( CacheType.weak, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testSoftRefCache()
    {
        GraphDatabaseAPI db = newDb( CacheType.soft.name() );
        assertEquals( CacheType.soft, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testNoCache()
    {
        GraphDatabaseAPI db = newDb( CacheType.none.name() );
        assertEquals( CacheType.none, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testStrongCache()
    {
        GraphDatabaseAPI db = newDb( CacheType.strong.name() );
        assertEquals( CacheType.strong, db.getNodeManager().getCacheType() );
        db.shutdown();
    }

    @Test
    public void testGcrCache()
    {
        GraphDatabaseAPI db = newDb( CacheType.gcr.name() );
        assertEquals( CacheType.gcr, db.getNodeManager().getCacheType() );
        db.shutdown();
    }
    
    @Test
    public void testInvalidCache()
    {
        // invalid cache type should fail
        GraphDatabaseAPI db = null;
        try
        {
            db = newDb( "whatever" );
            fail( "Wrong cache type should not be allowed" );
        }
        catch( Exception e )
        {
            // Ok
        }
    }
}
