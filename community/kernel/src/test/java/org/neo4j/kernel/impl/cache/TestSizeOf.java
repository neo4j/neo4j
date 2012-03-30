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
package org.neo4j.kernel.impl.cache;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.CACHE_TYPE;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TestSizeOf
{
    private ImpermanentGraphDatabase db;
    private Node node;
    
    @Before
    public void doBefore() throws Exception
    {
        db = new ImpermanentGraphDatabase( stringMap( CACHE_TYPE, "array" ) );
        Transaction tx = db.beginTx();
        try
        {
            node = db.createNode();
            for ( int i = 0; i < 10; i++ )
                node.createRelationshipTo( db.createNode(), TEST );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        // Clear the cache so that we start from a fresh slate.
        db.getNodeManager().clearCache();
    }

    @After
    public void doAfter() throws Exception
    {
        db.shutdown();
    }
    
    @SuppressWarnings( "unchecked" )
    private Cache<NodeImpl> getNodeCache()
    {
        // This is a bit fragile because we depend on the order of caches() returns its caches.
        return (Cache<NodeImpl>) db.getNodeManager().caches().iterator().next();
    }

    @Test
    public void cacheSizeCorrelatesWithNodeSizeAfterFullyLoadingRelationships() throws Exception
    {
        Cache<NodeImpl> nodeCache = getNodeCache();
        
        // Just an initial sanity assertion, we start off with a clean cache
        assertEquals( 0, nodeCache.size() );
        
        // Fully cache the node and its relationships
        count( node.getRelationships() );
        
        // Now the node cache size should be the same as doing node.size()
        assertEquals( db.getNodeManager().getNodeForProxy( node.getId(), null ).size(), nodeCache.size() );
    }
}
