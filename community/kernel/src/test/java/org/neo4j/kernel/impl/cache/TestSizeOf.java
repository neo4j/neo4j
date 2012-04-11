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
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.CACHE_TYPE;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TestSizeOf
{
    private static ImpermanentGraphDatabase db;

    @BeforeClass
    public static void setupDB()
    {
        db = new ImpermanentGraphDatabase( stringMap( CACHE_TYPE, CacheType.gcr.name() ) );
    }

    @AfterClass
    public static void shutdown() throws Exception
    {
        db.shutdown();
    }

    @Before
    public void clearCache()
    {
        ( (AbstractGraphDatabase) db ).getConfig().getGraphDbModule().getNodeManager().clearCache();
    }

    @SuppressWarnings( "unchecked" )
    private Cache<NodeImpl> getNodeCache()
    {
        // This is a bit fragile because we depend on the order of caches() returns its caches.
        return (Cache<NodeImpl>) IteratorUtil.first( ( (AbstractGraphDatabase) db ).getConfig().getGraphDbModule().getNodeManager().caches() );
    }

    @SuppressWarnings( "unchecked" )
    private Cache<RelationshipImpl> getRelationshipCache()
    {
        // This is a bit fragile because we depend on the order of caches() returns its caches.
        return (Cache<RelationshipImpl>) IteratorUtil.last( ( (AbstractGraphDatabase) db ).getConfig().getGraphDbModule().getNodeManager().caches() );
    }

    private Node createNodeAndLoadFresh( Map<String, Object> properties, int nrOfRelationships, int nrOfTypes )
    {
        return createNodeAndLoadFresh( properties, nrOfRelationships, nrOfTypes, 0 );
    }

    private Node createNodeAndLoadFresh( Map<String, Object> properties, int nrOfRelationships, int nrOfTypes, int directionStride )
    {
        Node node = null;
        Transaction tx = db.beginTx();
        try
        {
            node = db.createNode();
            setProperties( properties, node );
            for ( int t = 0; t < nrOfTypes; t++ )
            {
                RelationshipType type = DynamicRelationshipType.withName( relTypeName( t ) );
                for ( int i = 0, dir = 0; i < nrOfRelationships; i++, dir = (dir+directionStride)%3 )
                {
                    switch ( dir )
                    {
                    case 0: node.createRelationshipTo( db.createNode(), type ); break;
                    case 1: db.createNode().createRelationshipTo( node, type ); break;
                    case 2: node.createRelationshipTo( node, type ); break;
                    default: throw new IllegalArgumentException( "Invalid direction " + dir );
                    }
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        clearCache();

        if ( !properties.isEmpty() )
            loadProperties( node );
        if ( nrOfRelationships*nrOfTypes > 0 )
            count( node.getRelationships() );

        return node;
    }

    private void loadProperties( PropertyContainer entity )
    {
        for ( String key : entity.getPropertyKeys() )
            entity.getProperty( key );
    }

    private void setProperties( Map<String, Object> properties, PropertyContainer entity )
    {
        for ( Map.Entry<String, Object> property : properties.entrySet() )
            entity.setProperty( property.getKey(), property.getValue() );
    }

    private Relationship createRelationshipAndLoadFresh( Map<String, Object> properties )
    {
        Relationship relationship = null;
        Transaction tx = db.beginTx();
        try
        {
            relationship = db.createNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            setProperties( properties, relationship );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        clearCache();

        if ( !properties.isEmpty() )
            loadProperties( relationship );

        return relationship;
    }

    private String relTypeName( int t )
    {
        return "mytype" + t;
    }

    @Test
    public void cacheSizeCorrelatesWithNodeSizeAfterFullyLoadingRelationships() throws Exception
    {
        // Create node with a couple of relationships
        Node node = createNodeAndLoadFresh( map(), 10, 1 );
        Cache<NodeImpl> nodeCache = getNodeCache();

        // Just an initial sanity assertion, we start off with a clean cache
        clearCache();
        assertEquals( 0, nodeCache.size() );

        // Fully cache the node and its relationships
        count( node.getRelationships() );

        // Now the node cache size should be the same as doing node.size()
        assertEquals( db.getConfig().getGraphDbModule().getNodeManager().getNodeIfCached( node.getId() ).size(), nodeCache.size() );
    }
}
